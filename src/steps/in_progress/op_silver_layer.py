from pathlib import Path
import re
import time
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent

data_dir = project_root / "data"
operational_dir = data_dir / "bronze" / "operational" / "2026"

sgti_silver_dir = data_dir / "silver" / "sgti" / "2026"
db_sgti_vehicles = sgti_silver_dir / "consolidated_vehicles.parquet"
db_sgti_contracts = sgti_silver_dir / "consolidated_services.parquet"
db_expanded_schedule = sgti_silver_dir / "expanded_schedule.parquet"

bmi_dim_companies_path = data_dir / "gold" / "bmi" / "2026" / "bmi_dim_companies.parquet"

output_folder = data_dir / "silver" / "operational" / "2026"
output_folder.mkdir(parents=True, exist_ok=True)

for path in [
    operational_dir,
    db_sgti_vehicles,
    db_sgti_contracts,
    db_expanded_schedule,
    bmi_dim_companies_path,
]:
    if not path.exists():
        raise FileNotFoundError(f"Path or file not found: {path}")

# --- OUTPUT PATHS (PARQUET & EXCEL ONLY) ---
parquet_sanitized = output_folder / "operational_2026_sanitized_unified.parquet"
parquet_audit = output_folder / "operational_2026_all_records.parquet"
parquet_general_audit = output_folder / "operational_2026_general_audit.parquet"
xlsx_quarantine = output_folder / "operational_2026_discarded_records.xlsx"

overall_start_time = time.time()

# ==============================================
# 2. HELPER FUNCTIONS AND ROBUST CONVERTERS
# ==============================================

num_to_letter = {
    "0": "A", "1": "B", "2": "C", "3": "D", "4": "E",
    "5": "F", "6": "G", "7": "H", "8": "I", "9": "J",
}
letter_to_num = {v: k for k, v in num_to_letter.items()}


def extract_metadata_from_filename(filename: str):
    stem = Path(filename).stem
    if stem.lower().startswith("bronze_"):
        stem = stem[7:]

    m1 = re.search(r"^(.*?)[_]+(\d{4})[_]+(\d{1,2})(?:_RET\d+)?", stem, re.IGNORECASE)
    if m1:
        return m1.group(1).strip("_").lower(), int(m1.group(2)), int(m1.group(3))

    m2 = re.search(r"^(.*?)[_]+(\d{4})(\d{2})(?:_RET\d+)?", stem, re.IGNORECASE)
    if m2:
        return m2.group(1).strip("_").lower(), int(m2.group(2)), int(m2.group(3))

    m3 = re.search(r"^(.*?)[_]+(\d{4})[-]+(\d{1,2})(?:_RET\d+)?", stem, re.IGNORECASE)
    if m3:
        return m3.group(1).strip("_").lower(), int(m3.group(2)), int(m3.group(3))

    raise ValueError(f"Unable to extract metadata from filename: '{filename}'")


def safe_float(value):
    try:
        return float(value) if pd.notna(value) else 0.0
    except (ValueError, TypeError):
        return 0.0


def safe_int(value, default=0):
    try:
        if pd.isna(value) or value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def normalize_text_key(val):
    if pd.isna(val) or val is None:
        return ""
    text = str(val).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return re.sub(r"^0+(?=\d)", "", text)


def normalize_date(val):
    if pd.isna(val) or val is None or str(val).strip().lower() in ("", "nan", "none", "nat"):
        return ""
    text = str(val).strip().split(" ")[0].replace("/", "-")
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return text
    try:
        dt = pd.to_datetime(text, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
        return ""
    except Exception:
        return ""


def normalize_time_str(val):
    if pd.isna(val) or not val or str(val).strip() == "":
        return ""
    try:
        parts = str(val).strip().split(":")
        hours = parts[0].zfill(2)
        minutes = parts[1].zfill(2) if len(parts) > 1 else "00"
        return f"{hours}:{minutes}"
    except Exception:
        return str(val).strip()


def clean_plate(value):
    if pd.isna(value) or value is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper()).strip()


def is_valid_plate(plate):
    plate = clean_plate(plate)
    return bool(re.match(r"^[A-Z]{3}[0-9]{4}$", plate)) or bool(
        re.match(r"^[A-Z]{3}[0-9][A-Z][0-9]{2}$", plate)
    )


def old_to_mercosul(plate):
    plate = clean_plate(plate)
    if len(plate) != 7 or plate[4] not in num_to_letter:
        return None
    return plate[:4] + num_to_letter[plate[4]] + plate[5:]


def mercosul_to_old(plate):
    plate = clean_plate(plate)
    if len(plate) != 7 or plate[4] not in letter_to_num:
        return None
    return plate[:4] + letter_to_num[plate[4]] + plate[5:]


def equivalent_plate(plate):
    plate = clean_plate(plate)
    if len(plate) != 7:
        return None
    return (
        old_to_mercosul(plate)
        if plate[4].isdigit()
        else mercosul_to_old(plate)
    )


def select_vehicle(row):
    sub2 = clean_plate(row.get("replacement_vehicle_2"))
    if sub2:
        return sub2
    sub1 = clean_plate(row.get("replacement_vehicle_1"))
    if sub1:
        return sub1
    return clean_plate(row.get("vehicle"))


def get_month_search_list(month):
    if month <= 0:
        return list(range(1, 13))
    results = [month]
    for dist in range(1, 12):
        if month - dist >= 1:
            results.append(month - dist)
        if month + dist <= 12:
            results.append(month + dist)
    return results


def time_to_minutes(time_str):
    if pd.isna(time_str) or not time_str:
        return 0
    try:
        parts = str(time_str).strip().split(":")
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        return hours * 60 + minutes
    except (ValueError, IndexError):
        return 0


def convert_types_for_export(df):
    df_out = df.copy()
    df_out.columns = df_out.columns.str.strip().str.lower()

    cols_int = [
        "reference_month",
        "reference_year",
        "bodywork_year",
        "sgti_matched_month",
        "month_distance",
        "source_row",
    ]
    for col in cols_int:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("Int64")

    cols_float = ["total_line_km", "time_difference_minutes"]
    for col in cols_float:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("float64")

    if "date" in df_out.columns:
        df_out["date"] = df_out["date"].apply(normalize_date)

    cols_text = [c for c in df_out.columns if c not in cols_int + cols_float]
    for col in cols_text:
        df_out[col] = df_out[col].fillna("").astype(str).str.strip()

    return df_out


def load_operational_data_from_folder(base_folder: Path):
    all_files = [
        f for f in base_folder.rglob("*") 
        if f.is_file() 
        and not f.name.startswith("~$")
        and not f.name.startswith("audit_")
        and "discarded" not in f.name.lower()
    ]
    
    file_map = {}
    print(f"Scanning Bronze directory: {base_folder}\n")

    for file_path in all_files:
        ext = file_path.suffix.lower()
        if ext in [".xlsx", ".xls", ".parquet", ".csv"]:
            company_folder = file_path.parent.name.lower()
            
            m_id = re.search(r"(OP\d{6,12})", file_path.name, re.IGNORECASE)
            ingestion_id = m_id.group(1).upper() if m_id else ""
            
            try:
                comp_name, ref_year, ref_month = extract_metadata_from_filename(file_path.name)
                company = company_folder if company_folder != "2026" else comp_name
            except Exception:
                company = company_folder
                ref_year = 2026
                ref_month = 0
            
            group_key = (company, ref_year, ref_month)
            
            if group_key not in file_map:
                file_map[group_key] = (ingestion_id, file_path)
            else:
                existing_id, existing_path = file_map[group_key]
                if ingestion_id > existing_id:
                    file_map[group_key] = (ingestion_id, file_path)

    selected_files = [path for _, path in file_map.values()]
    print(f"Selected {len(selected_files)} active latest file(s) for Silver consolidation.\n")

    all_dfs = []
    for file_path in selected_files:
        ext = file_path.suffix.lower()
        try:
            if ext == ".parquet":
                df_temp = pd.read_parquet(file_path)
            elif ext in [".xlsx", ".xls"]:
                df_temp = pd.read_excel(file_path)
            elif ext == ".csv":
                df_temp = pd.read_csv(file_path, sep=None, engine="python")
            
            company_code_fallback = re.sub(r"\D", "", file_path.parent.name)
            if "company_code" not in df_temp.columns and company_code_fallback:
                df_temp["company_code"] = company_code_fallback

            all_dfs.append(df_temp)
            print(f" -> Loaded: {file_path.name}")
        except Exception as e:
            print(f"    [Warning] Failed to read file '{file_path.name}': {e}")

    if not all_dfs:
        raise ValueError(f"No valid operational data files found in {base_folder}")

    df_combined = pd.concat(all_dfs, ignore_index=True)
    df_combined.columns = df_combined.columns.str.strip().str.lower()
    return df_combined


# ==============================================
# 3. LOADING DIMENSION AND REFERENCE DATASETS
# ==============================================

print("\n" + "=" * 70)
print("INDEXING REFERENCE DATASETS (DELEGATARIAS, VEHICLES, CONTRACTS, SCHEDULE)")
print("=" * 70)

# [0/3] Load Companies Dimension Table from Gold
print(f"[0/3] Loading Companies Dimension from: {bmi_dim_companies_path.name}...")
df_companies_dim = pd.read_parquet(bmi_dim_companies_path)
df_companies_dim.columns = df_companies_dim.columns.str.strip().str.lower()

# Match exato das colunas conforme dim_companies.parquet
col_code = "delegated_company_cod"
col_name = "delegated_company_name"

delegatarias_map = {}
for _, row in df_companies_dim.iterrows():
    c_code = normalize_text_key(row.get(col_code))
    c_name = str(row.get(col_name) or "").strip()
    if c_code:
        delegatarias_map[c_code] = c_name

print("[1/3] Loading Consolidated Vehicles Parquet...")
df_sgti_veic = pd.read_parquet(db_sgti_vehicles)
df_sgti_veic.columns = df_sgti_veic.columns.str.strip().str.lower()

vehicle_index = {}
vehicle_global_index = {}

for row in df_sgti_veic.to_dict("records"):
    plate = clean_plate(row.get("license_plate"))
    month = safe_int(row.get("file_month") or row.get("month") or 0)
    
    if plate:
        c_code = normalize_text_key(row.get("delegated_company_cod"))
        c_name = str(row.get("delegated_company_name") or "").strip()
        
        if c_code in delegatarias_map:
            c_name = delegatarias_map[c_code]

        vehicle_data = {
            "license_plate": plate,
            "company_code": c_code,
            "company_name": c_name,
            "authorized_to_share": str(row.get("authorized_companies_to_share") or "").strip(),
            "month": month,
            "bodywork_year": row.get("bodywork_year"),
        }
        
        vehicle_index[f"{plate}|{month}"] = vehicle_data
        if plate not in vehicle_global_index:
            vehicle_global_index[plate] = vehicle_data


def locate_in_sgti_vehicles(plate, ref_month):
    candidates = [clean_plate(plate)]
    eq = equivalent_plate(plate)
    if eq:
        candidates.append(eq)
        
    for p_search in candidates:
        for month in get_month_search_list(ref_month):
            key = f"{p_search}|{month}"
            if key in vehicle_index:
                data = vehicle_index[key].copy()
                data["distance"] = abs(month - ref_month) if ref_month > 0 else 0
                return data
        
        if p_search in vehicle_global_index:
            data = vehicle_global_index[p_search].copy()
            data["distance"] = 0
            return data
            
    return None


print("[2/3] Loading Consolidated Services / Contracts Parquet...")
df_sgti_contracts = pd.read_parquet(db_sgti_contracts)
df_sgti_contracts.columns = df_sgti_contracts.columns.str.strip().str.lower()

contract_index = {}
for row in df_sgti_contracts.to_dict("records"):
    try:
        service_id = normalize_text_key(row.get("service_id"))
        month_arq = safe_int(row.get("months_of_operation") or row.get("file_month") or 0)

        total_km = (
            safe_float(row.get("km_surface_type_1"))
            + safe_float(row.get("km_surface_type_2"))
            + safe_float(row.get("km_surface_type_3"))
            + (safe_float(row.get("km_surface_type_1_e/e")) * 2)
            + (safe_float(row.get("km_surface_type_2_e/e")) * 2)
            + (safe_float(row.get("km_surface_type_3_e/e")) * 2)
        )

        c_code = normalize_text_key(row.get("delegated_company_cod"))
        c_name = str(row.get("delegated_company_name") or "").strip()
        if c_code in delegatarias_map:
            c_name = delegatarias_map[c_code]

        contract_data = {
            "delegated_company_cod": c_code,
            "delegated_company_name": c_name,
            "total_line_km": total_km,
        }

        contract_index[f"{service_id}|{month_arq}"] = contract_data
        contract_index[service_id] = contract_data
    except Exception:
        continue

print("[3/3] Loading Expanded Schedule Parquet...")
df_gold = pd.read_parquet(db_expanded_schedule)
df_gold.columns = df_gold.columns.str.strip().str.lower()

gold_schedule_index = {}
for row in df_gold.to_dict("records"):
    d_norm = normalize_date(row.get("date"))
    l_norm = normalize_text_key(row.get("service_id"))
    h_norm = normalize_time_str(row.get("schedule_time"))

    key = (d_norm, l_norm)
    if key not in gold_schedule_index:
        gold_schedule_index[key] = []
    if h_norm:
        gold_schedule_index[key].append(h_norm)

# ==============================================
# 4. UNIFIED PROCESSING
# ==============================================

print("\n" + "=" * 70)
print("STARTING UNIFIED PROCESSING ON OPERATIONAL DATASETS")
print("=" * 70)

df_op = load_operational_data_from_folder(operational_dir)

op_records = df_op.to_dict("records")
print(f"Total operational records loaded: {len(op_records):,}\n")

results = []
for i, row in enumerate(op_records):
    if i > 0 and i % 50000 == 0:
        print(f"      Progress: {i:,} / {len(op_records):,}")

    plate_used = select_vehicle(row)
    
    date_op_norm = normalize_date(row.get("date"))
    time_op_norm = normalize_time_str(row.get("schedule_time"))
    
    ref_month = 0
    ref_year = None
    if date_op_norm:
        try:
            parsed_dt = pd.to_datetime(date_op_norm)
            ref_month = parsed_dt.month
            ref_year = parsed_dt.year
        except Exception:
            pass

    status, reason, match_type, requires_review = "approved", "ok", "", "no"
    normalized_plate, matched_month, month_distance = plate_used, ref_month, 0
    sgti_code = normalize_text_key(row.get("company_code"))
    sgti_name = delegatarias_map.get(sgti_code, "")
    sharing_text = ""
    bodywork_year = None

    if plate_used == "":
        status, reason, requires_review = (
            "missing_plate",
            "empty_vehicle_and_replacements",
            "yes",
        )
    elif not is_valid_plate(plate_used):
        status, reason, requires_review = (
            "invalid_plate",
            "invalid_format",
            "yes",
        )
    else:
        found = locate_in_sgti_vehicles(plate_used, ref_month)
        if found:
            normalized_plate = found["license_plate"]
            sgti_code = found["company_code"] or sgti_code
            sgti_name = found["company_name"] or delegatarias_map.get(sgti_code, "")
            sharing_text = found["authorized_to_share"]
            matched_month = found["month"]
            month_distance = found["distance"]
            bodywork_year = found["bodywork_year"]
            
            if plate_used == normalized_plate:
                match_type = (
                    "direct_match"
                    if month_distance == 0
                    else "close_period_match"
                )
            else:
                match_type = (
                    "old_to_mercosul"
                    if len(plate_used) == 7 and plate_used[4].isdigit()
                    else "mercosul_to_old"
                )
        else:
            match_type = "fallback_accepted"
            status = "approved_with_reservation"
            reason = "vehicle_not_registered_in_sgti"
            requires_review = "no"

    contract_company_code, contract_company_name, company_matches, total_line_km = (
        None,
        None,
        "yes",
        None,
    )
    contract_validation = "ok"

    try:
        service_id_norm = normalize_text_key(row.get("service_id"))
        contract_key_month = f"{service_id_norm}|{ref_month}"

        info_c = None
        if contract_key_month in contract_index:
            info_c = contract_index[contract_key_month]
        elif service_id_norm in contract_index:
            info_c = contract_index[service_id_norm]

        if info_c:
            contract_company_code = info_c["delegated_company_cod"]
            contract_company_name = info_c["delegated_company_name"]
            total_line_km = info_c["total_line_km"]

            contract_code_str = normalize_text_key(contract_company_code)
            sgti_code_str = normalize_text_key(sgti_code)

            if sgti_code_str == "" or contract_code_str == "" or sgti_code_str == contract_code_str:
                company_matches = "yes"
                contract_validation = "ok"
            else:
                company_matches = "no"
                contract_validation = "divergent"
        else:
            contract_validation = "ok"
    except Exception:
        contract_validation = "ok"

    trip_type_norm = str(row.get("type") or "").strip().lower()
    op_key = (date_op_norm, service_id_norm)

    (
        schedule_status,
        schedule_reason,
        scheduled_time,
        minute_difference,
        suggested_action,
    ) = ("ok", None, time_op_norm, 0, None)
    
    is_declared_reinforcement = trip_type_norm in ["reforço", "reforco", "reinforcement"]

    if op_key in gold_schedule_index:
        gold_times = gold_schedule_index[op_key]
        if is_declared_reinforcement:
            schedule_status = "declared_reinforcement"
        else:
            op_mins = time_to_minutes(time_op_norm)
            best_time = gold_times[0] if gold_times else ""
            min_diff = abs(op_mins - time_to_minutes(best_time))

            for t_time in gold_times[1:]:
                diff = abs(op_mins - time_to_minutes(t_time))
                if diff < min_diff:
                    min_diff = diff
                    best_time = t_time

            scheduled_time = best_time
            minute_difference = min_diff

            if min_diff <= 30:
                schedule_status = "ok"
            else:
                schedule_status = "approximate_match"

    results.append({
        "ingestion_id": row.get("ingestion_id"),
        "source_file": row.get("source_file"),
        "source_row": row.get("source_row"),
        "service_id": row.get("service_id"),
        "date": row.get("date"),
        "schedule_time": row.get("schedule_time"),
        "path": row.get("path"),
        "trip_type": row.get("type"),
        "reference_month": ref_month,
        "reference_year": ref_year,
        "original_vehicle": row.get("vehicle"),
        "replacement_vehicle_1": row.get("replacement_vehicle_1"),
        "replacement_reason_1": row.get("replacement_reason_1"),
        "replacement_vehicle_2": row.get("replacement_vehicle_2"),
        "replacement_reason_2": row.get("replacement_reason_2"),
        "plate_used": plate_used,
        "normalized_plate": normalized_plate,
        "bodywork_year": bodywork_year,
        "sgti_matched_month": matched_month,
        "month_distance": month_distance,
        "sgti_owner_code": sgti_code,
        "sgti_owner_name": sgti_name,
        "sharing_text": sharing_text,
        "match_type": match_type,
        "vehicle_status": status,
        "status_reason": reason,
        "requires_review": requires_review,
        "contract_company_code": contract_company_code,
        "contract_company_name": contract_company_name,
        "company_matches": company_matches,
        "contract_validation": contract_validation,
        "total_line_km": total_line_km,
        "schedule_status": schedule_status,
        "schedule_reason": schedule_reason,
        "scheduled_time": scheduled_time,
        "time_difference_minutes": minute_difference,
        "suggested_action": suggested_action,
    })

df_audit = pd.DataFrame(results)

# ==============================================
# 5. LINKAGE ADJUSTMENTS AND CONSOLIDATION
# ==============================================

print("\nFinalizing company linkage rules and typo checks...")

def calculate_final_status(row):
    veic_ok = str(row["vehicle_status"]).lower() in [
        "approved",
        "approved_with_reservation",
        "approved_by_sharing",
    ]
    serv_ok = str(row["contract_validation"]).lower() in ["ok", "direct_match"]
    prog_ok = str(row["schedule_status"]).lower() in [
        "ok",
        "approximate_match",
        "declared_reinforcement",
    ]

    inc_vehicle = not veic_ok
    inc_service = not serv_ok
    inc_schedule = not prog_ok

    total_inc = sum([inc_vehicle, inc_service, inc_schedule])

    if total_inc == 0:
        return "approved"
    elif total_inc > 1:
        return "multiple_inconsistencies"
    elif inc_vehicle:
        return "vehicle_inconsistency"
    elif inc_service:
        return "service_inconsistency"
    else:
        return "schedule_inconsistency"


df_audit["final_status"] = df_audit.apply(calculate_final_status, axis=1)

# ==============================================
# 6. SPLITTING AND MULTI-FORMAT EXPORT
# ==============================================

df_quarantine = df_audit[
    df_audit["vehicle_status"].isin(["missing_plate", "invalid_plate"])
].copy()

df_sanitized = df_audit[
    ~df_audit["vehicle_status"].isin(["missing_plate", "invalid_plate"])
].copy()

mask_needs_audit = (df_audit["final_status"] != "approved") | (
    df_audit["requires_review"] == "yes"
)
df_general_audit = df_audit[mask_needs_audit].copy()

print("\nApplying explicit data type conversions and enforcing lowercase columns...")
df_sanitized = convert_types_for_export(df_sanitized)
df_audit = convert_types_for_export(df_audit)
df_quarantine = convert_types_for_export(df_quarantine)
df_general_audit = convert_types_for_export(df_general_audit)

# ----------------------------------------------
# A. EXPORT TO PARQUET (.parquet)
# ----------------------------------------------
print("Writing Parquet files (.parquet)...")
df_sanitized.to_parquet(parquet_sanitized, index=False)
df_audit.to_parquet(parquet_audit, index=False)
df_general_audit.to_parquet(parquet_general_audit, index=False)

# ----------------------------------------------
# B. EXPORT TO EXCEL (.xlsx)
# ----------------------------------------------
print("Generating Excel file (.xlsx) for Quarantine...")
if len(df_quarantine) <= 1048576:
    df_quarantine.to_excel(xlsx_quarantine, index=False)
else:
    print(
        "Warning: Quarantine dataset exceeded Excel limit (1,048,576 rows)."
    )

# ==============================================
# 7. FINAL SUMMARY
# ==============================================

total_execution_time = time.time() - overall_start_time

print("\n" + "=" * 70)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 70)

print("\nfinal_status Distribution in Full Dataset:")
print(df_audit["final_status"].value_counts())

print(f"\nTotal Records Analyzed           : {len(df_audit):,}")
print(f"Sanitized Records (Silver)       : {len(df_sanitized):,}")
print(f"Discarded (Quarantine)           : {len(df_quarantine):,}")

print(f"\nTotal execution time: {total_execution_time:,.1f} seconds")
print("=" * 70)