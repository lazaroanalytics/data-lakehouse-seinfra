from datetime import datetime
import gc
import os
from pathlib import Path
import re
import shutil
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
NEW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "new-receipts"
BASE_RAW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "raw"
BASE_BRONZE_FOLDER = DATA_DIR / "bronze" / "operational"
DISCARDED_AUDIT_FILE = BASE_BRONZE_FOLDER / "2026" / "audit_discarded_data.xlsx"

# Mandatory operational schema columns
REQUIRED_COLUMNS = [
    "service_id",
    "date",
    "schedule_time",
    "path",
    "type",
    "vehicle"
]

# Validation patterns and domains
# Legacy plate (ABC-1234 / ABC1234) or Mercosul format (ABC1D23 / ABC-1D23)
REGEX_VEHICLE = r"^[A-Z]{3}-?\d{4}$|^[A-Z]{3}-?\d[A-Z]\d{2}$"

# Service ID formats (1 digit, 4 digits, 5 digits, or 4 digits + 1 letter)
REGEX_SERVICE_ID = r"^\d{1}$|^\d{4}$|^\d{5}$|^\d{4}[A-Za-z]$"

VALID_PATHS = {"OUTBOUND", "RETURN"}
VALID_TYPES = {"SPECIFIED", "REINFORCEMENT"}


# ==============================================
# 2. HELPER & AUDIT FUNCTIONS
# ==============================================

def generate_ingestion_id(sequence_number: int) -> str:
    """Generates a unique batch ingestion ID: OP + YY + MM + DD + Sequence."""
    now = datetime.now()
    yy = now.strftime("%y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    return f"OP{yy}{mm}{dd}{sequence_number}"


def extract_metadata_from_filename(filename: str):
    """Extracts company name, reference year, and reference month from filename."""
    stem = Path(filename).stem
    
    m1 = re.search(r"^(.*?)[_]+(\d{4})[_]+(\d{1,2})(?:_RET\d+)?$", stem, re.IGNORECASE)
    if m1:
        return m1.group(1).strip("_").lower(), int(m1.group(2)), int(m1.group(3))

    m2 = re.search(r"^(.*?)[_]+(\d{4})(\d{2})(?:_RET\d+)?$", stem, re.IGNORECASE)
    if m2:
        return m2.group(1).strip("_").lower(), int(m2.group(2)), int(m2.group(3))

    m3 = re.search(r"^(.*?)[_]+(\d{4})[-]+(\d{1,2})(?:_RET\d+)?$", stem, re.IGNORECASE)
    if m3:
        return m3.group(1).strip("_").lower(), int(m3.group(2)), int(m3.group(3))

    raise ValueError(f"Unable to extract reference Year and Month from filename: '{filename}'")


def is_empty_value(val):
    """Evaluates whether a value is null, empty string, or NaN representation."""
    if pd.isna(val):
        return True
    s_val = str(val).strip().lower()
    return s_val in ["", "nan", "none", "<na>", "null"]


def validate_row_cells(row, ref_year, ref_month):
    """Executes syntactic, structural, and business rule validations on a record."""
    # 1. Null / Empty checks
    for col in REQUIRED_COLUMNS:
        if is_empty_value(row.get(col)):
            return False, f"EMPTY_FIELD_{col.upper()}"

    # 2. Service ID validation
    service_id = str(row.get("service_id", "")).strip()
    if not re.match(REGEX_SERVICE_ID, service_id):
        return False, f"INVALID_SERVICE_ID_FORMAT ('{service_id}')"

    # 3. Vehicle plate validation
    vehicle = str(row.get("vehicle", "")).strip().upper()
    if not re.match(REGEX_VEHICLE, vehicle):
        return False, f"INVALID_VEHICLE_PLATE ('{vehicle}')"

    # 4. Path validation
    path_val = str(row.get("path", "")).strip().upper()
    if path_val not in VALID_PATHS:
        return False, f"INVALID_PATH_VALUE ('{path_val}')"

    # 5. Service type validation
    type_val = str(row.get("type", "")).strip().upper()
    if type_val not in VALID_TYPES:
        return False, f"INVALID_TYPE_VALUE ('{type_val}')"

    # 6. Date formatting and reference period consistency
    raw_date = str(row.get("date", "")).strip().split()[0].replace("-", "/")
    try:
        parsed_date = datetime.strptime(raw_date, "%Y/%m/%d")
        if parsed_date.year != ref_year or parsed_date.month != ref_month:
            return False, f"DATE_OUT_OF_PERIOD ({parsed_date.strftime('%Y/%m/%d')} != {ref_year}/{ref_month:02d})"
    except ValueError:
        return False, f"INVALID_DATE_FORMAT ('{raw_date}')"

    return True, "PASSED"


def process_bronze_ingestion(file_path, ingestion_id, is_rectification=False):
    """Processes input spreadsheet and splits records into valid and discarded datasets."""
    filename = file_path.name
    company_name, ref_year, ref_month = extract_metadata_from_filename(filename)

    df = pd.read_excel(file_path, dtype=str)

    if df.empty:
        del df
        gc.collect()
        return None, None, company_name, ref_year, ref_month

    df.columns = df.columns.str.strip().str.lower()

    valid_rows = []
    discarded_rows = []
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for idx, row in df.iterrows():
        is_valid, reason = validate_row_cells(row, ref_year, ref_month)
        
        row_dict = {
            "ingestion_id": ingestion_id,
            "is_rectification": is_rectification,
            "source_file": filename,
            "source_row": idx + 2,
            **row.to_dict()
        }
        
        if is_valid:
            valid_rows.append(row_dict)
        else:
            row_dict["rejection_reason"] = reason
            row_dict["discarded_at"] = ingestion_timestamp
            discarded_rows.append(row_dict)

    df_valid = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    df_discarded = pd.DataFrame(discarded_rows) if discarded_rows else pd.DataFrame()

    del df
    gc.collect()

    return df_valid, df_discarded, company_name, ref_year, ref_month


# ==============================================
# 3. MAIN PIPELINE EXECUTION
# ==============================================

def execute_bronze_pipeline():
    if not NEW_FOLDER.exists():
        print(f"⚠️ Input directory '{NEW_FOLDER}' not found. Creating path...")
        NEW_FOLDER.mkdir(parents=True, exist_ok=True)
        return

    excel_files = [
        f for f in list(NEW_FOLDER.glob("*.xlsx")) + list(NEW_FOLDER.glob("*.xls"))
        if not f.name.startswith("~$")
    ]

    if not excel_files:
        print(f"ℹ️ No new files found in landing zone: {NEW_FOLDER}")
        return

    print(f"🚀 Found {len(excel_files)} file(s) in NEW_RECEIPTS for Bronze ingestion.\n")
    
    all_discarded_records = []

    for sequence, file_path in enumerate(excel_files, start=1):
        filename = file_path.name
        ingestion_id = generate_ingestion_id(sequence)
        
        try:
            company, year, month = extract_metadata_from_filename(filename)
        except Exception as e:
            print(f"   └─ ❌ Error parsing metadata from {filename}: {e}\n")
            continue

        company_raw_folder = BASE_RAW_FOLDER / company
        pattern_month = f"_{year}_{month:02d}"
        
        # Check existing RAW storage for rectification flag
        existing_month_files = []
        if company_raw_folder.exists():
            existing_month_files = [
                f for f in company_raw_folder.glob("*.xlsx")
                if f.is_file() and pattern_month in f.name
            ]

        is_rectification = len(existing_month_files) > 0

        print(f"📄 Processing: {filename} | ID: {ingestion_id} | Rectification: {is_rectification}")

        df_valid, df_discarded, company, year, month = process_bronze_ingestion(
            file_path, ingestion_id, is_rectification=is_rectification
        )

        if df_valid is None or (df_valid.empty and df_discarded.empty):
            print("   └─ ⚠️ Empty file. Skipping.")
            continue

        print(f"   ├─ Company: {company} | Period: {year}-{month:02d}")

        # 1. RAW Storage Handling
        company_raw_folder.mkdir(parents=True, exist_ok=True)

        if is_rectification:
            target_raw_folder = company_raw_folder / "retificadas"
            target_raw_folder.mkdir(parents=True, exist_ok=True)
            print("   ├─ 🔄 Prior version detected! Storing copy in 'raw/.../retificadas'.")
        else:
            target_raw_folder = company_raw_folder

        raw_filename = f"{file_path.stem}_{ingestion_id}{file_path.suffix}"
        raw_destination_path = target_raw_folder / raw_filename

        shutil.copy2(file_path, raw_destination_path)
        
        try:
            file_path.unlink()
        except PermissionError:
            gc.collect()
            file_path.unlink()

        print(f"   ├─ Raw file stored in RAW: {raw_destination_path.name}")

        # 2. Bronze Storage Handling
        target_bronze_folder = BASE_BRONZE_FOLDER / str(year) / company
        target_bronze_folder.mkdir(parents=True, exist_ok=True)
        
        bronze_filename = f"bronze_{file_path.stem}_{ingestion_id}{file_path.suffix}"
        bronze_output_path = target_bronze_folder / bronze_filename
        df_valid.to_excel(bronze_output_path, index=False, engine="openpyxl")
        print(f"   ├─ BRONZE layer saved ({len(df_valid)} valid rows): {bronze_output_path.name}")

        # 3. DLQ Tracking
        if not df_discarded.empty:
            all_discarded_records.append(df_discarded)
            print(f"   └─ ⚠️ Discarded records ({len(df_discarded)} rows) routed to audit queue.")
        else:
            print("   └─ ✅ 100% schema compliance (0 discarded rows).")
        print()

    # 4. DLQ Audit Persistence
    if all_discarded_records:
        BASE_BRONZE_FOLDER.mkdir(parents=True, exist_ok=True)
        df_new_discards = pd.concat(all_discarded_records, ignore_index=True)

        if DISCARDED_AUDIT_FILE.exists():
            df_existing_discards = pd.read_excel(DISCARDED_AUDIT_FILE, dtype=str)
            df_final_discards = pd.concat([df_existing_discards, df_new_discards], ignore_index=True)
        else:
            df_final_discards = df_new_discards

        df_final_discards.to_excel(DISCARDED_AUDIT_FILE, index=False, engine="openpyxl")
        print(f"🚨 Audit Dead-Letter Queue (DLQ) updated ({len(df_final_discards)} total discards): {DISCARDED_AUDIT_FILE.name}\n")

    print("🏁 Bronze Layer Pipeline executed successfully.")


if __name__ == "__main__":
    execute_bronze_pipeline()