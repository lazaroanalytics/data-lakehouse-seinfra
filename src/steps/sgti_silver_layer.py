import calendar
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import time
import pandas as pd

# ==============================================
# DIRECTORY CONFIGURATION (PROJECT_ROOT BASED)
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"

INPUT_DIR = DATA_DIR / "ingestion" / "sgti" / "2026"
BRONZE_DIR = DATA_DIR / "bronze" / "sgti" / "2026"
SILVER_DIR = DATA_DIR / "silver" / "sgti" / "2026"

CACHE_METADATA_FILE = SILVER_DIR / ".consolidation_cache.json"
LOG_TXT_FILE = SILVER_DIR / "consolidation_log.txt"

# ==============================================
# SCHEMA & MAPPING CONFIGURATION
# ==============================================
DATASET_SCHEMA_CONFIG = {
    "schedule": {
        "service_id": "service_id",
        "path": "path",
        "schedule_time": "schedule_time",
        "number_of_vehicles": "number_of_vehicles",
        "file_month": "file_month",
        "holiday": "holiday",
        "weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
        "months": [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]
    },
    "service": {
        "service_id": "service_id",
        "service_name": "service_name",
        "delegated_company_cod": "delegated_company_cod",
        "delegated_company_name": "delegated_company_name",
        "delegated_company_status": "delegated_company_status",
        "service_status": "servie_status",
        "contract_expiration": "contract_expiration",
        "contract_number": "contract_number",
        "utilization": "utilization",
        "necessary_vehicles": "necessary_vehicles",
        "ticket_cost": "ticket_cost",
        "trips_per_week": "trips_per_week",
        "months_of_operation": "months_of_operation"
    },
    "vehicles": {
        "registration_date": "registration_date",
        "utilization": "utilization",
        "delegated_company_code": "delegated_company_code",
        "delegated_company_name": "delegated_company_name",
        "chassis_manufacturer": "chassis_manufacturer",
        "chassis_model": "chassis_model",
        "chassis_year": "chassis_year",
        "situation": "situation",
        "deactivation_date": "deactivation_date",
        "bodywork_manufacturer": "bodywork_manufacturer",
        "bodywork_model": "bodywork_model",
        "colors": "colors",
        "equipment": "equipment",
        "fuel": "fuel"
    }
}

# ==============================================
# CONSTANTS & MAPS FOR SCHEDULE EXPANSION
# ==============================================
EXPANSION_YEAR = 2026

HOLIDAYS = {
    f"{EXPANSION_YEAR}-01-01",  # New Year's Day
    f"{EXPANSION_YEAR}-02-16",  # Carnival
    f"{EXPANSION_YEAR}-02-17",  # Carnival
    f"{EXPANSION_YEAR}-04-03",  # Good Friday
    f"{EXPANSION_YEAR}-04-21",  # Tiradentes Day
    f"{EXPANSION_YEAR}-05-01",  # Labor Day
    f"{EXPANSION_YEAR}-06-04",  # Corpus Christi
    f"{EXPANSION_YEAR}-09-07",  # Independence Day
    f"{EXPANSION_YEAR}-10-12",  # Our Lady of Aparecida
    f"{EXPANSION_YEAR}-11-02",  # All Souls' Day
    f"{EXPANSION_YEAR}-11-15",  # Republic Proclamation Day
    f"{EXPANSION_YEAR}-11-20",  # Black Awareness Day
    f"{EXPANSION_YEAR}-12-25",  # Christmas
}

WEEKDAYS_MAP = {
    0: ("monday", "Monday"),
    1: ("tuesday", "Tuesday"),
    2: ("wednesday", "Wednesday"),
    3: ("thursday", "Thursday"),
    4: ("friday", "Friday"),
    5: ("saturday", "Saturday"),
    6: ("sunday", "Sunday"),
}

MONTHS_MAP = {
    "january": ("January", 1),
    "february": ("February", 2),
    "march": ("March", 3),
    "april": ("April", 4),
    "may": ("May", 5),
    "june": ("June", 6),
    "july": ("July", 7),
    "august": ("August", 8),
    "september": ("September", 9),
    "october": ("October", 10),
    "november": ("November", 11),
    "december": ("December", 12),
}


def parse_file_metadata(file_path: Path):
    parts = file_path.stem.split("_")
    dataset_type = parts[0]
    month = parts[-1]
    return dataset_type, month


def calculate_group_hash(file_list: list) -> str:
    hasher = hashlib.md5()
    for item in sorted(file_list, key=lambda x: x["path"].name):
        path = item["path"]
        stat = path.stat()
        signature = f"{path.name}_{stat.st_size}_{stat.st_mtime}"
        hasher.update(signature.encode("utf-8"))
    return hasher.hexdigest()


def load_cache_metadata() -> dict:
    if CACHE_METADATA_FILE.exists():
        try:
            return json.loads(CACHE_METADATA_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_cache_metadata(cache_data: dict):
    try:
        CACHE_METADATA_FILE.write_text(json.dumps(cache_data, indent=2))
    except Exception as e:
        print(f"⚠️ Warning: Failed to persist cache metadata: {e}")


def append_update_log(dataset_type: str, file_list: list, total_rows_raw: int, total_rows_expanded: int = None):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = [
        "==================================================",
        f"EXECUTION TIMESTAMP : {now_str}",
        f"PROCESSED DATASET   : {dataset_type.upper()}",
        f"CONSOLIDATED ROWS   : {total_rows_raw:,}",
    ]

    if total_rows_expanded is not None:
        log_entry.append(f"EXPANDED ROWS       : {total_rows_expanded:,}")

    log_entry.append("SOURCE FILES MOVED TO BRONZE:")

    for item in sorted(file_list, key=lambda x: x["path"].name):
        path = item["path"]
        stat = path.stat()
        mtime_str = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        log_entry.append(f"  - {path.name} (Last Modified: {mtime_str})")

    log_entry.append("==================================================\n\n")

    try:
        with open(LOG_TXT_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(log_entry))
        print(f"   ├─ 📝 Consolidation log updated: {LOG_TXT_FILE.name}")
    except Exception as e:
        print(f"   ├─ ⚠️ Warning: Failed to write to log file: {e}")


def expand_schedule_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("   ⚡ Initiating Schedule expansion pipeline...")
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    cfg = DATASET_SCHEMA_CONFIG["schedule"]

    time_col = cfg["schedule_time"]
    line_col = cfg["service_id"]
    path_col = cfg["path"]
    veh_col = cfg["number_of_vehicles"]
    file_month_col = cfg["file_month"]
    holiday_col = cfg["holiday"]

    if time_col in df.columns:
        df[time_col] = (
            df[time_col]
            .astype(str)
            .str.strip()
            .apply(lambda x: x.split(" ")[-1] if " " in str(x) else x)
        )

    if file_month_col in df.columns:
        print("   ├─ Deduplicating records by latest source file month...")
        before_count = len(df)

        def get_month_order(val):
            if pd.isna(val):
                return 0
            val_str = str(val).strip().lower()
            if val_str in MONTHS_MAP:
                return MONTHS_MAP[val_str][1]
            try:
                return int(val)
            except ValueError:
                return 0

        df["_MONTH_ORDER"] = df[file_month_col].apply(get_month_order)
        comparison_cols = [c for c in df.columns if c not in [file_month_col, "_MONTH_ORDER"]]

        df = df.sort_values(by="_MONTH_ORDER", ascending=True)
        df = df.drop_duplicates(subset=comparison_cols, keep="last")
        df = df.drop(columns=["_MONTH_ORDER"]).reset_index(drop=True)
        print(f"   ├─ Deduplication complete: {before_count - len(df):,} redundant rows removed.")

    expanded_rows = []

    for idx, row in df.iterrows():
        for month_key in cfg["months"]:
            if month_key not in df.columns:
                continue

            month_active = str(row.get(month_key, "")).strip().upper()
            if month_active != "Y":
                continue

            month_name, month_num = MONTHS_MAP[month_key]
            last_day = calendar.monthrange(EXPANSION_YEAR, month_num)[1]

            for day in range(1, last_day + 1):
                date_obj = datetime(EXPANSION_YEAR, month_num, day)
                date_str = date_obj.strftime("%Y-%m-%d")
                weekday_key, weekday_name = WEEKDAYS_MAP[date_obj.weekday()]

                is_holiday = date_str in HOLIDAYS

                runs_on_day = str(row.get(weekday_key, "")).strip().upper() == "Y"
                runs_on_holiday = str(row.get(holiday_col, "")).strip().upper() == "Y"

                if is_holiday:
                    if not (runs_on_day or runs_on_holiday):
                        continue
                else:
                    if not runs_on_day:
                        continue

                expanded_rows.append(
                    {
                        "DATE": date_str,
                        "MONTH_NAME": month_name,
                        "MONTH_NUMBER": month_num,
                        "DAY_OF_WEEK": weekday_name,
                        "IS_HOLIDAY": "Y" if is_holiday else "N",
                        "FILE_MONTH": row.get(file_month_col) if file_month_col in df.columns else None,
                        "SERVICE_ID": row.get(line_col),
                        "PATH": row.get(path_col),
                        "SCHEDULE_TIME": row.get(time_col),
                        "NUMBER_OF_VEHICLES": row.get(veh_col),
                    }
                )

    expanded_df = pd.DataFrame(expanded_rows)
    print(f"   └─ Expansion complete: {len(expanded_df):,} daily trip instances generated.")
    return expanded_df


def consolidate_sgti_datasets(force_rebuild: bool = False):
    if not INPUT_DIR.exists():
        print(f"❌ Input directory not found: {INPUT_DIR}")
        return

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    valid_extensions = {".xlsx", ".xls", ".csv", ".parquet"}
    files = [
        f
        for f in INPUT_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in valid_extensions and not f.name.startswith("~$")
    ]

    if not files:
        print(f"ℹ️ No new SGTI files found for ingestion in: {INPUT_DIR}")
        return

    dataset_groups = defaultdict(list)
    for file in files:
        dataset_type, month = parse_file_metadata(file)
        dataset_groups[dataset_type].append({"path": file, "month": month})

    print(
        f"🚀 Found {len(files)} file(s) across {len(dataset_groups)} SGTI dataset category(ies).\n"
    )

    cache_metadata = load_cache_metadata()

    for dataset_type, file_list in dataset_groups.items():
        consolidated_filename = f"consolidated_{dataset_type}.parquet"
        consolidated_path = SILVER_DIR / consolidated_filename

        expanded_filename = f"expanded_{dataset_type}.parquet"
        expanded_path = SILVER_DIR / expanded_filename

        current_group_hash = calculate_group_hash(file_list)
        cached_group_hash = cache_metadata.get(dataset_type)

        is_schedule = dataset_type.lower() == "schedule"
        files_exist = (
            consolidated_path.exists() and expanded_path.exists()
            if is_schedule
            else consolidated_path.exists()
        )

        if not force_rebuild and files_exist and cached_group_hash == current_group_hash:
            print(
                f"✨ [CACHE HIT] Category '{dataset_type}' is up to date. Skipping re-processing."
            )
            continue

        months = [item["month"] for item in file_list]

        seen_months = set()
        duplicate_months = set()
        for m in months:
            if m in seen_months:
                duplicate_months.add(m)
            else:
                seen_months.add(m)

        if duplicate_months:
            print(
                f"🚨 [ABORTED] Duplicate reference month(s) {sorted(list(duplicate_months))} found for '{dataset_type}'."
            )
            print(
                f"   └─ Skipping consolidation for '{dataset_type}' due to month collision.\n"
            )
            continue

        for target_path in [consolidated_path, expanded_path if is_schedule else None]:
            if target_path and target_path.exists():
                try:
                    target_path.unlink()
                    print(f"🧹 Stale output file purged: {target_path.name}")
                except Exception as e:
                    print(f"   ├─ ❌ Failed to purge target file {target_path.name}: {e}")

        df_list = []
        print(f"📦 Stacking dataset category '{dataset_type}':")
        for item in file_list:
            file_path = item["path"]
            try:
                if file_path.suffix.lower() in [".xlsx", ".xls"]:
                    df = pd.read_excel(file_path)
                elif file_path.suffix.lower() == ".csv":
                    df = pd.read_csv(file_path)
                elif file_path.suffix.lower() == ".parquet":
                    df = pd.read_parquet(file_path)

                df_list.append(df)
                print(
                    f"   ├─ Loaded: {file_path.name} ({len(df)} rows | Month: {item['month']})"
                )
            except Exception as e:
                print(f"   ├─ ❌ Failed to read {file_path.name}: {e}")

        if df_list:
            consolidated_df = pd.concat(df_list, ignore_index=True)

            if "service_id" in consolidated_df.columns:
                consolidated_df["service_id"] = consolidated_df["service_id"].astype(str)

            consolidated_df.to_parquet(consolidated_path, index=False)
            print(
                f"   ├─ 🟢 [SILVER] Consolidated output saved ({len(consolidated_df):,} rows): {consolidated_path.name}"
            )

            total_expanded_rows = None

            if is_schedule:
                expanded_df = expand_schedule_dataframe(consolidated_df)
                expanded_df.to_parquet(expanded_path, index=False)
                total_expanded_rows = len(expanded_df)
                print(
                    f"   ├─ 🟢 [SILVER] Expanded output saved ({total_expanded_rows:,} rows): {expanded_path.name}"
                )

            append_update_log(
                dataset_type=dataset_type,
                file_list=file_list,
                total_rows_raw=len(consolidated_df),
                total_rows_expanded=total_expanded_rows,
            )

            cache_metadata[dataset_type] = current_group_hash
            save_cache_metadata(cache_metadata)

            for item in file_list:
                src_path = item["path"]
                dst_path = BRONZE_DIR / src_path.name
                
                for attempt in range(3):
                    try:
                        shutil.move(src_path, dst_path)
                        print(f"   ├─ 🚚 [BRONZE] Moved: {src_path.name} ➔ bronze/sgti/2026/")
                        break
                    except PermissionError:
                        if attempt < 2:
                            time.sleep(0.5)
                        else:
                            raise

            print()

if __name__ == "__main__":
    consolidate_sgti_datasets()