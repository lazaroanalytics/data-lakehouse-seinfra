from datetime import datetime
import gc
from pathlib import Path
import re
import shutil
import time
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"

# Busca a entrada tanto em landing quanto em ingestion
NEW_FOLDER = DATA_DIR / "ingestion" / "bmi" / "2026" / "new-receipts"

BASE_RAW_FOLDER = DATA_DIR / "ingestion" / "bmi" / "2026" / "raw"
BASE_BRONZE_FOLDER = DATA_DIR / "bronze" / "bmi" / "2026"

BASE_RAW_FOLDER.mkdir(parents=True, exist_ok=True)
BASE_BRONZE_FOLDER.mkdir(parents=True, exist_ok=True)

# ==============================================
# 2. HELPER FUNCTIONS
# ==============================================

def generate_ingestion_id(sequence_number: int) -> str:
    """Generates a unique batch ingestion ID: BMI + YY + MM + DD + Sequence."""
    now = datetime.now()
    yy = now.strftime("%y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")
    return f"BMI{yy}{mm}{dd}{sequence_number}"


def extract_metadata_from_filename(filename: str):
    """Extracts reference year and month from filename, fallback to current time."""
    stem = Path(filename).stem
    
    m1 = re.search(r"(\d{4})[-_]?(\d{1,2})", stem)
    if m1:
        return int(m1.group(1)), int(m1.group(2))

    m2 = re.search(r"(\d{1,2})[-_]?(\d{4})", stem)
    if m2:
        return int(m2.group(2)), int(m2.group(1))

    now = datetime.now()
    return now.year, now.month

# ==============================================
# 3. MAIN PIPELINE EXECUTION
# ==============================================

def execute_bmi_bronze_pipeline():
    overall_start_time = time.time()
    
    if not NEW_FOLDER.exists():
        print(f"⚠️ Input directory '{NEW_FOLDER}' not found. Creating path...")
        NEW_FOLDER.mkdir(parents=True, exist_ok=True)
        return

    excel_files = [
        f for f in list(NEW_FOLDER.glob("*.xlsx")) + list(NEW_FOLDER.glob("*.xls")) + list(NEW_FOLDER.glob("*.csv"))
        if not f.name.startswith("~$")
    ]

    if not excel_files:
        print(f"ℹ️ No new files found in landing zone: {NEW_FOLDER}")
        return

    print("\n" + "=" * 70)
    print(f"🚀 Found {len(excel_files)} file(s) in NEW_RECEIPTS for BMI Bronze ingestion.")
    print("=" * 70 + "\n")

    for sequence, file_path in enumerate(excel_files, start=1):
        filename = file_path.name
        ext = file_path.suffix.lower()
        ingestion_id = generate_ingestion_id(sequence)
        
        ref_year, ref_month = extract_metadata_from_filename(filename)
        ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"📄 Processing: {filename} | ID: {ingestion_id}")

        try:
            # 1. Read source data
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path, dtype=str)
            elif ext == ".csv":
                df = pd.read_csv(file_path, sep=None, engine="python", dtype=str)
            else:
                continue

            df.columns = df.columns.str.strip().str.lower()

            # 2. Add governance columns
            df.insert(0, "ingestion_id", ingestion_id)
            df["ingestion_at"] = ingestion_timestamp
            df["reference_month"] = ref_month
            df["reference_year"] = ref_year
            df["source_file"] = filename

            # 3. Save to RAW with Ingestion ID in filename
            raw_filename = f"{file_path.stem}_{ingestion_id}{file_path.suffix}"
            raw_destination_path = BASE_RAW_FOLDER / raw_filename

            shutil.copy2(file_path, raw_destination_path)
            print(f"   ├─ Raw file stored in RAW: {raw_destination_path.name}")

            # 4. Save to BRONZE as .xlsx with 'bronze_' prefix and Ingestion ID
            bronze_filename = f"bronze_{file_path.stem}_{ingestion_id}.xlsx"
            bronze_output_path = BASE_BRONZE_FOLDER / bronze_filename
            
            df.to_excel(bronze_output_path, index=False, engine="openpyxl")
            print(f"   ├─ BRONZE layer saved ({len(df)} rows): {bronze_output_path.name}")

            # 5. Remove original from input folder
            try:
                file_path.unlink()
            except PermissionError:
                del df
                gc.collect()
                file_path.unlink()

            print("   └─ ✅ File processed and moved successfully.\n")

        except Exception as e:
            print(f"   └─ ❌ Error processing {filename}: {e}\n")

    total_time = time.time() - overall_start_time
    print("=" * 70)
    print(f"🏁 BMI Bronze Layer Pipeline executed successfully in {total_time:.2f}s.")
    print("=" * 70)


if __name__ == "__main__":
    execute_bmi_bronze_pipeline()