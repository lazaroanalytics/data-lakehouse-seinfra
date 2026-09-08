from pathlib import Path
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"

# Operational directory resolution
RAW_DIR = DATA_DIR / "ingestion" / "operational" / "2026" / "new-receipts"
BRONZE_DIR = DATA_DIR / "bronze" / "operational" / "2026"

# Audit Dead-Letter Queue (DLQ) file
AUDIT_EXCEL = BRONZE_DIR / "audit_discarded_data.xlsx"


def reincorporate_audited_trips():
    """Reincorporates manually corrected records from the DLQ into the Bronze layer."""
    if not AUDIT_EXCEL.exists():
        print("❌ Audit ledger file not found.")
        return

    df_audit = pd.read_excel(AUDIT_EXCEL, dtype=str)

    # Filter records flagged as resolved by human auditor
    df_corrected = df_audit[df_audit["CORRIGIDO_POR_HUMANO"].str.upper() == "SIM"].copy()

    if df_corrected.empty:
        print("ℹ️ No records flagged with 'SIM' in column CORRIGIDO_POR_HUMANO.")
        return

    print(f"🔄 Reincorporating {len(df_corrected)} corrected record(s)...")

    # 1. Update Bronze Parquet Dataset
    bronze_parquet = BRONZE_DIR / "trips_operational.parquet"
    if bronze_parquet.exists():
        df_bronze = pd.read_parquet(bronze_parquet)
    else:
        df_bronze = pd.DataFrame()

    cols_to_drop = ["MOTIVO_EXPURGO", "CORRIGIDO_POR_HUMANO", "JUSTIFICATIVA_ALTERACAO"]
    df_corrected_bronze = df_corrected.drop(columns=[c for c in cols_to_drop if c in df_corrected.columns])

    df_updated_bronze = pd.concat([df_bronze, df_corrected_bronze], ignore_index=True)
    df_updated_bronze.to_parquet(bronze_parquet, index=False)
    print(f"🟢 [BRONZE] {len(df_corrected)} records successfully integrated into: {bronze_parquet.name}")

    # 2. Generate internally modified RAW copies for auditing lineage
    grouped = df_corrected.groupby("NOME_ARQUIVO_ORIGEM")

    for original_filename, group in grouped:
        original_path = RAW_DIR / original_filename
        
        modified_filename = original_filename.replace(".xlsx", "_internally_modified.xlsx")
        modified_path = RAW_DIR / modified_filename

        base_path = modified_path if modified_path.exists() else original_path

        if base_path.exists():
            df_original = pd.read_excel(base_path, dtype=str)

            for _, row_corrected in group.iterrows():
                row_index = int(row_corrected["LINHA_ORIGEM"]) - 2
                clean_row = row_corrected.drop(["NOME_ARQUIVO_ORIGEM", "LINHA_ORIGEM"] + cols_to_drop, errors="ignore")

                if 0 <= row_index < len(df_original):
                    df_original.iloc[row_index] = clean_row
                else:
                    df_original = pd.concat([df_original, pd.DataFrame([clean_row])], ignore_index=True)

            df_original.to_excel(modified_path, index=False, engine="openpyxl")
            print(f"📝 [RAW] Internal modification file updated: {modified_path.name}")

    # 3. Retain uncorrected pending items in the audit file
    df_remaining = df_audit[df_audit["CORRIGIDO_POR_HUMANO"].str.upper() != "SIM"]
    df_remaining.to_excel(AUDIT_EXCEL, index=False, engine="openpyxl")
    print(f"🧹 [AUDIT] Audit ledger updated. {len(df_remaining)} pending issue(s) remain.")


if __name__ == "__main__":
    reincorporate_audited_trips()