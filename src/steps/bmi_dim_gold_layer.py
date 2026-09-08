from datetime import datetime
import gc
from pathlib import Path
import re
import time
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION (Pathlib)
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_BMI_DIR = DATA_DIR / "bronze" / "bmi" / "2026"
GOLD_BMI_DIR = DATA_DIR / "gold" / "bmi" / "2026"

GOLD_BMI_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================
# 2. HELPER FUNCTIONS
# ==============================================

def identify_dataset_name(filename: str) -> str:
    """
    Extrai o nome base da dimensão garantindo o prefixo 'bmi_'.
    Exemplo: 'bronze_bmi_dim_companies_BMI2609081.xlsx' -> 'bmi_dim_companies'
             'bronze_dim_companies_BMI2609081.xlsx'     -> 'bmi_dim_companies'
    """
    stem = Path(filename).stem.lower()
    
    # 1. Remove prefixo 'bronze_'
    if stem.startswith("bronze_"):
        stem = stem[7:]
    
    # 2. Remove sufixos de Ingestion ID (ex: _bmi2609081 ou _ing...) e datas
    stem = re.sub(r"[_|-]bmi\d+$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_|-]ing_.*$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_|-]\d{4}[_|-]?\d{2}$", "", stem)
    
    # 3. Garante que o nome final comece com 'bmi_'
    if not stem.startswith("bmi_"):
        stem = f"bmi_{stem}"
        
    return stem


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza os nomes das colunas removendo espaços e em caixa baixa."""
    df.columns = df.columns.str.strip().str.lower()
    return df


def safe_int(val, default=0):
    try:
        if pd.isna(val) or str(val).strip() == "":
            return default
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return default

# ==============================================
# 3. MAIN GOLD PIPELINE EXECUTION
# ==============================================

def execute_bmi_dim_gold_pipeline():
    overall_start_time = time.time()

    if not BRONZE_BMI_DIR.exists():
        print(f"⚠️ Bronze directory '{BRONZE_BMI_DIR}' not found.")
        return

    # Busca apenas arquivos da camada Bronze que sejam dimensões (contêm 'dim' no nome)
    bronze_files = [
        f for f in list(BRONZE_BMI_DIR.glob("*.xlsx")) + list(BRONZE_BMI_DIR.glob("*.xls")) + list(BRONZE_BMI_DIR.glob("*.parquet"))
        if not f.name.startswith("~$") and "dim" in f.name.lower()
    ]

    if not bronze_files:
        print(f"ℹ️ No 'dim' dimension files found in Bronze layer: {BRONZE_BMI_DIR}")
        return

    print("\n" + "=" * 70)
    print(f"🚀 Found {len(bronze_files)} dimension file(s) in Bronze for Gold promotion.")
    print("=" * 70 + "\n")

    # Group files by dimension entity (e.g., 'bmi_dim_companies')
    dimension_groups = {}
    for f in bronze_files:
        dataset_name = identify_dataset_name(f.name)
        if dataset_name not in dimension_groups:
            dimension_groups[dataset_name] = []
        dimension_groups[dataset_name].append(f)

    # Process each dimension group independently
    for dataset_name, file_list in dimension_groups.items():
        print(f"📦 Processing Dimension: [{dataset_name}] ({len(file_list)} file(s))")

        dfs = []
        for file_path in file_list:
            try:
                if file_path.suffix.lower() == ".parquet":
                    df_temp = pd.read_parquet(file_path)
                else:
                    df_temp = pd.read_excel(file_path, dtype=str)

                df_temp = normalize_column_names(df_temp)
                dfs.append(df_temp)
                print(f"   ├─ Loaded Bronze file: {file_path.name}")
            except Exception as e:
                print(f"   ├─ ❌ Error loading {file_path.name}: {e}")

        if not dfs:
            continue

        # Combine all files of this dimension
        df_combined = pd.concat(dfs, ignore_index=True)

        # Ensure temporal reference integer types for sorting
        df_combined["reference_year"] = df_combined["reference_year"].apply(lambda x: safe_int(x, 2026))
        df_combined["reference_month"] = df_combined["reference_month"].apply(lambda x: safe_int(x, 0))

        # Preserve source lineage tracking before deduplication
        if "source_file" in df_combined.columns:
            df_combined["latest_source_file"] = df_combined["source_file"]
        if "ingestion_id" in df_combined.columns:
            df_combined["latest_ingestion_id"] = df_combined["ingestion_id"]

        # Sort values ascending to place the most recent record at the end
        sort_cols = ["reference_year", "reference_month"]
        if "ingestion_at" in df_combined.columns:
            sort_cols.append("ingestion_at")

        df_combined = df_combined.sort_values(by=sort_cols, ascending=True)

        # Identify business entity columns for deduplication
        # Ignore audit metadata columns during duplicate detection
        audit_cols = {
            "ingestion_id", "ingestion_at", "source_file", 
            "reference_month", "reference_year", "latest_source_file", "latest_ingestion_id"
        }
        business_cols = [col for col in df_combined.columns if col not in audit_cols]

        # Deduplicate keeping the LAST (most recent file/month version)
        initial_count = len(df_combined)
        df_gold = df_combined.drop_duplicates(subset=business_cols, keep="last").copy()
        dedup_count = initial_count - len(df_gold)

        # Reorganize audit tracking columns at the end of DataFrame
        df_gold["promoted_to_gold_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Cleanup internal lineage columns
        drop_temp_cols = [c for c in ["source_file", "ingestion_id"] if c in df_gold.columns]
        if drop_temp_cols:
            df_gold = df_gold.drop(columns=drop_temp_cols)

        # Export Gold Parquet file
        gold_parquet_filename = f"{dataset_name}.parquet"
        gold_output_path = GOLD_BMI_DIR / gold_parquet_filename
        
        df_gold.to_parquet(gold_output_path, index=False)

        print(f"   ├─ Deduplicated rows removed : {dedup_count:,}")
        print(f"   ├─ Final Gold records saved  : {len(df_gold):,}")
        print(f"   └─ ✅ Promoted to Gold Path  : {gold_output_path.name}\n")

        del df_combined, df_gold
        gc.collect()

    total_time = time.time() - overall_start_time
    print("=" * 70)
    print(f"🏁 BMI Dimension Gold Pipeline executed successfully in {total_time:.2f}s.")
    print("=" * 70)


if __name__ == "__main__":
    execute_bmi_dim_gold_pipeline()