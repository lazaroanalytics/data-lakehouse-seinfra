from datetime import datetime
import gc
from pathlib import Path
import re
import time
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION
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
    Extrai o nome base limpo da tabela e garante o prefixo 'bmi_'.
    Exemplo: 'bronze_Movto-custos-operac_18_06_2026_BMI260917001.xlsx'
             ➔ 'bmi_movto_custos_operac'
    """
    stem = Path(filename).stem.lower()

    # 1. Remove prefixo 'bronze_'
    if stem.startswith("bronze_"):
        stem = stem[7:]

    # 2. Remove o ID de Ingestão do final (ex: _bmi260917001)
    stem = re.sub(r"[_|-]bmi\d+$", "", stem, flags=re.IGNORECASE)

    # 3. Remove a data da pasta (ex: _18_06_2026 ou _18-06-2026)
    stem = re.sub(r"[_|-]\d{2}[_|-]\d{2}[_|-]\d{4}$", "", stem)

    # 4. Substitui hífens por underscores e normaliza espaços
    stem = stem.replace("-", "_").strip("_")

    # 5. Garante o prefixo 'bmi_'
    if not stem.startswith("bmi_"):
        stem = f"bmi_{stem}"

    return stem


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza as colunas em minúsculas e sem espaços."""
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
def execute_bmi_gold_pipeline():
    overall_start_time = time.time()

    if not BRONZE_BMI_DIR.exists():
        print(f"⚠️ Diretório Bronze '{BRONZE_BMI_DIR}' não encontrado.")
        return

    # Busca TODOS os arquivos válidos da Bronze (Excel ou Parquet)
    bronze_files = [
        f for f in list(BRONZE_BMI_DIR.glob("*.xlsx")) + list(BRONZE_BMI_DIR.glob("*.xls")) + list(BRONZE_BMI_DIR.glob("*.parquet"))
        if not f.name.startswith("~$")
    ]

    if not bronze_files:
        print(f"ℹ️ Nenhum arquivo encontrado na camada Bronze: {BRONZE_BMI_DIR}")
        return

    print("\n" + "=" * 70)
    print(f"🚀 Encontrado(s) {len(bronze_files)} arquivo(s) na Bronze para promoção à Gold.")
    print("=" * 70 + "\n")

    # Agrupa arquivos pelo nome do dataset (ex: 'bmi_movto_custos_operac')
    dataset_groups = {}
    for f in bronze_files:
        dataset_name = identify_dataset_name(f.name)
        if dataset_name not in dataset_groups:
            dataset_groups[dataset_name] = []
        dataset_groups[dataset_name].append(f)

    # Processa cada grupo de tabelas independentemente
    for dataset_name, file_list in dataset_groups.items():
        print(f"📦 Processando Tabela Gold: [{dataset_name}] ({len(file_list)} arquivo(s))")

        dfs = []
        for file_path in file_list:
            try:
                if file_path.suffix.lower() == ".parquet":
                    df_temp = pd.read_parquet(file_path)
                else:
                    df_temp = pd.read_excel(file_path, dtype=str)

                df_temp = normalize_column_names(df_temp)
                dfs.append(df_temp)
                print(f"   ├─ Carregado da Bronze: {file_path.name}")
            except Exception as e:
                print(f"   ├─ ❌ Erro ao ler {file_path.name}: {e}")

        if not dfs:
            continue

        # Consolida todas as cargas da mesma tabela
        df_combined = pd.concat(dfs, ignore_index=True)

        # Trata inteiros de referência temporal para ordenação
        for col in ["reference_year", "reference_month", "reference_day"]:
            if col in df_combined.columns:
                df_combined[col] = df_combined[col].apply(lambda x: safe_int(x, 0))

        # Preserva rastreabilidade de linhagem
        if "source_file" in df_combined.columns:
            df_combined["latest_source_file"] = df_combined["source_file"]
        if "ingestion_id" in df_combined.columns:
            df_combined["latest_ingestion_id"] = df_combined["ingestion_id"]

        # Ordena para garantir que a versão mais recente fique por último
        sort_cols = [c for c in ["reference_year", "reference_month", "reference_day", "ingestion_at"] if c in df_combined.columns]
        if sort_cols:
            df_combined = df_combined.sort_values(by=sort_cols, ascending=True)

        # Define colunas de negócio para deduplicação (ignora metadados de auditoria)
        audit_cols = {
            "ingestion_id", "ingestion_at", "source_file", "source_folder",
            "reference_day", "reference_month", "reference_year", 
            "latest_source_file", "latest_ingestion_id"
        }
        business_cols = [col for col in df_combined.columns if col not in audit_cols]

        # Deduplica mantendo o último registro (mais recente)
        initial_count = len(df_combined)
        df_gold = df_combined.drop_duplicates(subset=business_cols, keep="last").copy()
        dedup_count = initial_count - len(df_gold)

        # Adiciona carimbo de promoção para a Gold
        df_gold["promoted_to_gold_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Limpa colunas temporárias internas
        drop_temp_cols = [c for c in ["source_file", "ingestion_id"] if c in df_gold.columns]
        if drop_temp_cols:
            df_gold = df_gold.drop(columns=drop_temp_cols)

        # Exportação segura (substituição atômica) para Parquet
        gold_parquet_filename = f"{dataset_name}.parquet"
        gold_output_path = GOLD_BMI_DIR / gold_parquet_filename
        temp_gold_path = gold_output_path.with_suffix(".parquet.tmp")

        df_gold.to_parquet(temp_gold_path, index=False)
        temp_gold_path.replace(gold_output_path)

        print(f"   ├─ Registros duplicados removidos : {dedup_count:,}")
        print(f"   ├─ Registros finais salvos em Gold : {len(df_gold):,}")
        print(f"   └─ 🟢 Arquivo Parquet Gold gerado   : {gold_output_path.name}\n")

        del df_combined, df_gold
        gc.collect()

    total_time = time.time() - overall_start_time
    print("=" * 70)
    print(f"🏁 Pipeline BMI Gold executado com sucesso em {total_time:.2f}s.")
    print("=" * 70)


if __name__ == "__main__":
    execute_bmi_gold_pipeline()