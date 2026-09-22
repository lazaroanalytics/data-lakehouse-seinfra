from datetime import datetime
import gc
from pathlib import Path
import re
import shutil
import time
import pandas as pd

# ==============================================
# 1. DYNAMIC PATH CONFIGURATION
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"

NEW_FOLDER = DATA_DIR / "ingestion" / "bmi" / "2026" / "new-receipts"
BASE_RAW_FOLDER = DATA_DIR / "ingestion" / "bmi" / "2026" / "raw"
BASE_BRONZE_FOLDER = DATA_DIR / "bronze" / "bmi" / "2026"

BASE_RAW_FOLDER.mkdir(parents=True, exist_ok=True)
BASE_BRONZE_FOLDER.mkdir(parents=True, exist_ok=True)


# ==============================================
# 2. HELPER FUNCTIONS
# ==============================================
def generate_ingestion_id(sequence_number: int) -> str:
    """Gera ID único de ingestão: BMI + YY + MM + DD + Sequencial."""
    now = datetime.now()
    return f"BMI{now.strftime('%y%m%d')}{sequence_number:03d}"


def extract_metadata_from_foldername(folder_path: Path):
    """Extrai dia, mês e ano da pasta (ex: 18_06_2026 ou 18-06-2026)."""
    match = re.search(r"(\d{2})[-_](\d{2})[-_](\d{4})", folder_path.name)
    if match:
        return int(match.group(3)), int(match.group(2)), int(match.group(1))

    now = datetime.now()
    return now.year, now.month, now.day


# ==============================================
# 3. MAIN PIPELINE EXECUTION
# ==============================================
def execute_bmi_bronze_pipeline():
    overall_start_time = time.time()

    if not NEW_FOLDER.exists():
        print(f"⚠️ Diretório de entrada '{NEW_FOLDER}' não encontrado. Criando caminho...")
        NEW_FOLDER.mkdir(parents=True, exist_ok=True)
        return

    # Varre apenas subpastas de datas (ex: 18_06_2026)
    date_folders = [d for d in NEW_FOLDER.iterdir() if d.is_dir()]

    if not date_folders:
        print(f"ℹ️ Nenhuma pasta de lote encontrada em: {NEW_FOLDER}")
        return

    print("\n" + "=" * 70)
    print(f"🚀 Encontrada(s) {len(date_folders)} pasta(s) de lote para ingestão BMI Bronze.")
    print("=" * 70 + "\n")

    sequence = 1
    for date_folder in date_folders:
        ref_year, ref_month, ref_day = extract_metadata_from_foldername(date_folder)

        # Captura .xlsx, .xls e .csv (incluindo dicionario_dados.csv)
        files = [
            f for f in date_folder.iterdir()
            if f.is_file() and f.suffix.lower() in [".xlsx", ".xls", ".csv"] and not f.name.startswith("~$")
        ]

        if not files:
            print(f"⚠️ Pasta sem arquivos válidos: {date_folder.name}")
            continue

        print(f"📁 Processando Lote/Pasta: {date_folder.name} (Data Ref: {ref_day:02d}/{ref_month:02d}/{ref_year})")

        for file_path in files:
            filename = file_path.name
            ext = file_path.suffix.lower()
            ingestion_id = generate_ingestion_id(sequence)
            ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(f"   📄 Processando: {filename} | ID: {ingestion_id}")

            try:
                # 1. Leitura com suporte a CSV (separa por ';' se necessário) e Excel
                if ext in [".xlsx", ".xls"]:
                    df = pd.read_excel(file_path, dtype=str)
                elif ext == ".csv":
                    df = pd.read_csv(file_path, sep=None, engine="python", dtype=str, encoding="utf-8-sig")
                else:
                    continue

                # Normalização de nomes das colunas
                df.columns = df.columns.str.strip().str.lower()

                # 2. Injeção das colunas de Governança
                df.insert(0, "ingestion_id", ingestion_id)
                df["ingestion_at"] = ingestion_timestamp
                df["reference_day"] = ref_day
                df["reference_month"] = ref_month
                df["reference_year"] = ref_year
                df["source_file"] = filename
                df["source_folder"] = date_folder.name

                # 3. Cópia exata do arquivo original para a RAW
                raw_filename = f"{file_path.stem}_{date_folder.name}_{ingestion_id}{file_path.suffix}"
                raw_destination_path = BASE_RAW_FOLDER / raw_filename
                shutil.copy2(file_path, raw_destination_path)
                print(f"      ├─ Salvo na RAW: {raw_destination_path.name}")

                # 4. Gravação da versão tratada na BRONZE em Excel
                bronze_filename = f"bronze_{file_path.stem}_{date_folder.name}_{ingestion_id}.xlsx"
                bronze_output_path = BASE_BRONZE_FOLDER / bronze_filename

                df.to_excel(bronze_output_path, index=False, engine="openpyxl")
                print(f"      ├─ Salvo na BRONZE ({len(df)} linhas, {len(df.columns)} colunas): {bronze_output_path.name}")

                # 5. Remoção do arquivo de origem da landing zone
                try:
                    file_path.unlink()
                except PermissionError:
                    del df
                    gc.collect()
                    file_path.unlink()

                sequence += 1

            except Exception as e:
                print(f"      └─ ❌ Erro ao processar {filename}: {e}\n")

        # Limpeza da pasta de data após esvaziamento
        try:
            date_folder.rmdir()
            print(f"   └─ 🧹 Pasta do lote {date_folder.name} limpa e removida.\n")
        except Exception as e:
            print(f"   └─ ⚠️ Não foi possível remover a pasta {date_folder.name}: {e}\n")

    total_time = time.time() - overall_start_time
    print("=" * 70)
    print(f"🏁 Pipeline BMI Bronze executado com sucesso em {total_time:.2f}s.")
    print("=" * 70)


if __name__ == "__main__":
    execute_bmi_bronze_pipeline()