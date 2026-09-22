"""
audited_reincorporation.py

Lê e valida arquivos de reincorporação da auditoria e aciona incrementalmente
as camadas Silver e Gold atualizando os arquivos Parquet de cache.
"""

from datetime import datetime
import gc
import os
from pathlib import Path
import re
import shutil
import pandas as pd

# Importa as funções executoras das camadas Silver e Gold
from op_silver_layer import processar_op_silver
from op_sgti_gold_layer import gerar_camada_op_sgti_gold

# ==============================================
# CONFIGURAÇÃO DE CAMINHOS
# ==============================================
SCRIPT_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = SCRIPT_DIR
while PROJECT_ROOT.parent != PROJECT_ROOT:
    if (PROJECT_ROOT / "data").exists():
        break
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"

NEW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "new-receipts"
BASE_RAW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "raw" / "reincorporation"
REINCORPORATION_BRONZE_FOLDER = DATA_DIR / "bronze" / "operational" / "2026" / "reincorporation"
AUDIT_FOLDER = DATA_DIR / "bronze" / "operational" / "2026"
DISCARDED_AUDIT_FILE = AUDIT_FOLDER / "audit_discarded_data.xlsx"

REQUIRED_BUSINESS_COLUMNS = ["service_id", "date", "schedule_time", "path", "type", "vehicle"]

REGEX_VEHICLE_CLEAN = r"^[A-Z]{3}\d{4}$|^[A-Z]{3}\d[A-Z]\d{2}$"
REGEX_SERVICE_ID_CLEAN = r"^\d{1,10}[A-Za-z]?$"

VALID_PATHS = {"OUTBOUND", "RETURN", "IDA", "VOLTA"}
VALID_TYPES = {"SPECIFIED", "REINFORCEMENT", "ESPECIFICADA", "ESPECIFICADO", "REFORÇO", "REFORCO"}


def generate_reincorporation_id(sequence_number: int) -> str:
    now = datetime.now()
    return f"REINC{now.strftime('%y%m%d')}{sequence_number}"


def is_empty_value(val):
    if pd.isna(val): return True
    s_val = str(val).strip().lower()
    return s_val in ["", "nan", "none", "<na>", "null"]


def validate_reincorporation_row(row):
    for col in REQUIRED_BUSINESS_COLUMNS:
        if is_empty_value(row.get(col)):
            return False, f"EMPTY_FIELD_{col.upper()}"

    raw_service_id = str(row.get("service_id", "")).strip()
    clean_service_id = re.sub(r"[\s\-]", "", raw_service_id)
    if not re.match(REGEX_SERVICE_ID_CLEAN, clean_service_id):
        return False, f"INVALID_SERVICE_ID_FORMAT ('{raw_service_id}')"

    raw_vehicle = str(row.get("vehicle", "")).strip().upper()
    clean_vehicle = re.sub(r"[\s\-]", "", raw_vehicle)
    if not re.match(REGEX_VEHICLE_CLEAN, clean_vehicle):
        return False, f"INVALID_VEHICLE_PLATE ('{raw_vehicle}')"

    path_val = str(row.get("path", "")).strip().upper()
    if path_val not in VALID_PATHS:
        return False, f"INVALID_PATH_VALUE ('{path_val}')"

    type_val = str(row.get("type", "")).strip().upper()
    if type_val not in VALID_TYPES:
        return False, f"INVALID_TYPE_VALUE ('{type_val}')"

    raw_date = row.get("date")
    try:
        if isinstance(raw_date, (pd.Timestamp, datetime)):
            parsed_date = raw_date
        else:
            s_date = str(raw_date).strip().split()[0]
            if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}", s_date):
                parsed_date = pd.to_datetime(s_date, format="%Y-%m-%d")
            else:
                parsed_date = pd.to_datetime(s_date, format="%d/%m/%Y")
    except Exception:
        return False, f"INVALID_DATE_FORMAT ('{raw_date}')"

    return True, "PASSED"


def append_to_audit_file(df_new_discards: pd.DataFrame):
    AUDIT_FOLDER.mkdir(parents=True, exist_ok=True)
    if DISCARDED_AUDIT_FILE.exists():
        df_existing = pd.read_excel(DISCARDED_AUDIT_FILE, dtype=str)
        df_final = pd.concat([df_existing, df_new_discards], ignore_index=True)
    else:
        df_final = df_new_discards
    df_final.to_excel(DISCARDED_AUDIT_FILE, index=False, engine="openpyxl")


def process_reincorporation_file(file_path, reinc_id):
    filename = file_path.name
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        df = pd.read_excel(file_path, dtype=str)
    except Exception as e:
        discarded_rows = [{
            "ingestion_id": reinc_id,
            "is_reincorporation": True,
            "source_file": filename,
            "source_row": 1,
            "rejection_reason": f"FILE_READ_ERROR ({str(e)})",
            "discarded_at": ingestion_timestamp
        }]
        return pd.DataFrame(), pd.DataFrame(discarded_rows)

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df.columns = [str(col).strip() for col in df.columns]

    valid_rows, discarded_rows = [], []
    for idx, row in df.iterrows():
        is_valid, reason = validate_reincorporation_row(row)
        row_dict = row.to_dict()
        row_dict["ingestion_id"] = reinc_id
        row_dict["is_reincorporation"] = True
        row_dict["source_file"] = filename
        row_dict["source_row"] = idx + 2

        if is_valid:
            row_dict["reincorporated_at"] = ingestion_timestamp
            valid_rows.append(row_dict)
        else:
            row_dict["rejection_reason"] = f"REINCORPORATION_FAILED: {reason}"
            row_dict["discarded_at"] = ingestion_timestamp
            discarded_rows.append(row_dict)

    df_valid = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    df_discarded = pd.DataFrame(discarded_rows) if discarded_rows else pd.DataFrame()

    return df_valid, df_discarded


def execute_reincorporation_pipeline():
    print("🔄 INICIANDO REINCORPORAÇÃO E ATUALIZAÇÃO DE CACHE (SILVER -> GOLD)")
    
    reinc_files = [
        f for f in list(NEW_FOLDER.glob("reincorporacao*.xlsx")) + list(NEW_FOLDER.glob("reinc*.xlsx"))
        if not f.name.startswith("~$")
    ]

    if not reinc_files:
        print("ℹ️ Nenhum arquivo de reincorporação encontrado.")
        return

    all_valid_reincorporations = []

    for seq, file_path in enumerate(reinc_files, start=1):
        reinc_id = generate_reincorporation_id(seq)
        filename = file_path.name
        print(f"\n📄 Processando Reincorporação: {filename} | ID: {reinc_id}")

        df_valid, df_discarded = process_reincorporation_file(file_path, reinc_id)

        if not df_discarded.empty:
            append_to_audit_file(df_discarded)
            print(f"   ├─ 🚨 {len(df_discarded)} linha(s) não passaram e foram re-enviadas para a auditoria.")

        if not df_valid.empty:
            BASE_RAW_FOLDER.mkdir(parents=True, exist_ok=True)
            raw_dest = BASE_RAW_FOLDER / f"{file_path.stem}_{reinc_id}{file_path.suffix}"
            shutil.copy2(file_path, raw_dest)

            try:
                file_path.unlink()
            except PermissionError:
                gc.collect()
                file_path.unlink()

            REINCORPORATION_BRONZE_FOLDER.mkdir(parents=True, exist_ok=True)
            bronze_out = REINCORPORATION_BRONZE_FOLDER / f"bronze_reinc_{file_path.stem}_{reinc_id}.xlsx"
            df_valid.to_excel(bronze_out, index=False, engine="openpyxl")
            
            all_valid_reincorporations.append(df_valid)
            print(f"   └─ ✅ Registros de reincorporação gravados na Bronze: {bronze_out.name}")

    if all_valid_reincorporations:
        df_incremental = pd.concat(all_valid_reincorporations, ignore_index=True)
        
        # 1. Executa atualização em Cache da Silver
        processar_op_silver(df_incremental=df_incremental)
        
        # 2. Executa a consolidação final da Gold
        gerar_camada_op_sgti_gold()


if __name__ == "__main__":
    execute_reincorporation_pipeline()