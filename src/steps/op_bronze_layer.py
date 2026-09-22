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

# Navega até encontrar a raiz do projeto (onde 'data' e 'src' coexistem)
PROJECT_ROOT = SCRIPT_DIR
while PROJECT_ROOT.parent != PROJECT_ROOT:
    if (PROJECT_ROOT / "data").exists() and (PROJECT_ROOT / "src").exists():
        break
    PROJECT_ROOT = PROJECT_ROOT.parent

# Fallback de segurança se não encontrar
if not (PROJECT_ROOT / "data").exists():
    PROJECT_ROOT = SCRIPT_DIR.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
NEW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "new-receipts"
BASE_RAW_FOLDER = DATA_DIR / "ingestion" / "operational" / "2026" / "raw"
BASE_BRONZE_FOLDER = DATA_DIR / "bronze" / "operational"
AUDIT_FOLDER = BASE_BRONZE_FOLDER / "2026"
DISCARDED_AUDIT_FILE = AUDIT_FOLDER / "audit_discarded_data.xlsx"

# ==============================================
# CADASTRO DE DELEGATÁRIAS PERMITIDAS
# ==============================================
# Planilhas cujo nome de empresa extraído não constar nesta lista serão IGNORADAS e mantidas em 'new-receipts' para ajuste manual.

DELEGATARIAS_PERMITIDAS = {
    "barraca",
    "coutinho",
    "emp_sao_cristovao",
    "exp_uniao",
    "gontijo",
    "itauna",
    "mansur",
    "novo_horizonte",
    "paraibuna",
    "passaro_verde",
    "piracicaba",
    "platina",
    "presidente",
    "riodoce",
    "santa_izabel",
    "santa_maria",
    "sao_luiz",
    "saritur",
    "sc_minas",
    "sertaneja",
    "setelagoano",
    "sudoestino",
    "teixeira",
    "transnorte",
    "transur",
    "univale",
    "util_uniao",
    "viac_sao_cristovao"
}

# Mandatory operational schema columns
REQUIRED_COLUMNS = [
    "service_id",    # Coluna A (Índice 0)
    "date",          # Coluna B (Índice 1)
    "schedule_time", # Coluna C (Índice 2)
    "path",          # Coluna D (Índice 3)
    "type",          # Coluna E (Índice 4)
    "vehicle"        # Coluna F (Índice 5)
]

# Validation patterns and domains
REGEX_VEHICLE_CLEAN = r"^[A-Z]{3}\d{4}$|^[A-Z]{3}\d[A-Z]\d{2}$"
REGEX_SERVICE_ID_CLEAN = r"^\d{1,10}[A-Za-z]?$"

#Domínios aceitos para não descartar dados válidos na Bronze

VALID_PATHS = {
    "OUTBOUND", "RETURN", 
    "IDA", "VOLTA", 
    "SAIDA", "SAÍDA", "CHEGADA",
    "I", "V",
    "S","C"     
}
VALID_TYPES = {
    "SPECIFIED", "REINFORCEMENT", 
    "ESPECIFICADA", "ESPECIFICADO", 
    "ESPEFICICADA", "E"
    "REFORÇO", "REFORCO", "R"
}

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
    raw_service_id = str(row.get("service_id", "")).strip()
    clean_service_id = re.sub(r"[\s\-]", "", raw_service_id)
    if not re.match(REGEX_SERVICE_ID_CLEAN, clean_service_id):
        return False, f"INVALID_SERVICE_ID_FORMAT ('{raw_service_id}')"

    # 3. Vehicle plate validation
    raw_vehicle = str(row.get("vehicle", "")).strip().upper()
    clean_vehicle = re.sub(r"[\s\-]", "", raw_vehicle)
    if not re.match(REGEX_VEHICLE_CLEAN, clean_vehicle):
        return False, f"INVALID_VEHICLE_PLATE ('{raw_vehicle}')"

    # 4. Path validation
    path_val = str(row.get("path", "")).strip().upper()
    if path_val not in VALID_PATHS:
        return False, f"INVALID_PATH_VALUE ('{path_val}')"

    # 5. Service type validation
    type_val = str(row.get("type", "")).strip().upper()
    if type_val not in VALID_TYPES:
        return False, f"INVALID_TYPE_VALUE ('{type_val}')"

    # 6. Date formatting and reference period consistency
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

        if parsed_date.year != ref_year or parsed_date.month != ref_month:
            return False, f"DATE_OUT_OF_PERIOD ({parsed_date.strftime('%d/%m/%Y')} != {ref_year}/{ref_month:02d})"
    except Exception:
        return False, f"INVALID_DATE_FORMAT ('{raw_date}')"

    return True, "PASSED"


def process_bronze_ingestion(file_path, ingestion_id, is_rectification=False):
    filename = file_path.name
    company_name, ref_year, ref_month = extract_metadata_from_filename(filename)
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. LEITURA SEGURA DO ARQUIVO (.xls, .xlsx ou corrompido)
    try:
        df = pd.read_excel(file_path, dtype=str)
    except Exception as e:
        discarded_rows = [{
            "ingestion_id": ingestion_id,
            "is_rectification": is_rectification,
            "source_file": filename,
            "source_row": 1,
            "rejection_reason": f"FILE_READ_ERROR ({str(e)})",
            "discarded_at": ingestion_timestamp
        }]
        return pd.DataFrame(), pd.DataFrame(discarded_rows), company_name, ref_year, ref_month

    if df.empty:
        del df
        gc.collect()
        return pd.DataFrame(), pd.DataFrame(), company_name, ref_year, ref_month

    # 2. VALIDAÇÃO ESTRUTURAL (Menos de 6 colunas)
    if len(df.columns) < len(REQUIRED_COLUMNS):
        discarded_rows = []
        reason = f"INVALID_FILE_STRUCTURE (Possui {len(df.columns)} colunas, esperado no mínimo {len(REQUIRED_COLUMNS)})"
        
        for idx, row in df.iterrows():
            row_dict = {
                "ingestion_id": ingestion_id,
                "is_rectification": is_rectification,
                "source_file": filename,
                "source_row": idx + 2,
                "rejection_reason": reason,
                "discarded_at": ingestion_timestamp,
                **row.to_dict()
            }
            discarded_rows.append(row_dict)

        df_discarded = pd.DataFrame(discarded_rows)
        del df
        gc.collect()
        return pd.DataFrame(), df_discarded, company_name, ref_year, ref_month

    # 3. MAPEAMENTO DE COLUNAS E VALIDAÇÃO DAS LINHAS
    col_names = list(df.columns)
    col_names[:len(REQUIRED_COLUMNS)] = REQUIRED_COLUMNS
    df.columns = col_names

    valid_rows = []
    discarded_rows = []

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


def append_to_audit_file(df_new_discards: pd.DataFrame):
    """Escreve ou anexa novos descartes imediatamente no arquivo Excel de auditoria."""
    AUDIT_FOLDER.mkdir(parents=True, exist_ok=True)

    if DISCARDED_AUDIT_FILE.exists():
        df_existing = pd.read_excel(DISCARDED_AUDIT_FILE, dtype=str)
        df_final = pd.concat([df_existing, df_new_discards], ignore_index=True)
    else:
        df_final = df_new_discards

    df_final.to_excel(DISCARDED_AUDIT_FILE, index=False, engine="openpyxl")


# ==============================================
# 3. MAIN PIPELINE EXECUTION
# ==============================================

def execute_operational_bronze_pipeline() -> dict:
    print(f"🔍 Diretório de Entrada: {NEW_FOLDER.resolve()}")
    print(f"🎯 Ficheiro de Auditoria (DLQ): {DISCARDED_AUDIT_FILE.resolve()}\n")

    excel_files = [
        f for f in list(NEW_FOLDER.glob("*.xlsx")) + list(NEW_FOLDER.glob("*.xls"))
        if not f.name.startswith("~$")
    ]

    # Dicionário para guardar {nome_do_arquivo: motivo_erro}
    arquivos_nao_processados = {}

    if not excel_files:
        print(f"ℹ️ Nenhum novo ficheiro encontrado em: {NEW_FOLDER}")
        return arquivos_nao_processados

    print(f"🚀 Encontrado(s) {len(excel_files)} ficheiro(s) para ingestão Bronze.\n")

    for sequence, file_path in enumerate(excel_files, start=1):
        filename = file_path.name
        ingestion_id = generate_ingestion_id(sequence)
        
        try:
            company, year, month = extract_metadata_from_filename(filename)
        except Exception as e:
            msg_erro = f"Erro ao extrair metadados do nome do arquivo ({e})"
            print(f"   └─ ❌ {msg_erro}\n")
            arquivos_nao_processados[filename] = msg_erro
            continue

        # Validação de Delegatária Permitida
        if DELEGATARIAS_PERMITIDAS and company not in DELEGATARIAS_PERMITIDAS:
            msg_erro = f"Delegatária '{company}' não cadastrada em DELEGATARIAS_PERMITIDAS"
            print(f"📄 Processando: {filename}")
            print(f"   └─ ⚠️ DELEGATÁRIA NÃO RECONHECIDA: {msg_erro}\n")
            arquivos_nao_processados[filename] = msg_erro
            continue

        company_raw_folder = BASE_RAW_FOLDER / company
        pattern_month = f"_{year}_{month:02d}"
        
        existing_month_files = []
        if company_raw_folder.exists():
            existing_month_files = [
                f for f in company_raw_folder.glob("*.xlsx")
                if f.is_file() and pattern_month in f.name
            ]

        is_rectification = len(existing_month_files) > 0

        print(f"📄 Processando: {filename} | ID: {ingestion_id} | Retificação: {is_rectification}")

        df_valid, df_discarded, company, year, month = process_bronze_ingestion(
            file_path, ingestion_id, is_rectification=is_rectification
        )

        if (df_valid is None or df_valid.empty) and (df_discarded is None or df_discarded.empty):
            msg_erro = "Arquivo completamente vazio"
            print(f"   └─ ⚠️ {msg_erro}.")
            arquivos_nao_processados[filename] = msg_erro
            continue

        print(f"   ├─ Empresa: {company} | Período: {year}-{month:02d}")

        if not df_discarded.empty:
            append_to_audit_file(df_discarded)
            print(f"   ├─ 🚨 {len(df_discarded)} linha(s) descartada(s) registada(s) na auditoria.")

        # Validação de Tolerância de Descarre/Erro
        total_rows = len(df_valid) + len(df_discarded)
        discard_rate = (len(df_discarded) / total_rows) if total_rows > 0 else 0.0

        if discard_rate >= 0.75:
            msg_erro = f"Taxa de erro de {discard_rate:.1%} (>= 75% limite)"
            print(f"   └─ 🛑 ALERTA CRÍTICO: {msg_erro}. Ingestão abortada.")
            arquivos_nao_processados[filename] = msg_erro

        elif not df_valid.empty:
            # Movimentação normal para RAW e Bronze
            company_raw_folder.mkdir(parents=True, exist_ok=True)
            target_raw_folder = company_raw_folder / "retificadas" if is_rectification else company_raw_folder
            target_raw_folder.mkdir(parents=True, exist_ok=True)

            raw_filename = f"{file_path.stem}_{ingestion_id}{file_path.suffix}"
            raw_destination_path = target_raw_folder / raw_filename
            shutil.copy2(file_path, raw_destination_path)
            
            try:
                file_path.unlink()
            except PermissionError:
                gc.collect()
                file_path.unlink()

            target_bronze_folder = BASE_BRONZE_FOLDER / str(year) / company
            target_bronze_folder.mkdir(parents=True, exist_ok=True)
            
            bronze_filename = f"bronze_{file_path.stem}_{ingestion_id}{file_path.suffix}"
            bronze_output_path = target_bronze_folder / bronze_filename
            df_valid.to_excel(bronze_output_path, index=False, engine="openpyxl")
            print(f"   └─ Camada BRONZE guardada ({len(df_valid)} linhas válidas)")

        else:
            msg_erro = "Estrutura inválida ou nenhuma linha válida identificada"
            print(f"   └─ ⚠️ {msg_erro}.")
            arquivos_nao_processados[filename] = msg_erro

        print()

    print("🏁 Pipeline da Camada Bronze concluída.")
    return arquivos_nao_processados

if __name__ == "__main__":
    execute_operational_bronze_pipeline()