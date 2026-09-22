import calendar
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import time
from typing import Dict, Any
import pandas as pd
import re

DATASET_TYPE_ALIAS = {
    "horarios": "schedule",
    "schedule": "schedule",
    "linhascomdatacontrato": "services",
    "service": "service",
    "consultaveiculos": "vehicles",
    "vehicles": "vehicles",
}

# ==============================================
# 1. DIRECTORY CONFIGURATION
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
# 2. COLUMN MAPPING CONFIGURATION (PT ➔ EN)
# ==============================================
COLUMN_MAPPING_CONFIG = {
    "schedule": {
        "Número Linha": "service_id",
        "Trajeto": "path",
        "Horário": "schedule_time",
        "Número Veículos": "number_of_vehicles",
        "Feriado": "holiday",
        "Segunda": "monday",
        "Terça": "tuesday",
        "Quarta": "wednesday",
        "Quinta": "thursday",
        "Sexta": "friday",
        "Sábado": "saturday",
        "Domingo": "sunday",
        "Janeiro": "january",
        "Fevereiro": "february",
        "Março": "march",
        "Abril": "april",
        "Maio": "may",
        "Junho": "june",
        "Julho": "july",
        "Agosto": "august",
        "Setembro": "september",
        "Outubro": "october",
        "Novembro": "november",
        "Dezembro": "december",
    },
    "service": {
        "Linha": "service_id",
        "Nome": "service_name",
        "Código delegatária": "delegated_company_cod",
        "Delegatária": "delegated_company_name",
        "Situação delegatária": "delegated_company_status",
        "Situação da linha": "service_status",
        "Data de vencimento": "contract_expiration",
        "Número do contrato": "contract_number",
        "Tipo utilização": "utilization",
        "Veículos necessários": "necessary_vehicles",
        "Custo da passagem": "ticket_cost",
        "Viag. programadas": "trips_per_week",
        "Núm. meses de operação": "months_of_operation",
    },
    "vehicles": {
        "Número": "vehicle_number",
        "Placa": "plate",
        "Data de Registro": "registration_date",
        "Utilização": "utilization",
        "Código": "delegated_company_code",
        "Delegatária": "delegated_company_name",
        "Marca chassi": "chassis_manufacturer",
        "Modelo chassi": "chassis_model",
        "Ano chassi": "chassis_year",
        "Situação": "situation",
        "Data baixa": "deactivation_date",
        "Marca carroçaria": "bodywork_manufacturer",
        "Modelo carroçaria": "bodywork_model",
        "Cores": "colors",
        "Equipamentos": "equipment",
        "Combustível": "fuel",
    },
}

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
    }
}

# ==============================================
# 3. CONSTANTS & MAPS FOR SCHEDULE EXPANSION
# ==============================================
EXPANSION_YEAR = 2026

HOLIDAYS = {
    f"{EXPANSION_YEAR}-01-01",
    f"{EXPANSION_YEAR}-02-16",
    f"{EXPANSION_YEAR}-02-17",
    f"{EXPANSION_YEAR}-04-03",
    f"{EXPANSION_YEAR}-04-21",
    f"{EXPANSION_YEAR}-05-01",
    f"{EXPANSION_YEAR}-06-04",
    f"{EXPANSION_YEAR}-09-07",
    f"{EXPANSION_YEAR}-10-12",
    f"{EXPANSION_YEAR}-11-02",
    f"{EXPANSION_YEAR}-11-15",
    f"{EXPANSION_YEAR}-11-20",
    f"{EXPANSION_YEAR}-12-25",
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


def is_truthy(val) -> bool:
    if pd.isna(val):
        return False
    s = str(val).strip().upper()
    return s in {"S", "SIM", "Y", "YES", "1", "TRUE"}


def safe_read_sgti_file(file_path: Path) -> pd.DataFrame:
    """Lê arquivos SGTI tratando diversos formatos e relatórios HTML/XML mascarados como XLS."""
    ext = file_path.suffix.lower()
    
    if ext == ".parquet":
        return pd.read_parquet(file_path)

    if ext == ".csv":
        try:
            return pd.read_csv(file_path, dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(file_path, dtype=str, encoding="latin1", sep=";")

    try:
        return pd.read_excel(file_path, dtype=str)
    except Exception:
        pass

    try:
        return pd.read_excel(file_path, dtype=str, engine="xlrd")
    except Exception:
        pass

    try:
        dfs = pd.read_html(str(file_path))
        if dfs:
            df = dfs[0]
            df.columns = [str(col).strip() for col in df.columns]
            return df.astype(str)
    except Exception:
        pass

    raise ValueError(f"Não foi possível determinar a engine de leitura adequada para: {file_path.name}")


def parse_file_metadata(file_path: Path):
    stem = file_path.stem
    match = re.search(r"^([A-Za-z]+)[\s_]+(\d{4})\.(\d{2})\.(\d{2})", stem)
    if match:
        dataset_type = match.group(1)
        month = match.group(3)
        return dataset_type, month

    parts = stem.split()
    dataset_type = parts[0]
    month = parts[1].split(".")[1] if len(parts) > 1 and "." in parts[1] else "01"
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

            if not is_truthy(row.get(month_key)):
                continue

            month_name, month_num = MONTHS_MAP[month_key]
            last_day = calendar.monthrange(EXPANSION_YEAR, month_num)[1]

            for day in range(1, last_day + 1):
                date_obj = datetime(EXPANSION_YEAR, month_num, day)
                date_str = date_obj.strftime("%Y-%m-%d")
                weekday_key, weekday_name = WEEKDAYS_MAP[date_obj.weekday()]

                is_holiday = date_str in HOLIDAYS

                runs_on_day = is_truthy(row.get(weekday_key))
                runs_on_holiday = is_truthy(row.get(holiday_col))

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


def consolidate_sgti_datasets(force_rebuild: bool = False) -> Dict[str, str]:
    """
    Processa e consolida os arquivos do SGTI.
    Retorna um dicionário {nome_arquivo: motivo_erro} com todos os problemas encontrados.
    """
    erros_sgti: Dict[str, str] = {}

    if not INPUT_DIR.exists():
        msg = f"Diretório de entrada não encontrado: {INPUT_DIR}"
        print(f"❌ {msg}")
        erros_sgti["diretorio_sgti"] = msg
        return erros_sgti

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
        return erros_sgti

    dataset_groups = defaultdict(list)
    for file in files:
        dataset_type, month = parse_file_metadata(file)
        dataset_groups[dataset_type].append({"path": file, "month": month})

    print(
        f"🚀 Found {len(files)} file(s) across {len(dataset_groups)} SGTI dataset category(ies).\n"
    )

    cache_metadata = load_cache_metadata()

    for dataset_type, file_list in dataset_groups.items():
        target_dataset_type = DATASET_TYPE_ALIAS.get(dataset_type.lower(), dataset_type.lower())
        is_schedule = target_dataset_type == "schedule"

        consolidated_filename = f"consolidated_{target_dataset_type}.parquet"
        consolidated_path = SILVER_DIR / consolidated_filename

        expanded_filename = f"expanded_{target_dataset_type}.parquet"
        expanded_path = SILVER_DIR / expanded_filename

        current_group_hash = calculate_group_hash(file_list)
        cached_group_hash = cache_metadata.get(dataset_type)

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

        # Agrupa arquivos do mesmo dataset por mês e escolhe a versão mais recente/retificada
        files_by_month = defaultdict(list)
        for item in file_list:
            files_by_month[item["month"]].append(item)

        selected_file_list = []
        for m, items in files_by_month.items():
            if len(items) > 1:
                # Extrai número da retificação (RET1 -> 1, RET2 -> 2, sem RET -> 0)
                def get_ret_num(item_obj):
                    name = item_obj["path"].stem.upper()
                    match = re.search(r"_RET(\d+)", name)
                    return int(match.group(1)) if match else 0

                # Ordena por número de RET e depois por data de modificação
                items_sorted = sorted(
                    items, 
                    key=lambda x: (get_ret_num(x), x["path"].stat().st_mtime), 
                    reverse=True
                )
                escolhido = items_sorted[0]
                print(f"   ℹ️ Retificação detectada no SGTI mês {m}: usando '{escolhido['path'].name}' e ignorando versão(ões) anterior(es).")
                selected_file_list.append(escolhido)
            else:
                selected_file_list.append(items[0])

        file_list = selected_file_list
        
        df_list = []
        files_failed_in_group = []
        print(f"📦 Stacking dataset category '{dataset_type}':")

        for item in file_list:
            file_path = item["path"]
            try:
                df = safe_read_sgti_file(file_path)
                df.columns = df.columns.str.strip()

                mapping = COLUMN_MAPPING_CONFIG.get(target_dataset_type, {})
                df = df.rename(columns=mapping)

                df["file_month"] = item["month"]
                df_list.append(df)
                print(
                    f"   ├─ Loaded: {file_path.name} ({len(df)} rows | Month: {item['month']})"
                )
            except Exception as e:
                msg_erro = f"Falha ao ler/converter arquivo SGTI: {e}"
                print(f"   ├─ ❌ {file_path.name}: {msg_erro}")
                erros_sgti[file_path.name] = msg_erro
                files_failed_in_group.append(file_path)

        if df_list:
            try:
                consolidated_df = pd.concat(df_list, ignore_index=True)

                if "service_id" in consolidated_df.columns:
                    consolidated_df["service_id"] = consolidated_df["service_id"].astype(str)

                # Tratamento de Frota Histórica (Veículos SGTI)
                if target_dataset_type == "vehicles":
                    print("   ⚡ Processando histórico de frota e sinalizando registros mais recentes...")
                    consolidated_df["file_month"] = pd.to_numeric(consolidated_df["file_month"], errors="coerce")
                    col_placa = "plate" if "plate" in consolidated_df.columns else "license_plate"

                    if col_placa in consolidated_df.columns:
                        max_month_per_plate = consolidated_df.groupby(col_placa)["file_month"].transform("max")
                        consolidated_df["is_latest_record"] = consolidated_df["file_month"] == max_month_per_plate
                    else:
                        consolidated_df["is_latest_record"] = True

                # Exportação para Parquet Silver
                temp_consolidated_path = consolidated_path.with_suffix(".parquet.tmp")
                consolidated_df.to_parquet(temp_consolidated_path, index=False)
                temp_consolidated_path.replace(consolidated_path)
        
                print(
                    f"   ├─ 🟢 [SILVER] Consolidated output saved ({len(consolidated_df):,} rows): {consolidated_path.name}"
                )

                total_expanded_rows = None

                if is_schedule:
                    expanded_df = expand_schedule_dataframe(consolidated_df)
                    temp_expanded_path = expanded_path.with_suffix(".parquet.tmp")
                    expanded_df.to_parquet(temp_expanded_path, index=False)
                    temp_expanded_path.replace(expanded_path)
            
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

                # Mover arquivos processados para a pasta Bronze
                for item in file_list:
                    src_path = item["path"]
                    # Pula arquivos que deram erro na leitura individual
                    if src_path in files_failed_in_group:
                        continue

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
                                msg_mva = f"Arquivo processado, mas não pôde ser movido para Bronze (Permissão negada)"
                                erros_sgti[src_path.name] = msg_mva

            except Exception as e:
                msg_cons = f"Erro no agrupamento/salvamento do dataset '{dataset_type}': {e}"
                print(f"   └─ ❌ {msg_cons}")
                for item in file_list:
                    erros_sgti[item["path"].name] = msg_cons

            print()

    return erros_sgti


if __name__ == "__main__":
    consolidate_sgti_datasets()