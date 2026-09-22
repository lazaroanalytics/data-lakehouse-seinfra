"""
op_silver_layer.py

Processa os dados operacionais da camada Bronze Operacional (bronze/operational)
e realiza os cruzamentos de saneamento, enriquecimento e auditoria com as tabelas
da Silver SGTI (Veículos, Serviços e Programação Expandida), gerando os arquivos
da camada Silver Operacional (silver/operational) em formato Parquet com suporte a Cache.
"""

import os
import re
import time
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from collections import defaultdict

# ==============================================
# 1. CONFIGURAÇÃO DE CAMINHOS PADRONIZADOS
# ==============================================

BASE_DIR = os.getcwd()

# Caminho da Tabela Dimensão (De-Para de Empresas)
PATH_DIM_DELEGATARIAS = os.path.join(BASE_DIR, "data", "metadata", "dim_delegatarias.xlsx")

# Caminhos de Entrada ajustados com a subpasta '2026' da Silver SGTI
PATH_BRONZE_OPERACIONAL = os.path.join(BASE_DIR, "data", "bronze", "operational")
PATH_SILVER_SGTI_VEICULOS = os.path.join(BASE_DIR, "data", "silver", "sgti", "2026", "consolidated_vehicles.parquet")
PATH_SILVER_SGTI_SERVICOS = os.path.join(BASE_DIR, "data", "silver", "sgti", "2026", "consolidated_services.parquet")
PATH_SILVER_SGTI_PROGRAMACAO = os.path.join(BASE_DIR, "data", "silver", "sgti", "2026", "expanded_schedule.parquet")

# Pasta de Saída (Silver Operacional)
PASTA_SAIDA = os.path.join(BASE_DIR, "data", "silver", "operational")
os.makedirs(PASTA_SAIDA, exist_ok=True)

# Caminhos de Arquivos de Saída (Arquivos Consolidados Únicos)
PARQUET_SANEADO = os.path.join(PASTA_SAIDA, "op_silver_saneado.parquet")
PARQUET_AUDITORIA_GERAL = os.path.join(PASTA_SAIDA, "op_silver_auditoria_geral.parquet")
PARQUET_TODOS_REGISTROS = os.path.join(PASTA_SAIDA, "op_silver_todos_registros.parquet")
PARQUET_QUARENTENA = os.path.join(PASTA_SAIDA, "op_silver_quarentena.parquet")
XLSX_QUARENTENA = os.path.join(PASTA_SAIDA, "op_silver_quarentena.xlsx")


# ==============================================
# 2. FUNÇÕES AUXILIARES E CONVERSORES DE PLACA
# ==============================================

NUM_PARA_LETRA = {
    "0": "A", "1": "B", "2": "C", "3": "D", "4": "E",
    "5": "F", "6": "G", "7": "H", "8": "I", "9": "J",
}
LETRA_PARA_NUM = {v: k for k, v in NUM_PARA_LETRA.items()}


def safe_float(valor: Any, default: float = 0.0) -> float:
    try:
        return float(valor) if pd.notna(valor) else default
    except (ValueError, TypeError):
        return default


def safe_int(valor: Any, default: int = 0) -> int:
    try:
        if pd.isna(valor) or valor is None or str(valor).strip() == "":
            return default
        return int(float(str(valor).strip()))
    except (ValueError, TypeError):
        return default


def normalizar_chave_texto(val: Any) -> str:
    if pd.isna(val) or val is None:
        return ""
    texto = str(val).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return re.sub(r"^0+(?=\d)", "", texto)


def normalizar_data(val: Any) -> str:
    if pd.isna(val) or val is None or str(val).strip().lower() in ("", "nan", "none", "nat"):
        return ""
    texto = str(val).strip().split(" ")[0]
    if re.match(r"^\d{4}-\d{2}-\d{2}$", texto):
        return texto
    try:
        dt = pd.to_datetime(texto, dayfirst=True, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
        return ""
    except Exception:
        return ""


def normalizar_horario_str(val: Any) -> str:
    if pd.isna(val) or not val or str(val).strip() == "":
        return ""
    try:
        partes = str(val).strip().split(":")
        horas = partes[0].zfill(2)
        minutos = partes[1].zfill(2) if len(partes) > 1 else "00"
        return f"{horas}:{minutos}"
    except Exception:
        return str(val).strip()


def limpar_placa(valor: Any) -> str:
    if pd.isna(valor) or valor is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(valor).upper()).strip()


def placa_valida(placa: str) -> bool:
    p = limpar_placa(placa)
    return bool(re.match(r"^[A-Z]{3}[0-9]{4}$", p)) or bool(
        re.match(r"^[A-Z]{3}[0-9][A-Z][0-9]{2}$", p)
    )


def antiga_para_mercosul(placa: str) -> Optional[str]:
    p = limpar_placa(placa)
    if len(p) != 7 or p[4] not in NUM_PARA_LETRA:
        return None
    return p[:4] + NUM_PARA_LETRA[p[4]] + p[5:]


def mercosul_para_antiga(placa: str) -> Optional[str]:
    p = limpar_placa(placa)
    if len(p) != 7 or p[4] not in LETRA_PARA_NUM:
        return None
    return p[:4] + LETRA_PARA_NUM[p[4]] + p[5:]


def placa_equivalente(placa: str) -> Optional[str]:
    p = limpar_placa(placa)
    if len(p) != 7:
        return None
    return (
        antiga_para_mercosul(p)
        if p[4].isdigit()
        else mercosul_para_antiga(p)
    )


def escolher_placa(linha: dict) -> str:
    sub2 = limpar_placa(linha.get("vehicle_substitute_2") or linha.get("VEICULO_SUBSTITUTO_2"))
    if sub2:
        return sub2
    sub1 = limpar_placa(linha.get("vehicle_substitute_1") or linha.get("VEICULO_SUBSTITUTO_1"))
    if sub1:
        return sub1
    return limpar_placa(linha.get("vehicle") or linha.get("VEÍCULO") or linha.get("VEICULO"))


def lista_meses_busca(mes: int) -> List[int]:
    resultado = [mes]
    for dist in range(1, 12):
        if mes - dist >= 1:
            resultado.append(mes - dist)
        if mes + dist <= 12:
            resultado.append(mes + dist)
    return resultado


def extrair_codigos(texto: Any) -> List[str]:
    if pd.isna(texto) or texto is None:
        return []
    return list(set(re.findall(r"\d+", str(texto))))


def extrair_chave_empresa(source_file: Any) -> str:
    """Extrai a chave da empresa do nome do arquivo original (ex: 'barraca_2026_01.xlsx' -> 'barraca')."""
    if pd.isna(source_file) or not str(source_file).strip():
        return ""
    stem = os.path.splitext(os.path.basename(str(source_file)))[0]
    match = re.search(r"^(.*?)[_]+\d{4}", stem)
    if match:
        return match.group(1).strip().lower()
    return stem.split("_")[0].lower()


def horario_para_minutos(horario_str: str) -> int:
    if pd.isna(horario_str) or not horario_str:
        return 0
    try:
        partes = str(horario_str).strip().split(":")
        horas = int(partes[0])
        minutos = int(partes[1]) if len(partes) > 1 else 0
        return horas * 60 + minutos
    except (ValueError, IndexError):
        return 0


def converter_tipos_para_exportacao(df: pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()
    cols_int = [
        "reference_month",
        "reference_year",
        "chassis_year",
        "sgti_file_month",
        "month_distance",
    ]
    for col in cols_int:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("Int64")

    cols_float = ["total_line_km", "time_difference_minutes"]
    for col in cols_float:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("float64")

    if "date" in df_out.columns:
        df_out["date"] = df_out["date"].apply(normalizar_data)

    cols_texto = [c for c in df_out.columns if c not in cols_int + cols_float]
    for col in cols_texto:
        df_out[col] = df_out[col].fillna("").astype(str).str.strip()

    return df_out

# ==============================================
# PADRONIZAÇÃO DE VALORES DA CAMADA SILVER
# ==============================================

MAPA_PATH = {
    "IDA": "IDA",
    "I": "IDA",
    "SAIDA": "IDA",
    "S": "IDA",
    "SAÍDA": "IDA",
    "OUTBOUND": "IDA",
    "VOLTA": "VOLTA",
    "V": "VOLTA",
    "CHEGADA": "VOLTA",
    "C": "VOLTA",
    "RETURN": "VOLTA",
}

MAPA_TIPO_VIAGEM = {
    "ESPECIFICADA": "ESPECIFICADA",
    "E": "ESPECIFICADA",
    "ESPECIFICADO": "ESPECIFICADA",
    "ESPEFICICADA": "ESPECIFICADA",
    "SPECIFIED": "ESPECIFICADA",
    "REFORÇO": "REFORÇO",
    "REFORCO": "REFORÇO",
    "R": "REFORÇO",
    "REINFORCEMENT": "REFORÇO",
}

def normalizar_path(valor: Any) -> str:
    """Remove hífens, barras e caracteres especiais e converte para 'IDA' ou 'VOLTA'."""
    if pd.isna(valor) or not valor:
        return ""
    texto = str(valor).strip().upper()
    texto_limpo = re.sub(r"[^A-Z]", "", texto)
    return MAPA_PATH.get(texto_limpo, texto)


def normalizar_tipo_viagem(valor: Any) -> str:
    """Padroniza variações de tipo para 'ESPECIFICADA' ou 'REFORÇO'."""
    if pd.isna(valor) or not valor:
        return ""
    texto = str(valor).strip().upper()
    texto_limpo = re.sub(r"[^A-Z]", "", texto)
    return MAPA_TIPO_VIAGEM.get(texto_limpo, texto)

# ==============================================
# 3. CARREGAMENTO DAS BASES BRONZE, SILVER E DIMENSÃO
# ==============================================

def carregar_dim_delegatarias() -> Dict[str, Dict[str, str]]:
    """Carrega a planilha dimensão De-Para de delegatárias."""
    if not os.path.exists(PATH_DIM_DELEGATARIAS):
        print(f"⚠️ Aviso: Tabela dimensão não encontrada em '{PATH_DIM_DELEGATARIAS}'. O De-Para de empresas não será aplicado.")
        return {}
    
    try:
        df_dim = pd.read_excel(PATH_DIM_DELEGATARIAS, dtype=str)
        dim_map = {}
        for _, r in df_dim.iterrows():
            key = str(r.get("file_company_key", "")).strip().lower()
            if key:
                dim_map[key] = {
                    "operational_delegated_code": normalizar_chave_texto(r.get("operational_delegated_code", "")),
                    "official_company_name": str(r.get("official_company_name", "")).strip(),
                }
        print(f"✅ Tabela dimensão de delegatárias carregada com sucesso ({len(dim_map)} empresas MAPEADAS).")
        return dim_map
    except Exception as e:
        print(f"❌ Erro ao carregar tabela dimensão '{PATH_DIM_DELEGATARIAS}': {e}")
        return {}


def carregar_bronze_operacional(caminho: str) -> pd.DataFrame:
    """Carrega apenas a versão mais recente/retificada (RET2 > RET1 > Base) de cada (Empresa, Mês)."""
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Caminho Bronze Operacional não localizado: {caminho}")

    arquivos = []
    if os.path.isdir(caminho):
        for root, _, files in os.walk(caminho):
            for f in files:
                if f.endswith(".parquet") or f.endswith(".xlsx"):
                    arquivos.append(os.path.join(root, f))
    else:
        arquivos = [caminho]

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {caminho}")

    # Mapeia e seleciona apenas a maior retificação por (empresa, mês)
    grupos_retificacao = defaultdict(list)

    for fpath in arquivos:
        fname = os.path.basename(fpath)
        key_empresa = extrair_chave_empresa(fname)
        
        # Extrai mês (ex: _2026_01 -> 01)
        match_mes = re.search(r"_\d{4}_(\d{2})", fname)
        mes = match_mes.group(1) if match_mes else "00"

        # Extrai RET (ex: _RET2 -> 2)
        match_ret = re.search(r"_RET(\d+)", fname, re.IGNORECASE)
        num_ret = int(match_ret.group(1)) if match_ret else 0

        st_mtime = os.path.getmtime(fpath)
        
        grupos_retificacao[(key_empresa, mes)].append({
            "path": fpath,
            "num_ret": num_ret,
            "mtime": st_mtime
        })

    # Filtra apenas o arquivo com maior RET (e mtime como desempate)
    arquivos_finais = []
    for (empresa, mes), lista in grupos_retificacao.items():
        lista_ordenada = sorted(lista, key=lambda x: (x["num_ret"], x["mtime"]), reverse=True)
        escolhido = lista_ordenada[0]["path"]
        arquivos_finais.append(escolhido)
        if len(lista) > 1:
            print(f"   ℹ️ Bronze OP [{empresa} - Mês {mes}]: Selecionado '{os.path.basename(escolhido)}' entre {len(lista)} versões disponíveis.")

    # Lê e concatena apenas os arquivos válidos
    dfs = []
    for f in arquivos_finais:
        if f.endswith(".parquet"):
            dfs.append(pd.read_parquet(f))
        elif f.endswith(".xlsx"):
            dfs.append(pd.read_excel(f, dtype=str))

    return pd.concat(dfs, ignore_index=True)


def carregar_e_indexar_apoio():
    print("\n" + "=" * 70)
    print("INDEXANDO BASES DA CAMADA SILVER SGTI E METADADOS")
    print("=" * 70)

    print("[1/3] Carregando Silver SGTI Veículos...")
    df_sgti_veic = pd.read_parquet(PATH_SILVER_SGTI_VEICULOS)

    indice_veiculos = {}
    for row in df_sgti_veic.to_dict("records"):
        placa = limpar_placa(row.get("plate") or row.get("Placa"))
        mes = safe_int(row.get("file_month") or row.get("MES_ARQUIVO"))
        if placa:
            indice_veiculos[f"{placa}|{mes}"] = {
                "plate": placa,
                "delegated_company_code": normalizar_chave_texto(
                    row.get("delegated_company_code") or row.get("Código")
                ),
                "delegated_company_name": row.get("delegated_company_name") or row.get("Delegatária") or "",
                "shared_companies": row.get("authorized_shared_companies") or row.get("Empresas autorizadas a compartilhar") or "",
                "file_month": mes,
                "chassis_year": row.get("chassis_year") or row.get("ano carroceria"),
            }

    def localizar_no_sgti(placa: str, mes_referencia: int) -> Optional[dict]:
        candidatos = [limpar_placa(placa)]
        eq = placa_equivalente(placa)
        if eq:
            candidatos.append(eq)
        for p_busca in candidatos:
            for mes in lista_meses_busca(mes_referencia):
                chave = f"{p_busca}|{mes}"
                if chave in indice_veiculos:
                    dados = indice_veiculos[chave].copy()
                    dados["month_distance"] = abs(mes - mes_referencia)
                    return dados
        return None

    print("[2/3] Carregando Silver SGTI Serviços/Contratos...")
    df_sgti_contratos = pd.read_parquet(PATH_SILVER_SGTI_SERVICOS)

    indice_contratos = {}
    for row in df_sgti_contratos.to_dict("records"):
        try:
            linha_id = normalizar_chave_texto(row.get("service_id") or row.get("Linha"))
            mes_arq = safe_int(row.get("file_month") or row.get("MES_ARQUIVO"))
            chave = f"{linha_id}|{mes_arq}"

            km_total = row.get("total_line_km")
            if km_total is None:
                km_total = (
                    safe_float(row.get("Km normal piso I"))
                    + safe_float(row.get("Km normal piso II"))
                    + safe_float(row.get("Km normal piso III"))
                    + (safe_float(row.get("Km E/S piso I")) * 2)
                    + (safe_float(row.get("Km E/S piso II")) * 2)
                    + (safe_float(row.get("Km E/S piso III")) * 2)
                )

            indice_contratos[chave] = {
                "delegated_company_code": row.get("delegated_company_cod") or row.get("delegated_company_code") or row.get("Código do deleg."),
                "delegated_company_name": row.get("delegated_company_name") or row.get("Delegatário"),
                "total_line_km": km_total,
            }
        except Exception:
            continue

    print("[3/3] Carregando Silver SGTI Programação Expandida...")
    df_sgti_prog = pd.read_parquet(PATH_SILVER_SGTI_PROGRAMACAO)

    indice_programacao = {}
    for row in df_sgti_prog.to_dict("records"):
        d_norm = normalizar_data(row.get("DATE") or row.get("date") or row.get("DATA"))
        l_norm = normalizar_chave_texto(row.get("SERVICE_ID") or row.get("service_id") or row.get("NUMERO_LINHA"))
        h_norm = normalizar_horario_str(row.get("SCHEDULE_TIME") or row.get("schedule_time") or row.get("HORARIO"))

        chave = (d_norm, l_norm)
        if chave not in indice_programacao:
            indice_programacao[chave] = []
        if h_norm:
            indice_programacao[chave].append(h_norm)

    return localizar_no_sgti, indice_contratos, indice_programacao


# ==============================================
# 4. EXECUTOR PRINCIPAL COM CACHE E UPSERT
# ==============================================

def processar_op_silver(df_incremental: Optional[pd.DataFrame] = None):
    inicio_geral = time.time()

    mapa_delegatarias = carregar_dim_delegatarias()

    if df_incremental is not None and not df_incremental.empty:
        print("\n⚡ MODO INCREMENTAL ATIVADO: Processando apenas registros de reincorporação via Cache...")
        df_op = df_incremental.copy()
    else:
        print("\nCarregando dados completos da Bronze Operacional...")
        df_op = carregar_bronze_operacional(PATH_BRONZE_OPERACIONAL)

    df_op.columns = df_op.columns.str.strip()
    localizar_no_sgti, indice_contratos, indice_programacao = carregar_e_indexar_apoio()

    registros_op = df_op.to_dict("records")
    print(f"\nTotal de registros a processar nesta etapa: {len(registros_op):,}\n")

    resultado = []
    for i, linha in enumerate(registros_op):
        # 0. Resolução de Empresa e Código via Tabela Dimensão (De-Para)
        source_file = linha.get("source_file", "")
        file_key = extrair_chave_empresa(source_file)
        info_dim = mapa_delegatarias.get(file_key, {})

        cod_deleg_op = (
            linha.get("operational_delegated_code")
            or linha.get("delegated_company_code")
            or linha.get("company_code")
            or linha.get("COD_DELEGATARIA")
        )
        if not cod_deleg_op or str(cod_deleg_op).strip() in ["", "nan", "None"]:
            cod_deleg_op = info_dim.get("operational_delegated_code", "")

        company_name = linha.get("company_name") or linha.get("company") or linha.get("EMPRESA")
        if not company_name or str(company_name).strip() in ["", "nan", "None"]:
            company_name = info_dim.get("official_company_name") or file_key

        placa_utilizada = escolher_placa(linha)
        mes_ref = safe_int(linha.get("reference_month") or linha.get("MES_REFERENCIA") or linha.get("file_month"))
        ano_ref = safe_int(linha.get("reference_year") or linha.get("ANO_REFERENCIA"))

        # Suporte a extração de mês/ano da data caso estejam nulos
        if mes_ref == 0 or ano_ref == 0:
            data_str = normalizar_data(linha.get("date") or linha.get("DATA"))
            if data_str:
                dt_p = pd.to_datetime(data_str, errors="coerce")
                if pd.notna(dt_p):
                    mes_ref = dt_p.month
                    ano_ref = dt_p.year

        id_servico = linha.get("service_id") or linha.get("ID_SERVICO") or linha.get("Linha")
        id_servico_norm = normalizar_chave_texto(id_servico)

        data_op = linha.get("date") or linha.get("DATA")
        data_op_norm = normalizar_data(data_op)

        horario_op = linha.get("schedule_time") or linha.get("HORARIO")
        horario_op_norm = normalizar_horario_str(horario_op)

        # LEITURA E PADRONIZAÇÃO DO SENTIDO (PATH) E TIPO
        raw_path = linha.get("path") or linha.get("SENTIDO") or linha.get("PATH")
        path_norm = normalizar_path(raw_path)

        raw_tipo = linha.get("type") or linha.get("TIPO_VIAGEM") or ""
        tipo_viagem_norm = normalizar_tipo_viagem(raw_tipo)

        # 1. Validação de Veículo
        vehicle_status, status_reason, match_type, requires_review = "", "", "", "NÃO"
        normalized_plate, sgti_file_month, month_distance = "", None, None
        sgti_owner_code, sgti_owner_name, compart_text = "", "", ""
        chassis_year = None

        if placa_utilizada == "":
            vehicle_status, status_reason, requires_review = "PLACA_AUSENTE", "VEICULO E SUBSTITUTOS VAZIOS", "SIM"
        elif not placa_valida(placa_utilizada):
            vehicle_status, status_reason, requires_review = "PLACA_INVALIDA", "FORMATO_INVALIDO", "SIM"
        else:
            encontrado = localizar_no_sgti(placa_utilizada, mes_ref)
            if encontrado:
                normalized_plate = encontrado["plate"]
                sgti_owner_code = encontrado["delegated_company_code"]
                sgti_owner_name = encontrado["delegated_company_name"]
                compart_text = encontrado["shared_companies"]
                sgti_file_month = encontrado["file_month"]
                month_distance = encontrado["month_distance"]
                chassis_year = encontrado["chassis_year"]

                if placa_utilizada == normalized_plate:
                    match_type = "MATCH_DIRETO" if month_distance == 0 else "MATCH_COMPETENCIA_PROXIMA"
                else:
                    match_type = "ANTIGA_PARA_MERCOSUL" if len(placa_utilizada) == 7 and placa_utilizada[4].isdigit() else "MERCOSUL_PARA_ANTIGA"
            else:
                vehicle_status, status_reason, requires_review = "SEM_MATCH", "PLACA_NAO_LOCALIZADA", "SIM"

        # 2. Validação de Contrato/Serviço
        contract_delegated_code, contract_delegated_name, delegated_matches, total_line_km = None, None, "NAO", None
        try:
            chave_contrato = f"{id_servico_norm}|{mes_ref}"
            if chave_contrato in indice_contratos:
                info_c = indice_contratos[chave_contrato]
                contract_delegated_code = info_c["delegated_company_code"]
                contract_delegated_name = info_c["delegated_company_name"]
                total_line_km = info_c["total_line_km"]

                if normalizar_chave_texto(cod_deleg_op) != "" and normalizar_chave_texto(cod_deleg_op) == normalizar_chave_texto(contract_delegated_code):
                    delegated_matches, contract_validation = "SIM", "OK"
                else:
                    contract_validation = "DIVERGENTE"
            else:
                contract_validation = "NAO_LOCALIZADO"
        except Exception:
            contract_validation = "NAO_LOCALIZADO"

        # 3. Validação de Programação de Viagens
        chave_op = (data_op_norm, id_servico_norm)
        schedule_status, schedule_reason, scheduled_time, time_difference_minutes, suggested_action = None, None, None, None, None
        is_reforco = tipo_viagem_norm in ["REFORÇO", "REFORCO", "REINFORCEMENT"]

        if chave_op not in indice_programacao:
            if is_reforco:
                schedule_status = "REFORCO_DECLARADO"
            else:
                schedule_status, schedule_reason = "INCONSISTENCIA_DIA", "SEM_PROGRAMACAO_NA_DATA"
                if tipo_viagem_norm in ["ESPECIFICADA", "SPECIFIED"]:
                    suggested_action = "REFORCO"
        else:
            horarios_sgti = indice_programacao[chave_op]
            if is_reforco:
                schedule_status = "REFORCO_DECLARADO"
            else:
                min_op = horario_para_minutos(horario_op_norm)
                melhor_h = horarios_sgti[0] if horarios_sgti else ""
                menor_dif = abs(min_op - horario_para_minutos(melhor_h))

                for h in horarios_sgti[1:]:
                    dif = abs(min_op - horario_para_minutos(h))
                    if dif < menor_dif:
                        menor_dif, melhor_h = dif, h

                scheduled_time, time_difference_minutes = melhor_h, menor_dif
                if menor_dif == 0:
                    schedule_status = "OK"
                elif menor_dif <= 20:
                    schedule_status = "MATCH_APROXIMADO"
                else:
                    schedule_status = "HORARIO_DIFERENTE"
                    if tipo_viagem_norm in ["ESPECIFICADA", "SPECIFIED"]:
                        suggested_action = "REFORCO"

        resultado.append({
            "ingestion_id": linha.get("ingestion_id"),
            "source_file": source_file,
            "source_row": linha.get("source_row"),
            "company_name": company_name,
            "service_id": id_servico,
            "date": data_op_norm,
            "schedule_time": horario_op_norm,
            "path": path_norm,
            "type": tipo_viagem_norm,
            "reference_month": mes_ref,
            "reference_year": ano_ref,
            "operational_delegated_code": cod_deleg_op,
            "original_vehicle": linha.get("vehicle") or linha.get("VEÍCULO") or linha.get("VEICULO"),
            "vehicle_substitute_1": linha.get("vehicle_substitute_1") or linha.get("VEICULO_SUBSTITUTO_1"),
            "vehicle_substitute_2": linha.get("vehicle_substitute_2") or linha.get("VEICULO_SUBSTITUTO_2"),
            "used_plate": placa_utilizada,
            "normalized_plate": normalized_plate,
            "chassis_year": chassis_year,
            "sgti_file_month": sgti_file_month,
            "month_distance": month_distance,
            "sgti_owner_code": sgti_owner_code,
            "sgti_owner_name": sgti_owner_name,
            "compart_text": compart_text,
            "match_type": match_type,
            "vehicle_status": vehicle_status,
            "status_reason": status_reason,
            "requires_review": requires_review,
            "contract_delegated_code": contract_delegated_code,
            "contract_delegated_name": contract_delegated_name,
            "delegated_matches": delegated_matches,
            "contract_validation": contract_validation,
            "total_line_km": total_line_km,
            "schedule_status": schedule_status,
            "schedule_reason": schedule_reason,
            "scheduled_time": scheduled_time,
            "time_difference_minutes": time_difference_minutes,
            "suggested_action": suggested_action,
            "reincorporated_at": linha.get("reincorporated_at") or time.strftime("%Y-%m-%d %H:%M:%S")
        })

    df_novos = pd.DataFrame(resultado)

    # 5. Regras de Vínculo de Delegatária
    link_types, final_vehicle_statuses, final_reasons, final_reviews, sharing_codes = [], [], [], [], []
    for linha in df_novos.to_dict("records"):
        status, motivo, exige = linha["vehicle_status"], linha["status_reason"], linha["requires_review"]
        tipo_vinculo = ""
        codigo_operacional, codigo_sgti = normalizar_chave_texto(linha["operational_delegated_code"]), normalizar_chave_texto(linha["sgti_owner_code"])
        codigos_comp = extrair_codigos(linha["compart_text"])
        sharing_codes.append(";".join(codigos_comp))

        if status not in ["PLACA_AUSENTE", "PLACA_INVALIDA", "SEM_MATCH"]:
            if codigo_operacional == codigo_sgti:
                tipo_vinculo = "PROPRIETARIA"
                distancia = linha["month_distance"]
                status, motivo, exige = ("APROVADO", "CODIGO_PROPRIETARIO", "NÃO") if pd.notna(distancia) and distancia <= 1 else ("APROVADO_COM_RESSALVA", f"DISTANCIA_MESES={distancia}", "SIM")
            elif codigo_operacional in codigos_comp:
                tipo_vinculo = "COMPARTILHADA"
                distancia = linha["month_distance"]
                status, motivo, exige = ("APROVADO_POR_COMPARTILHAMENTO", "COMPARTILHAMENTO_AUTORIZADO", "NÃO") if pd.notna(distancia) and distancia <= 1 else ("APROVADO_POR_COMPARTILHAMENTO", f"COMPARTILHADA_DISTANCIA={distancia}", "SIM")
            else:
                tipo_vinculo, status, motivo, exige = "SEM_VINCULO", "DIVERGENCIA_DELEGATARIA", f"OPERACIONAL={codigo_operacional};SGTI={codigo_sgti}", "SIM"

        link_types.append(tipo_vinculo)
        final_vehicle_statuses.append(status)
        final_reasons.append(motivo)
        final_reviews.append(exige)

    df_novos["shared_delegated_codes"] = sharing_codes
    df_novos["link_type"] = link_types
    df_novos["vehicle_status"] = final_vehicle_statuses
    df_novos["status_reason"] = final_reasons
    df_novos["requires_review"] = final_reviews

    # Cálculo do Status Final
    def calcular_status_final(row: pd.Series) -> str:
        veic_ok = row["vehicle_status"] in ["APROVADO", "APROVADO_COM_RESSALVA", "APROVADO_POR_COMPARTILHAMENTO"]
        serv_ok = row["contract_validation"] == "OK"
        prog_ok = row["schedule_status"] in ["OK", "MATCH_APROXIMADO", "REFORCO_DECLARADO"]
        total_inc = sum([not veic_ok, not serv_ok, not prog_ok])
        if total_inc == 0: return "APROVADO"
        elif total_inc > 1: return "MAIS DE UMA INCONSISTENCIA"
        elif not veic_ok: return "INCONSISTENCIA VEICULO"
        elif not serv_ok: return "INCONSISTENCIA SERVICO"
        else: return "INCONSISTENCIA PROGRAMACAO"

    df_novos["final_status"] = df_novos.apply(calcular_status_final, axis=1)

    # ==============================================
    # 6. CONSOLIDAÇÃO VIA CACHE (UPSERT NOS PARQUETS)
    # ==============================================
    if os.path.exists(PARQUET_TODOS_REGISTROS) and df_incremental is not None:
        print("\n📦 Mesclando registros novos ao Cache Parquet existente...")
        df_cache = pd.read_parquet(PARQUET_TODOS_REGISTROS)
        df_auditoria = pd.concat([df_cache, df_novos], ignore_index=True)
        
        # Upsert: Ordena pelo timestamp e remove duplicados mantendo o mais recente
        df_auditoria = df_auditoria.sort_values(by="reincorporated_at", na_position="first") \
                                   .drop_duplicates(subset=["company_name", "service_id", "date", "schedule_time", "path"], keep="last")
    else:
        df_auditoria = df_novos

    df_quarentena = df_auditoria[df_auditoria["vehicle_status"].isin(["PLACA_AUSENTE", "PLACA_INVALIDA"])].copy()
    df_saneada = df_auditoria[~df_auditoria["vehicle_status"].isin(["PLACA_AUSENTE", "PLACA_INVALIDA"])].copy()
    df_auditoria_geral = df_auditoria[(df_auditoria["final_status"] != "APROVADO") | (df_auditoria["requires_review"] == "SIM")].copy()

    df_saneada = converter_tipos_para_exportacao(df_saneada)
    df_auditoria = converter_tipos_para_exportacao(df_auditoria)
    df_quarentena = converter_tipos_para_exportacao(df_quarentena)
    df_auditoria_geral = converter_tipos_para_exportacao(df_auditoria_geral)

    # Salvamento nos nomes exatos de Parquet definidos no projeto
    df_saneada.to_parquet(PARQUET_SANEADO, index=False)
    df_auditoria.to_parquet(PARQUET_TODOS_REGISTROS, index=False)
    df_auditoria_geral.to_parquet(PARQUET_AUDITORIA_GERAL, index=False)
    df_quarentena.to_parquet(PARQUET_QUARENTENA, index=False)

    if len(df_quarentena) <= 1048576:
        df_quarentena.to_excel(XLSX_QUARENTENA, index=False)

    print("\n" + "=" * 70)
    print("CAMADA SILVER OPERACIONAL ATUALIZADA COM SUCESSO (CACHE PARQUET)")
    print(f" -> {os.path.basename(PARQUET_SANEADO)} ({len(df_saneada):,} linhas)")
    print(f" -> {os.path.basename(PARQUET_TODOS_REGISTROS)} ({len(df_auditoria):,} linhas)")
    print("=" * 70)


if __name__ == "__main__":
    processar_op_silver()