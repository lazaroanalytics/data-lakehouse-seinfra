"""
op_sgti_gold_layer.py

Consolida a visão Gold de consistência mensal focando exclusivamente
nas delegatárias operacionais via CÓDIGO de empresa.
"""

import os
import pandas as pd

# ==============================================
# 1. CAMINHOS DOS ARQUIVOS
# ==============================================
BASE_DIR = os.getcwd()

PATH_METADATA_DELEGATARIAS = os.path.join(BASE_DIR, "data", "metadata", "dim_delegatarias.parquet")

PATH_SILVER_OP = os.path.join(BASE_DIR, "data", "silver", "operational", "op_silver_saneado.parquet")
PATH_SILVER_VEICULOS = os.path.join(BASE_DIR, "data", "silver", "sgti", "2026", "consolidated_vehicles.parquet")
PATH_SILVER_SERVICOS = os.path.join(BASE_DIR, "data", "silver", "sgti", "2026", "consolidated_services.parquet")

PATH_GOLD_SAIDA = os.path.join(BASE_DIR, "data", "gold", "operational")
os.makedirs(PATH_GOLD_SAIDA, exist_ok=True)

FILE_GOLD_PARQUET = os.path.join(PATH_GOLD_SAIDA, "op_sgti_gold_consistencia_mensal.parquet")
FILE_GOLD_EXCEL = os.path.join(PATH_GOLD_SAIDA, "op_sgti_gold_consistencia_mensal.xlsx")


def normalizar_codigo(val) -> str:
    """Padroniza códigos ('5030.0', ' 5030 ', '05030') para a string limpa '5030'."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        s = str(int(s))
    return s


def carregar_dim_delegatarias() -> pd.DataFrame:
    """Carrega a dimensão oficial trazendo os códigos limpos e seus respectivos nomes."""
    caminho = PATH_METADATA_DELEGATARIAS
    
    if os.path.exists(caminho):
        df = pd.read_parquet(caminho)
    elif os.path.exists(caminho.replace(".parquet", ".csv")):
        df = pd.read_csv(caminho.replace(".parquet", ".csv"), dtype=str)
    elif os.path.exists(caminho.replace(".parquet", ".xlsx")):
        df = pd.read_excel(caminho.replace(".parquet", ".xlsx"), dtype=str)
    else:
        raise FileNotFoundError(f"Não foi possível encontrar dim_delegatarias em: {caminho}")

    # Busca a coluna de código na dimensão
    col_code = "operational_delegated_code" if "operational_delegated_code" in df.columns else df.columns[0]
    col_name = [c for c in df.columns if any(k in c.lower() for k in ["name", "nome", "razao", "social"])][0]

    df_clean = pd.DataFrame()
    df_clean["company_code"] = df[col_code].apply(normalizar_codigo)
    df_clean["company_name"] = df[col_name].astype(str).str.strip()

    return df_clean.drop_duplicates(subset=["company_code"])


def gerar_camada_op_sgti_gold():
    print("======================================================================")
    print("GERANDO CAMADA GOLD DE CONSISTÊNCIA MENSAL (CÓDIGO PADRONIZADO)")
    print("======================================================================\n")

    # 1. CARREGA DIMENSÃO OFICIAL (28 EMPRESAS)
    df_deleg = carregar_dim_delegatarias()
    codigos_dim = set(df_deleg["company_code"].unique())
    print(f"🎯 28 Delegatárias oficiais carregadas de dim_delegatarias.")
    print(f"   Códigos esperados: {sorted(list(codigos_dim))[:10]}...\n")

    # 2. PRATA OPERACIONAL
    df_op = pd.read_parquet(PATH_SILVER_OP)
    col_emp_op = "operational_delegated_code"
    
    df_op["company_code"] = df_op[col_emp_op].apply(normalizar_codigo)
    df_op = df_op[df_op["company_code"].isin(codigos_dim)].copy()
    df_op["reference_month"] = pd.to_numeric(df_op["reference_month"], errors="coerce")

    # Métricas Operacionais
    plate_used_col = "used_plate"
    plate_norm_col = "normalized_plate"
    service_col = "service_id"

    df_op["is_cadastrado"] = df_op[plate_norm_col].fillna("").astype(str).ne("") & df_op.get("vehicle_status", pd.Series()).ne("SEM_MATCH")

    def calcular_metricas_op(group):
        valid = group[group[plate_used_col].fillna("").astype(str).str.strip() != ""]
        cad = valid[valid["is_cadastrado"]]
        nao_cad = valid[~valid["is_cadastrado"]]

        return pd.Series({
            "qtd_veiculos_utilizados_total": valid[plate_used_col].nunique(),
            "qtd_veiculos_operados_cadastrados": cad[plate_used_col].nunique(),
            "qtd_veiculos_operados_nao_cadastrados": nao_cad[plate_used_col].nunique(),
            "qtd_viagens_realizadas": len(group),
            "qtd_viagens_veiculo_nao_cadastrado": len(group[~group["is_cadastrado"]]),
            "producao_quilometrica_km": pd.to_numeric(group.get("total_line_km"), errors="coerce").sum(),
            "qtd_servicos_declarados": group[service_col].nunique()
        })

    if not df_op.empty:
        op_resumo = df_op.groupby(["company_code", "reference_month"]).apply(calcular_metricas_op, include_groups=False).reset_index()
    else:
        op_resumo = pd.DataFrame(columns=["company_code", "reference_month"])

    # 3. BASES CADASTRAIS SGTI
    # A. Veículos Cadastrados
    df_veic = pd.read_parquet(PATH_SILVER_VEICULOS)
    col_emp_veic = "delegated_company_code"

    df_veic["company_code"] = df_veic[col_emp_veic].apply(normalizar_codigo)
    df_veic = df_veic[df_veic["company_code"].isin(codigos_dim)].copy()
    
    col_ano = "chassis_year" if "chassis_year" in df_veic.columns else "Ano carroceria"
    df_veic["idade_veiculo"] = 2026 - pd.to_numeric(df_veic[col_ano], errors="coerce") if col_ano in df_veic.columns else None
    df_veic["reference_month"] = pd.to_numeric(df_veic.get("file_month"), errors="coerce")

    veic_resumo = df_veic.groupby(["company_code", "reference_month"]).agg(
        qtd_veiculos_cadastrados=("plate", "nunique"),
        idade_media_frota_cadastrada=("idade_veiculo", "mean")
    ).reset_index()

    # B. Serviços Cadastrados
    df_serv = pd.read_parquet(PATH_SILVER_SERVICOS)
    col_emp_serv = "Código do deleg."

    df_serv["company_code"] = df_serv[col_emp_serv].apply(normalizar_codigo)
    df_serv = df_serv[df_serv["company_code"].isin(codigos_dim)].copy()
    df_serv["reference_month"] = pd.to_numeric(df_serv.get("file_month"), errors="coerce")

    col_linha = "Linha" if "Linha" in df_serv.columns else df_serv.columns[0]
    serv_resumo = df_serv.groupby(["company_code", "reference_month"]).agg(
        qtd_servicos_empresa=(col_linha, "nunique")
    ).reset_index()

    # 4. UNIFICAÇÃO FINAL POR CÓDIGO
    keys = ["company_code", "reference_month"]
    
    if not op_resumo.empty:
        gold_df = pd.merge(op_resumo, veic_resumo, on=keys, how="left")
        gold_df = pd.merge(gold_df, serv_resumo, on=keys, how="left")
    else:
        gold_df = pd.merge(veic_resumo, serv_resumo, on=keys, how="outer")

    # Anexa a Razão Social vindo de dim_delegatarias
    gold_df = pd.merge(gold_df, df_deleg[["company_code", "company_name"]], on="company_code", how="left")

    cols_ordem = [
        "company_code",
        "company_name",
        "reference_month",
        "qtd_veiculos_utilizados_total",
        "qtd_veiculos_operados_cadastrados",
        "qtd_veiculos_operados_nao_cadastrados",
        "qtd_viagens_realizadas",
        "qtd_viagens_veiculo_nao_cadastrado",
        "producao_quilometrica_km",
        "qtd_servicos_declarados",
        "qtd_veiculos_cadastrados",
        "idade_media_frota_cadastrada",
        "qtd_servicos_empresa"
    ]

    gold_df = gold_df.reindex(columns=cols_ordem).fillna(0)
    gold_df["reference_month"] = pd.to_numeric(gold_df["reference_month"], errors="coerce").fillna(0).astype(int)
    gold_df = gold_df[gold_df["reference_month"] > 0]
    
    gold_df.sort_values(by=["company_code", "reference_month"], inplace=True)

    # Salvamento final
    gold_df.to_parquet(FILE_GOLD_PARQUET, index=False)
    gold_df.to_excel(FILE_GOLD_EXCEL, index=False)

    print(f"✅ CAMADA GOLD GERADA COM SUCESSO ({len(gold_df)} registros gerados):")
    print(f" -> {FILE_GOLD_PARQUET}")
    print(f" -> {FILE_GOLD_EXCEL}")


if __name__ == "__main__":
    gerar_camada_op_sgti_gold()