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

# ============================================================
# REGRA DE VEICULO CADASTRADO NO SGTI
# ============================================================

    VALID_MATCH_TYPES = {
        "MATCH_DIRETO",
        "ANTIGA_PARA_MERCOSUL",
        "MERCOSUL_PARA_ANTIGA",
    }

    df_op["is_cadastrado"] = (
        df_op["match_type"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .isin(VALID_MATCH_TYPES)
)

    def calcular_metricas_op(group):

        # ========================================================
        # VEICULOS UTILIZADOS
        # ========================================================

        valid = group[
            group[plate_norm_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .ne("")
        ].copy()

        cad = valid[
            valid["is_cadastrado"]
        ].copy()

        nao_cad = valid[
            ~valid["is_cadastrado"]
        ].copy()


        # ========================================================
        # IDADE MEDIA DA FROTA UTILIZADA
        # ========================================================
        # Somente veiculos classificados como cadastrados.
        # Cada placa participa apenas uma vez da media.
        # ========================================================

        frota_idade = cad[
            [
                plate_norm_col,
                "chassis_year",
                "reference_year"
            ]
        ].copy()


        # Converter anos para numerico
        frota_idade["chassis_year"] = pd.to_numeric(
            frota_idade["chassis_year"],
            errors="coerce"
        )

        frota_idade["reference_year"] = pd.to_numeric(
            frota_idade["reference_year"],
            errors="coerce"
        )


        # Remover registros sem informacoes essenciais
        frota_idade = frota_idade.dropna(
            subset=[
                plate_norm_col,
                "chassis_year",
                "reference_year"
            ]
        )


        # Manter somente anos de carroceria validos
        frota_idade = frota_idade[
            (frota_idade["chassis_year"] > 0)
            &
            (
                frota_idade["chassis_year"]
                <= frota_idade["reference_year"]
            )
        ].copy()


        # Cada placa participa somente uma vez da media
        frota_idade = frota_idade.drop_duplicates(
            subset=[plate_norm_col]
        )


        # Calcular idade
        frota_idade["vehicle_age"] = (
            frota_idade["reference_year"]
            - frota_idade["chassis_year"]
        )


        # Quantidade de veiculos que participaram da media
        qtd_veiculos_com_idade = (
            frota_idade[plate_norm_col].nunique()
        )


        # Calcular idade media
        if qtd_veiculos_com_idade > 0:

            idade_media_frota_utilizada = round(
                frota_idade["vehicle_age"].mean(),
                2
            )

        else:

            idade_media_frota_utilizada = pd.NA


        # ========================================================
        # RETORNO DAS METRICAS OPERACIONAIS
        # ========================================================

        return pd.Series({

            "qtd_veiculos_utilizados_total":
                valid[plate_norm_col].nunique(),

            "qtd_veiculos_operados_cadastrados":
                cad[plate_norm_col].nunique(),

            "qtd_veiculos_operados_nao_cadastrados":
                nao_cad[plate_norm_col].nunique(),

            "idade_media_frota_utilizada":
                idade_media_frota_utilizada,

            "qtd_veiculos_com_idade_calculada":
                qtd_veiculos_com_idade,

            "qtd_viagens_realizadas":
                len(group),

            "qtd_viagens_veiculo_nao_cadastrado":
                len(group[~group["is_cadastrado"]]),

            "producao_quilometrica_km":
                pd.to_numeric(
                    group.get("total_line_km"),
                    errors="coerce"
                ).sum(),

            "qtd_servicos_declarados":
                group[service_col].nunique()

        })

    if not df_op.empty:
        op_resumo = df_op.groupby(["company_code", "reference_month"]).apply(calcular_metricas_op, include_groups=False).reset_index()
    else:
        op_resumo = pd.DataFrame(columns=["company_code", "reference_month"])

    # 3. BASES CADASTRAIS SGTI
        # ============================================================
    # A. VEICULOS CADASTRADOS SGTI
    # ============================================================

    df_veic = pd.read_parquet(PATH_SILVER_VEICULOS)

    col_emp_veic = "delegated_company_code"

    # Padronizar codigo da delegataria
    df_veic["company_code"] = (
        df_veic[col_emp_veic]
        .apply(normalizar_codigo)
    )

    # Manter somente delegatarias do universo operacional
    df_veic = df_veic[
        df_veic["company_code"].isin(codigos_dim)
    ].copy()


    # ============================================================
    # CONSIDERAR SOMENTE VEICULOS ATIVOS
    # ============================================================

    df_veic["situation"] = (
        df_veic["situation"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df_veic = df_veic[
        df_veic["situation"] == "ATIVO"
    ].copy()


    # ============================================================
    # TRATAR MES DE REFERENCIA
    # ============================================================

    df_veic["reference_month"] = pd.to_numeric(
        df_veic["file_month"],
        errors="coerce"
    )

    df_veic = df_veic[
        df_veic["reference_month"].between(1, 12)
    ].copy()


    # ============================================================
    # NORMALIZAR PLACA
    # ============================================================

    df_veic["plate"] = (
        df_veic["plate"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df_veic = df_veic[
        df_veic["plate"] != ""
    ].copy()


    # ============================================================
    # TRATAR ANO DA CARROCERIA
    # ============================================================

    col_ano = "chassis_year"

    df_veic[col_ano] = pd.to_numeric(
        df_veic[col_ano],
        errors="coerce"
    )


    # ============================================================
    # UMA PLACA POR EMPRESA / MES
    # ============================================================
    #
    # Como a base SGTI e cadastral, a mesma placa pode aparecer
    # novamente em cada arquivo mensal.
    #
    # Isso e correto.
    #
    # Dentro do mesmo mes, entretanto, cada placa deve contar
    # apenas uma vez.
    # ============================================================

    veiculos_mes = (
        df_veic[
            [
                "company_code",
                "reference_month",
                "plate",
                col_ano
            ]
        ]
        .drop_duplicates(
            subset=[
                "company_code",
                "reference_month",
                "plate"
            ]
        )
        .copy()
    )


    # ============================================================
    # QUANTIDADE DE VEICULOS ATIVOS CADASTRADOS
    # ============================================================

    qtd_frota_sgti = (
        veiculos_mes
        .groupby(
            [
                "company_code",
                "reference_month"
            ]
        )
        .agg(
            qtd_veiculos_cadastrados=(
                "plate",
                "nunique"
            )
        )
        .reset_index()
    )


    # ============================================================
    # BASE VALIDA PARA IDADE
    # ============================================================

    frota_idade_sgti = veiculos_mes[
        veiculos_mes[col_ano].notna()
    ].copy()

    # Ano deve ser valido para 2026
    frota_idade_sgti = frota_idade_sgti[
        (frota_idade_sgti[col_ano] > 0)
        &
        (frota_idade_sgti[col_ano] <= 2026)
    ].copy()


    # ============================================================
    # CALCULAR IDADE
    # ============================================================

    frota_idade_sgti["idade_veiculo"] = (
        2026
        - frota_idade_sgti[col_ano]
    )


    # ============================================================
    # IDADE MEDIA E QUANTIDADE COM IDADE
    # ============================================================

    idade_frota_sgti = (
        frota_idade_sgti
        .groupby(
            [
                "company_code",
                "reference_month"
            ]
        )
        .agg(
            idade_media_frota_cadastrada=(
                "idade_veiculo",
                "mean"
            ),
            qtd_veiculos_cadastrados_com_idade=(
                "plate",
                "nunique"
            )
        )
        .reset_index()
    )


    # Arredondar idade media
    idade_frota_sgti[
        "idade_media_frota_cadastrada"
    ] = (
        idade_frota_sgti[
            "idade_media_frota_cadastrada"
        ]
        .round(2)
    )


    # ============================================================
    # JUNTAR QUANTIDADE + IDADE
    # ============================================================

    veic_resumo = pd.merge(
        qtd_frota_sgti,
        idade_frota_sgti,
        on=[
            "company_code",
            "reference_month"
        ],
        how="left"
    )


    # Quantidade com idade pode receber zero
    veic_resumo[
        "qtd_veiculos_cadastrados_com_idade"
    ] = (
        veic_resumo[
            "qtd_veiculos_cadastrados_com_idade"
        ]
        .fillna(0)
        .astype(int)
    )

       # ============================================================
    # B. SERVICOS CADASTRADOS SGTI
    # ============================================================

    df_serv = pd.read_parquet(PATH_SILVER_SERVICOS)

    col_emp_serv = "Código do deleg."
    col_linha = "Linha"
    col_situacao_linha = "Situação da linha"


    # ============================================================
    # PADRONIZAR CODIGO DA DELEGATARIA
    # ============================================================

    df_serv["company_code"] = (
        df_serv[col_emp_serv]
        .apply(normalizar_codigo)
    )

    # Manter somente delegatarias do universo operacional
    df_serv = df_serv[
        df_serv["company_code"].isin(codigos_dim)
    ].copy()


    # ============================================================
    # CONSIDERAR SOMENTE LINHAS OFICIAIS
    # ============================================================

    df_serv[col_situacao_linha] = (
        df_serv[col_situacao_linha]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df_serv = df_serv[
        df_serv[col_situacao_linha] == "OFICIAL"
    ].copy()


    # ============================================================
    # TRATAR MES DE REFERENCIA
    # ============================================================

    df_serv["reference_month"] = pd.to_numeric(
        df_serv["file_month"],
        errors="coerce"
    )

    df_serv = df_serv[
        df_serv["reference_month"].between(1, 12)
    ].copy()


    # ============================================================
    # NORMALIZAR CODIGO DA LINHA
    # ============================================================
    #
    # Linha deve permanecer como TEXTO.
    #
    # Exemplos possiveis:
    # 001E
    # 1010A
    # 1035/1
    #
    # Nao converter para numero.
    # ============================================================

    df_serv[col_linha] = (
        df_serv[col_linha]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Remover linhas vazias
    df_serv = df_serv[
        df_serv[col_linha] != ""
    ].copy()


    # ============================================================
    # UMA LINHA POR EMPRESA / MES
    # ============================================================
    #
    # Como a base SGTI e cadastral, a mesma linha pode aparecer
    # novamente em cada arquivo mensal.
    #
    # Isso e esperado.
    #
    # Dentro do mesmo mes, entretanto, cada linha deve contar
    # apenas uma vez.
    # ============================================================

    servicos_mes = (
        df_serv[
            [
                "company_code",
                "reference_month",
                col_linha
            ]
        ]
        .drop_duplicates(
            subset=[
                "company_code",
                "reference_month",
                col_linha
            ]
        )
        .copy()
    )


    # ============================================================
    # CONTAR SERVICOS OFICIAIS CADASTRADOS
    # ============================================================

    serv_resumo = (
        servicos_mes
        .groupby(
            [
                "company_code",
                "reference_month"
            ]
        )
        .agg(
            qtd_servicos_empresa=(
                col_linha,
                "nunique"
            )
        )
        .reset_index()
    )

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

        # Frota utilizada no operacional
        "qtd_veiculos_utilizados_total",
        "qtd_veiculos_operados_cadastrados",
        "qtd_veiculos_operados_nao_cadastrados",
        "idade_media_frota_utilizada",
        "qtd_veiculos_com_idade_calculada",

        # Viagens
        "qtd_viagens_realizadas",
        "qtd_viagens_veiculo_nao_cadastrado",

        # Producao
        "producao_quilometrica_km",

        # Servicos operacionais
        "qtd_servicos_declarados",

        # Frota cadastral SGTI
        "qtd_veiculos_cadastrados",
        "idade_media_frota_cadastrada",
        "qtd_veiculos_cadastrados_com_idade",

        # Servicos cadastrais SGTI
        "qtd_servicos_empresa"
    ]
       # ============================================================
    # ORGANIZAR COLUNAS
    # ============================================================

    gold_df = gold_df.reindex(
        columns=cols_ordem
    )


    # ============================================================
    # PREENCHER ZERO SOMENTE NAS MEDIDAS QUANTITATIVAS
    # ============================================================

    colunas_quantidade = [
        "qtd_veiculos_utilizados_total",
        "qtd_veiculos_operados_cadastrados",
        "qtd_veiculos_operados_nao_cadastrados",
        "qtd_veiculos_cadastrados_com_idade",
        "qtd_veiculos_com_idade_calculada",
        "qtd_viagens_realizadas",
        "qtd_viagens_veiculo_nao_cadastrado",
        "producao_quilometrica_km",
        "qtd_servicos_declarados",
        "qtd_veiculos_cadastrados",
        "qtd_servicos_empresa"
    ]

    gold_df[colunas_quantidade] = (
        gold_df[colunas_quantidade]
        .fillna(0)
    )


    # ============================================================
    # TRATAR MES
    # ============================================================

    gold_df["reference_month"] = pd.to_numeric(
        gold_df["reference_month"],
        errors="coerce"
    )

    gold_df = gold_df[
        gold_df["reference_month"].notna()
        &
        (gold_df["reference_month"] > 0)
    ].copy()

    gold_df["reference_month"] = (
        gold_df["reference_month"]
        .astype(int)
    )


    # ============================================================
    # ORDENAR
    # ============================================================

    gold_df.sort_values(
        by=[
            "company_code",
            "reference_month"
        ],
        inplace=True
    )


    # ============================================================
    # SALVAR GOLD
    # ============================================================

    gold_df.to_parquet(
        FILE_GOLD_PARQUET,
        index=False
    )

    gold_df.to_excel(
        FILE_GOLD_EXCEL,
        index=False
    )

    print(
        f"✅ CAMADA GOLD GERADA COM SUCESSO "
        f"({len(gold_df)} registros gerados):"
    )

    print(
        f" -> {FILE_GOLD_PARQUET}"
    )

    print(
        f" -> {FILE_GOLD_EXCEL}"
    )


if __name__ == "__main__":
    gerar_camada_op_sgti_gold()