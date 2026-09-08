import os
import glob
import pandas as pd

# ==============================================================================
# CONFIGURAÇÃO DO CAMINHO DA PASTA
# ==============================================================================
PASTA = r"C:\Users\x20652550\Desktop\trabalho dia 08.09\data-lakehouse-seinfra-1.0\data\silver\operational\2026"
ARQUIVO_SAIDA = os.path.join(PASTA, "relatorio_estrutura_bancos.xlsx")
# ==============================================================================

# Busca todos os arquivos .parquet na pasta
arquivos_parquet = glob.glob(os.path.join(PASTA, "*.parquet"))

if not arquivos_parquet:
    print(f"❌ Nenhum arquivo .parquet foi encontrado na pasta:\n   {PASTA}")
else:
    lista_detalhes = []
    lista_resumo = []

    print(f"🔍 Encontrados {len(arquivos_parquet)} arquivo(s) Parquet. Lendo metadados...\n")

    for caminho in arquivos_parquet:
        nome_arquivo = os.path.basename(caminho)
        try:
            # Lê o arquivo Parquet
            df_temp = pd.read_parquet(caminho)
            total_linhas = len(df_temp)
            total_colunas = len(df_temp.columns)

            # Aba 1: Resumo do Arquivo
            lista_resumo.append({
                'ARQUIVO': nome_arquivo,
                'TOTAL_COLUNAS': total_colunas,
                'TOTAL_REGISTROS': total_linhas
            })

            # Aba 2: Mapeamento Detalhado por Coluna e Ordem
            for ordem, coluna in enumerate(df_temp.columns, start=1):
                lista_detalhes.append({
                    'ARQUIVO': nome_arquivo,
                    'ORDEM': ordem,  # Posição exata da coluna no banco
                    'COLUNA': coluna,
                    'TIPO_DADO': str(df_temp[coluna].dtype)
                })

            print(f"  ✔ Mapeado: {nome_arquivo:<45} | {total_colunas:>2} colunas | {total_linhas:>8,} linhas")

        except Exception as e:
            print(f"  ✖ Erro ao ler o arquivo {nome_arquivo}: {e}")

    # Converte listas em DataFrames
    df_resumo = pd.DataFrame(lista_resumo)
    df_detalhes = pd.DataFrame(lista_detalhes)

    # Exporta para um único arquivo Excel (.xlsx) com 2 abas organizadas
    with pd.ExcelWriter(ARQUIVO_SAIDA) as writer:
        df_resumo.to_excel(writer, sheet_name='Resumo_Geral', index=False)
        df_detalhes.to_excel(writer, sheet_name='Estrutura_Colunas', index=False)

    print("\n" + "=" * 65)
    print("✅ RELATÓRIO EXCEL GERADO COM SUCESSO!")
    print(f"📁 Salvo em: {ARQUIVO_SAIDA}")
    print("=" * 65)