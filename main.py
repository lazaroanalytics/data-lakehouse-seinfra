import sys
from pathlib import Path
from typing import Dict
import pandas as pd

# ==============================================
# 1. CONFIGURAÇÃO DE CAMINHOS NA RAIZ
# ==============================================
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src" / "steps"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importação dos passos da pipeline
from op_bronze_layer import execute_operational_bronze_pipeline
from bmi_bronze_layer import execute_bmi_bronze_pipeline
from sgti_silver_layer import consolidate_sgti_datasets
from op_silver_layer import processar_op_silver
from op_sgti_gold_layer import gerar_camada_op_sgti_gold
from audited_reincorporation import execute_reincorporation_pipeline

# ==============================================
# CONFIGURAÇÃO DE VALIDAÇÃO DE INGESTÃO (ATUALIZADA)
# ==============================================
DIR_INGESTION_OP = PROJECT_ROOT / "data" / "ingestion" / "operational" / "2026" / "new-receipts"


def validar_arquivos_entrada() -> Dict[str, str]:
    """
    Valida as planilhas recebidas na pasta de ingestão operacional.
    Retorna um dicionário {nome_arquivo: motivo_erro} para arquivos inválidos.
    """
    print("🔍 [0/6] Validando arquivos na pasta de ingestão operacional...")
    erros = {}

    if not DIR_INGESTION_OP.exists():
        print(f"⚠️ Diretório de ingestão não encontrado: {DIR_INGESTION_OP}")
        return erros

    # Lista arquivos ignorando temporários do Excel (~$)
    arquivos = [
        f for f in list(DIR_INGESTION_OP.glob("*.xlsx")) + list(DIR_INGESTION_OP.glob("*.xls"))
        if not f.name.startswith("~$")
    ]

    if not arquivos:
        print("⚠️ Nenhum arquivo novo encontrado para processamento.")
        return erros

    for caminho in arquivos:
        nome_arquivo = caminho.name
        try:
            with pd.ExcelFile(caminho) as excel:
                if len(excel.sheet_names) == 0:
                    erros[nome_arquivo] = "Arquivo Excel vazio ou sem abas validas."
        except Exception as e:
            erros[nome_arquivo] = f"Erro ao abrir arquivo Excel: {str(e)}"

    if erros:
        print(f"⚠️ {len(erros)} falha(s) de integridade detectada(s) na entrada.")
    else:
        print(f"✅ {len(arquivos)} arquivo(s) de entrada checado(s) com sucesso!")

    return erros


def exibir_relatorio_final(nao_processados: Dict[str, str]):
    """
    Exibe uma tabela consolidada de todos os arquivos que não foram processados
    ou que apresentaram falhas de validação durante a execução da pipeline.
    """
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL DE ARQUIVOS NÃO PROCESSADOS / COM ERRO")
    print("=" * 80)

    if not nao_processados:
        print("✅ Todos os arquivos do fluxo foram validados e processados com sucesso!")
    else:
        print(f"⚠️  Total de arquivos não processados ou rejeitados: {len(nao_processados)}\n")
        print(f"{'ARQUIVO':<40} | {'MOTIVO DO ERRO / REJEIÇÃO'}")
        print("-" * 80)
        for arquivo, motivo in nao_processados.items():
            print(f"{arquivo:<40} | {motivo}")
        print("-" * 80)
        print("\n💡 Ação necessária: Corrija os arquivos apontados antes da próxima execução.")

    print("=" * 80)


def run_full_pipeline():
    print("=" * 80)
    print("🚀 INICIANDO EXECUÇÃO COMPLETA DA PIPELINE DE DADOS (LAKEHOUSE)")
    print("=" * 80)

    # Dicionário unificado de falhas
    arquivos_nao_processados: Dict[str, str] = {}

    # 0. Pré-validação
    erros_pre_validacao = validar_arquivos_entrada()
    arquivos_nao_processados.update(erros_pre_validacao)

    # 1. Bronze Operacional (coleta arquivos com taxa de descarte alta ou erro de nome)
    print("\n▶️ [1/6] Executando Camada Bronze Operacional...")
    erros_bronze = execute_operational_bronze_pipeline()
    if isinstance(erros_bronze, dict):
        arquivos_nao_processados.update(erros_bronze)

    # 2. Reincorporação de Auditoria
    print("\n▶️ [2/6] Verificando Reincorporação de Auditoria...")
    execute_reincorporation_pipeline()

    # 3. Consolidação Silver SGTI
    print("\n▶️ [3/6] Executando Consolidação Silver SGTI...")
    consolidate_sgti_datasets()

    # 4. Processamento Silver Operacional (com Cache)
    print("\n▶️ [4/6] Executando Saneamento e Auditoria Silver Operacional...")
    processar_op_silver()

    # 5. Consolidação Gold Operacional x SGTI
    print("\n▶️ [5/6] Gerando Camada Gold de Consistência Mensal...")
    gerar_camada_op_sgti_gold()

    # 6. Exibição do Relatório Tabular ao Final
    exibir_relatorio_final(arquivos_nao_processados)

    print("\n" + "=" * 80)
    print("🏁 PIPELINE COMPLETA EXECUTADA!")
    print("=" * 80)


if __name__ == "__main__":
    run_full_pipeline()