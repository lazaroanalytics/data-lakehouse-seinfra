# Public Transit Regulatory Data Lakehouse

An end-to-end Data Lakehouse solution designed for public transport regulation, processing monthly operational data, log records, and registry data to support fare calculation, fleet monitoring, and schedule adherence.

> ℹ️ **Note:** This repository is a simplified, English-translated demo version of the work I carried out as an intern in public transport regulation for the State Government of Minas Gerais (SEINFRA-MG), and it is currently under active development.

## 🏗️ Architecture & Features

The project follows the **Medallion Architecture** (Bronze, Silver, Gold) with strict data quality and lineage rules.

## 📐 Data Flow Architecture

```mermaid
flowchart TD
    subgraph Landing[" Landing & Ingestion "]
        A[SEI Operational Spreadsheets] -->|Raw Files| B[op_bronze_layer.py]
        C[SGTI System Metadata] -->|Registry Data| D[sgti_silver_layer.py]
    end

    subgraph Bronze[" Bronze Layer "]
        B -->|Validation Pass| E[(Bronze Parquet Storage)]
        B -->|Validation Fail| F[DLQ: audit_discarded_data.xlsx]
        F -->|Human Correction| G[op_audit_reincorporation.py]
        G -->|Re-ingestion| E
    end

    subgraph Silver[" Silver Layer "]
        D -->|Consolidation & MD5 Cache| H[(SGTI Parquet Datasets)]
        D -->|Matrix Expansion Engine| I[(Expanded Schedule Trips 2026)]
    end

    subgraph Gold[" Future Gold Layer "]
        E -.- K[(Dimensional Models / Star Schema)]
        H -.- K
        I -.- K
        K -.- L[Power BI Dashboards]
    end

    style F fill:#ffe6e6,stroke:#ff4d4d,stroke-width:1px
    style E fill:#e6f2ff,stroke:#3385ff,stroke-width:1px
    style H fill:#e6ffe6,stroke:#33cc33,stroke-width:1px
    style I fill:#e6ffe6,stroke:#33cc33,stroke-width:1px

## 📂 Project Structure

```text
data-lakehouse-seinfra/
├── data/              # Storage partitioned by medallion layers (Bronze / Silver)
├── src/steps/
│   ├── op_bronze_layer.py            # Ingestion, validation & DLQ routing
│   ├── sgti_silver_layer.py          # Consolidation & schedule expansion
│   └── op_audit_reincorporation.py   # Re-ingestion workflow for resolved DLQ items
├── requirements.txt
└── README.md

## 🚀 Execution Workflow

* **1. Operational Ingestion & Bronze (`op_bronze_layer.py`)**: Validates raw operational spreadsheets, routes malformed rows to DLQ (`audit_discarded_data.xlsx`), and saves clean data as Bronze Parquet.
* **2. SGTI Consolidation & Silver (`sgti_silver_layer.py`)**: Consolidates registry metadata using MD5 hashing and expands schedule matrices into projected daily trip instances.
* **3. Audit Reincorporation (`op_audit_reincorporation.py`)**: Re-ingests manually resolved records (`CORRIGIDO_POR_HUMANO = "SIM"`) back into Bronze storage.