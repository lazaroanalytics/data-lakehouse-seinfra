# Public Transit Regulatory Data Lakehouse (`data-lakehouse-seinfra`)

A demo repository showcasing an end-to-end Data Lakehouse solution designed for the public transportation regulatory domain (inspired by operations at the **Diretoria de Regulação do Transporte Coletivo do Estado de Minas Gerais**).

This platform automates the ingestion, auditing, consolidation, and business modeling of monthly operational trip logs and system registration metadata. It bridges declared operational data with cadastral records to support **tariff calculation, fleet utilization monitoring, and schedule compliance reporting**.

---

# Business Context & Motivation

Public transit regulation requires continuous monitoring of operational data submitted by concessionaire bus companies, cross-referencing their declared performance against official state registrations. 

[ Data Streams ]
  ├── Realized Trips Data ────► Operational logs submitted via SEI System
  ├── SGTI System Data ───────► Master Registration (Routes, Schedules, Vehicles, Contracts)
  └── BMI Reports ────────────► Monthly Informative Bulletins (Boletim Mensal Informativo)

# Medallion Architecture

The project enforces strict separation of concerns across a three-tier Medallion Architecture, guaranteeing data quality, traceability, and high-performance querying for Power BI consumption.

┌───────────────────────┐
│      RAW / NEW RECEIPTS       │
└───────────────────────┘
                │
    [ Validation & DLQ Audit ]
                │
                ▼
┌───────────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Trusted Operational Data)                                                          │
│ - Validated, schema-conforming records with standardized data types                              │
│ - Raw historical preservation with strict versioning                                             │
│ - Rejection of invalid records to Dead-Letter Queue (DLQ / audit_discarded_data.xlsx)            │
└────────────────────────────────────────────────┬──────────────────────┘
                │
                │
                ▼
┌───────────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Consolidated & Enriched Data)                                                      │
│ - Cleaned, deduplicated, and unified across monthly files (Apache Parquet format)                │
│ - Schedule Matrix Expansion: static timetables converted into daily row-level projections        │
│ - Metadata hashing (MD5) to prevent redundant state reprocessing                                 │
└───────────────────────────────────────────────────────────────────────┘
                │
                │
                ▼
┌───────────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Analytics & Business Model)                                                          │
│ - Star Schema dimensional modeling (Fact Trips vs. Dim Schedule vs. Dim Vehicles)                │
│ - Aggregated regulatory metrics (Schedule Fulfillment Rate, Fleet Efficiency)                    │
│ - Optimized structures ready for direct consumption in Power BI Dashboards                       │
└───────────────────────────────────────────────────────────────────────┘#

# Key Features

-Data Quality & Validation Framework: Syntactic and business validation (Regex pattern matching for vehicle license plates and service identifiers, date range verification, and strict domain enforcement).

-Automated Rectification Lifecycle: Automatically detects re-submitted data for previously ingested operating periods, storing raw copies in dedicated retificadas archives and flagging row lineage (is_rectification=True).

-Schedule Matrix Expansion Engine: Converts static schedule matrices (operating days, month flags, holiday rules) into granular, date-specific trip instances for the entire target operational year.

-Incremental Processing via MD5 Hashing: Implements file signature caching to prevent redundant consolidation and re-processing of unchanged SGTI datasets.

-Dead-Letter Queue (DLQ) & Lineage Audit: Routes invalid records to audit_discarded_data.xlsx with detailed rejection codes, enabling manual resolution without data loss.

# Repository Structure

data-lakehouse-seinfra/
├── data/
│   ├── ingestion/
│   │   ├── operational/2026/
│   │   │   ├── new-receipts/         # Landing directory for operational receipts
│   │   │   └── raw/                  # Immutable raw archive (partitioned by company)
│   │   └── sgti/2026/                # Landing directory for SGTI files
│   ├── bronze/
│   │   ├── operational/2026/         # Validated operational data & DLQ audit ledger
│   │   └── sgti/2026/                # Archived SGTI source files
│   └── silver/
│       └── sgti/2026/                # Consolidated Parquet datasets & log ledgers
├── src/
│   └── steps/
│       ├── op_bronze_layer.py        # Ingestion, validation & DLQ routing
│       ├── sgti_silver_layer.py      # Consolidation, schedule expansion & caching
│       └── op_audit_reincorporation.py # Re-ingestion workflow for resolved DLQ items
├── requirements.txt
└── README.md

# Pipeline Execution Steps

1. Operational Ingestion & Bronze Layer (op_bronze_layer.py)
Processes raw operational spreadsheets, validates columns (service_id, date, schedule_time, path, type, vehicle), partitions raw data by company, and segregates malformed rows into the audit queue.

2. SGTI Consolidation & Silver Layer (sgti_silver_layer.py)
Consolidates system metadata across schedule, service, and vehicles categories into Parquet files. Executes the calendar expansion engine on schedule matrices to project daily operating trips for calendar year 2026.

3. Audit Reincorporation Pipeline (op_audit_reincorporation.py)
Reads manually corrected records flagged with CORRIGIDO_POR_HUMANO = "SIM" in the audit ledger, updates the primary Bronze Parquet dataset, and appends internal tracking copies to RAW.

# Future Roadmap

[ ] Operational Trips Silver Layer Processing (op_silver_layer.py): Consolidating validated operational receipts from Bronze into unified Parquet storage, performing deduplication and timestamp normalization across all operators.
[ ] BMI Report Pipeline: Ingesting and standardizing Monthly Informative Bulletins (Boletim Mensal Informativo) to track financial and operational indicators (fuel, mileage, passenger volume).
[ ] Gold Layer Dimensional Modeling: Transforming Silver datasets into a Star Schema models and PowerBI reports
[ ] Building a centralized internal fleet dataset based on SGTI metadata, incorporating customized vehicle categorizations tailored for departmental monitoring and tariff calculation.
[ ] Automated Ingestion Platform: Transitioning manual file landing to direct web-portal API ingestion for bus operators.
[ ] Automated Non-Compliance Feedback Loop: Automated ticket generation notifying bus companies of rejected records upon submission.