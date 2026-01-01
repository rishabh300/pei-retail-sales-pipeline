# pei-retail-sales-pipeline
This is an assessment project to design and implement a data processing system using Databricks for an e-commerce platform.

🏗️ Architecture
The pipeline follows the medallion architecture pattern:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Bronze    │ ──> │   Silver    │ ──> │    Gold     │
│  (Raw Data) │     │ (Cleaned)   │     │ (Business)  │
└─────────────┘     └─────────────┘     └─────────────┘

Bronze Layer: Raw data ingestion from source systems
Silver Layer: Data cleansing, validation, and enrichment
Gold Layer: Business-level aggregations and analytics


📁 Project Structure
pei-retail-sales-pipeline/
│
├── init/                          # Setup and initialization scripts
│   └── setup_script.sql          # Database and table initialization
│
├── src/                          # Source code
│   ├── notebooks/               # Databricks notebooks
│   │   ├── 01_ingestion.ipynb  # Data ingestion notebook (Bronze layer)
│   │   ├── 02_enrichment.ipynb # Data enrichment notebook (Silver layer)
│   │   └── 03_business.ipynb   # Business layer notebook (Gold layer)
│   │
│   └── utils/                   # Utility functions and helpers
│
├── tests/                        # Test suite
│   └── unit_tests/              # Unit tests for pipeline components
│
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore file
└── README.md                    # This file


Prerequisites

Databricks workspace (Community Edition or Enterprise)
Python 3.11.9
Access to source data systems
Azure Account
