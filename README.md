# PEI Retail Sales Pipeline

This is an assessment project to design and implement a data processing system using Databricks for an e-commerce platform.

## Architecture

The pipeline follows the **medallion architecture** pattern:

- **Bronze Layer**: Raw data ingestion from source systems
- **Silver Layer**: Data cleansing, validation, and enrichment  
- **Gold Layer**: Business-level aggregations and analytics

## Project Structure

```
pei-retail-sales-pipeline/
├── init/                       Setup and initialization scripts
│   └── setup_script.sql       Database and table initialization
│
├── src/                       Source code
│   ├── notebooks/            Databricks notebooks
│   │   ├── ingestion.ipynb       Bronze layer - Data ingestion
│   │   ├── enrichment.ipynb      Silver layer - Data enrichment
│   │   └── business.ipynb        Gold layer - Business metrics
│   └── utils/                Utility functions and helpers
│
├── tests/                     Test suite
│   └── unit_tests/           Unit tests for pipeline components
│
├── requirements.txt           Python dependencies
├── .gitignore                Git ignore file
└── README.md                 This file
```

## Key Directories

- **init/** - Contains setup scripts for database and table initialization
- **src/notebooks/** - Contains 3 main pipeline notebooks (ingestion, enrichment, business)
- **tests/** - Unit tests for data quality and pipeline validation

## Prerequisites

- Databricks workspace (Community Edition or Enterprise)
- Python 3.11.9
- Access to source data systems
- Azure Account



## Notebooks Overview

| Notebook | Layer | Purpose |
|----------|-------|---------|
| ingestion.ipynb | Bronze | Raw data ingestion from sources |
| enrichment.ipynb | Silver | Data cleansing and enrichment |
| business.ipynb | Gold | Business metrics and analytics |


## Running the Pipeline

Execute the notebooks in sequence:

1. Run the Ingestion notebook
2. Run the Enrichment notebook
3. Run the Business Layer notebook



## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

- Rishabh - [rishabh300](https://github.com/rishabh300)

## License

This project is an assessment/demonstration project for educational purposes.
