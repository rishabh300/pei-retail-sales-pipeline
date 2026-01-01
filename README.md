# PEI Retail Sales Pipeline

A comprehensive data processing system designed and implemented using Databricks for an e-commerce platform. This project demonstrates end-to-end data pipeline architecture following medallion architecture principles for retail sales data processing.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Pipeline Components](#pipeline-components)
- [Notebooks](#notebooks)
- [Technologies Used](#technologies-used)
- [Getting Started](#getting-started)
- [Contributing](#contributing)

## 🎯 Overview

This project implements a robust data processing pipeline for retail sales data using Databricks. The pipeline follows a three-layered architecture (Bronze → Silver → Gold) to transform raw e-commerce data into business-ready insights.

### Key Features

- **Multi-layer data processing** following medallion architecture
- **Data ingestion** from various source systems
- **Data enrichment** with quality checks and transformations
- **Business layer** with analytics-ready datasets
- **Scalable architecture** built on Databricks
- **Automated testing** for data quality assurance

## 🏗️ Architecture

The pipeline follows the **medallion architecture** pattern with three distinct layers:

**Bronze Layer (Raw Data)** → **Silver Layer (Cleaned)** → **Gold Layer (Business)**

| Layer | Purpose | Description |
|-------|---------|-------------|
| 🥉 **Bronze** | Raw Ingestion | Raw data ingestion from source systems with minimal transformation |
| 🥈 **Silver** | Data Quality | Data cleansing, validation, enrichment, and quality checks |
| 🥇 **Gold** | Business Ready | Business-level aggregations, KPIs, and analytics-ready datasets |

## 📁 Project Structure

```
pei-retail-sales-pipeline/
│
├── init/                           # Setup and initialization scripts
│   └── setup_script.sql           # Database and table initialization
│
├── src/                           # Source code
│   ├── notebooks/                # Databricks notebooks
│   │   ├── 01_ingestion.ipynb   # Bronze layer - Data ingestion
│   │   ├── 02_enrichment.ipynb  # Silver layer - Data enrichment
│   │   └── 03_business.ipynb    # Gold layer - Business metrics
│   │
│   └── utils/                    # Utility functions and helpers
│
├── tests/                         # Test suite
│   └── unit_tests/               # Unit tests for pipeline components
│
├── requirements.txt               # Python dependencies
├── .gitignore                    # Git ignore configuration
└── README.md                     # Project documentation
```

### Key Directories

- **`init/`** - Contains setup scripts for database and table initialization
- **`src/notebooks/`** - Contains 3 main pipeline notebooks (ingestion, enrichment, business)
- **`tests/`** - Unit tests for data quality and pipeline validation

## 🚀 Setup Instructions

### Prerequisites

- Databricks workspace (Community Edition or Enterprise)
- Python 3.8+
- Access to source data systems
- Azure/AWS/GCP account (depending on your cloud provider)

### Installation Steps

#### 1. Clone the Repository
```bash
git clone https://github.com/rishabh300/pei-retail-sales-pipeline.git
cd pei-retail-sales-pipeline
```

#### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

#### 3. Configure Databricks
- Import notebooks from `src/notebooks/` to your Databricks workspace
- Configure cluster settings (recommended: DBR 11.3 LTS or higher)
- Set up compute resources

#### 4. Initialize the Database
Navigate to the `init/` directory and execute the setup script:

```sql
-- Run in Databricks SQL or notebook
%run ./init/setup_script
```

#### 5. Configure Data Sources
- Update connection strings and credentials in notebooks
- Configure mount points for data storage (Azure/AWS/GCP)
- Set up appropriate access permissions

## 🔄 Pipeline Components

### 1. Data Ingestion (Bronze Layer)
Located in: `src/notebooks/01_ingestion.ipynb`

- Connects to source systems
- Extracts raw data in its original format
- Stores data with minimal transformation
- Maintains data lineage and audit trail

### 2. Data Enrichment (Silver Layer)
Located in: `src/notebooks/02_enrichment.ipynb`

- Cleanses and validates data
- Handles missing values and duplicates
- Standardizes data formats
- Performs data quality checks
- Enriches data with additional attributes

### 3. Business Layer (Gold Layer)
Located in: `src/notebooks/03_business.ipynb`

- Creates aggregated views and KPIs
- Builds dimensional models
- Generates analytics-ready datasets
- Prepares data for reporting and dashboards

## 📓 Notebooks

All notebooks are located in `src/notebooks/` directory:

| Notebook | Layer | Purpose | Key Operations |
|----------|-------|---------|----------------|
| **01_ingestion.ipynb** | 🥉 Bronze | Raw data ingestion | - Connect to data sources<br>- Read data (batch/streaming)<br>- Write to Bronze Delta tables |
| **02_enrichment.ipynb** | 🥈 Silver | Data transformation & quality | - Data validation<br>- Schema enforcement<br>- Deduplication<br>- Business rules |
| **03_business.ipynb** | 🥇 Gold | Business metrics & analytics | - KPI calculations<br>- Dimensional modeling<br>- Aggregate tables<br>- Report-ready views |

### Notebook Details

### 01_ingestion.ipynb
- **Purpose**: Ingests raw sales data from various sources
- **Output**: Bronze tables with raw data
- **Key Operations**:
  - Connect to data sources
  - Read data in batch/streaming mode
  - Write to Bronze layer Delta tables

### 02_enrichment.ipynb
- **Purpose**: Transforms and cleanses data
- **Output**: Silver tables with curated data
- **Key Operations**:
  - Data validation and quality checks
  - Schema enforcement
  - Deduplication
  - Data type conversions
  - Business rule applications

### 03_business.ipynb
- **Purpose**: Creates business-level metrics and aggregations
- **Output**: Gold tables ready for analytics
- **Key Operations**:
  - KPI calculations
  - Dimensional modeling
  - Aggregate tables creation
  - Report-ready views

## 🛠️ Technologies Used

- **Apache Spark**: Distributed data processing
- **Databricks**: Unified analytics platform
- **Delta Lake**: ACID transactions and time travel
- **Python**: Primary programming language
- **PySpark**: Spark Python API
- **SQL**: Data manipulation and querying

## 🏃 Getting Started

### Running the Pipeline

1. **Execute Ingestion Notebook**
   ```python
   # In Databricks workspace
   %run ./src/notebooks/01_ingestion
   ```

2. **Execute Enrichment Notebook**
   ```python
   %run ./src/notebooks/02_enrichment
   ```

3. **Execute Business Layer Notebook**
   ```python
   %run ./src/notebooks/03_business
   ```

### Scheduling

Configure Databricks Jobs to schedule the pipeline:
- Set up job clusters
- Configure dependencies between notebooks
- Set appropriate retry policies
- Configure alerts and notifications

## 🧪 Testing

Run tests using pytest:

```bash
cd tests
pytest unit_tests/ -v
```

## 📊 Data Flow

The pipeline processes data through distinct stages:

1. **Source Systems** - E-commerce transaction data, customer data, product catalogs
2. **Bronze Layer** - Raw data storage in Delta Lake format
3. **Silver Layer** - Cleaned and validated data with quality checks
4. **Gold Layer** - Aggregated business metrics and KPIs
5. **Analytics/BI** - Consumption by reporting tools and dashboards

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is an assessment/demonstration project for educational purposes.

## 👥 Authors

- **Rishabh** - [rishabh300](https://github.com/rishabh300)

## 🙏 Acknowledgments

- Databricks documentation and best practices
- Delta Lake community
- Contributors and reviewers

## 📧 Contact

For any queries or suggestions, please open an issue in the GitHub repository.

---

**Note**: This is an assessment project demonstrating data engineering capabilities using Databricks and modern data stack technologies.
