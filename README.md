# Customer Analytics Pipeline | PySpark, Python & SQL Server

An end-to-end data engineering project that processes and analyzes over **1 million retail transaction records** using Apache PySpark, Python, Parquet, and Microsoft SQL Server.

The project demonstrates large-scale data processing, data cleaning, data quality validation, partitioning, performance analysis, SQL analytics, and incremental data loading.

## Project Overview

The project uses the Online Retail II dataset to build a structured data processing workflow.

Raw Excel data is combined, transformed, cleaned, validated, stored in Parquet format, and loaded into SQL Server for further analysis.

### Key Results

| Metric | Result |
|---|---:|
| Original Records | 1,067,371 |
| Cleaned Records | 1,033,036 |
| Exact Duplicate Records Removed | 34,335 |
| SQL Server Loaded Records | 1,033,036 |
| Parquet Output | 4 Partitions |

## Technology Stack

- **Python** — Pipeline development and automation
- **Apache PySpark** — Distributed data processing
- **PyArrow** — Parquet dataset reading and batch processing
- **Pandas** — Supporting data operations
- **Microsoft SQL Server** — Data warehouse and analytics
- **PyODBC** — Python-to-SQL Server connectivity
- **SQL** — Data validation and business analysis
- **Git & GitHub** — Version control

## Project Architecture

```text
Online Retail II Dataset
          |
          v
     Data Extraction
          |
          v
      Bronze Layer
       Raw CSV
          |
          v
   PySpark Processing
          |
          v
    Data Cleaning
          |
          v
   Data Quality Checks
          |
          v
      Silver Layer
    Cleaned Parquet
          |
          v
    SQL Server Loading
          |
          v
   CustomerInsightsDW
          |
          v
   SQL Analytics & Validation
```

## Data Processing Workflow

### 1. Data Extraction
- Combined data from both Online Retail II Excel sheets.
- Prepared the raw transaction dataset.
- Stored the extracted data in the Bronze layer.

### 2. Data Cleaning and Transformation
- Removed exact duplicate records.
- Trimmed and standardized product descriptions.
- Handled missing and blank descriptions.
- Preserved missing customer IDs and classified their status.
- Created transaction classification flags.

### 3. Transaction Classification

Transactions were classified into categories:

- REGULAR_TRANSACTION
- CANCELLED_INVOICE
- RETURN_OR_REFUND
- ZERO_PRICE_TRANSACTION
- NEGATIVE_PRICE_TRANSACTION
- ACCOUNTING_ADJUSTMENT

### 4. Data Quality Validation

The pipeline checks important data quality conditions, including:

- Duplicate records
- Missing customer IDs
- Missing descriptions
- Zero-price transactions
- Negative-price transactions
- Returns and cancelled invoices
- Accounting adjustments

### 5. Parquet Storage

Cleaned data is stored in the Silver layer using Parquet format with Snappy compression.

The output contains four Parquet data partitions.

### 6. SQL Server Integration

Processed data is loaded into Microsoft SQL Server.

**Database:** `CustomerInsightsDW`  
**Schema:** `staging`  
**Table:** `OnlineRetail`

The SQL Server table contains 1,033,036 records.

Python connects to SQL Server using PyODBC. The project includes an initial loader and an incremental loading script.

### 7. SQL Analytics

SQL queries are used for:

- Total transaction analysis
- Customer sales analysis
- Transaction classification
- Cancelled invoice analysis
- Returns and refunds
- Missing customer ID analysis
- Country-level sales analysis
- Customer ranking
- Data quality validation

## Data Quality Results

| Transaction Type | Total Records | Cancelled Invoices | Returns | Zero Price | Negative Price |
|---|---:|---:|---:|---:|---:|
| REGULAR_TRANSACTION | 1,007,913 | 0 | 0 | 0 | 0 |
| CANCELLED_INVOICE | 19,104 | 19,104 | 19,103 | 0 | 0 |
| RETURN_OR_REFUND | 3,393 | 0 | 3,393 | 3,393 | 0 |
| ZERO_PRICE_TRANSACTION | 2,621 | 0 | 0 | 2,621 | 0 |
| ACCOUNTING_ADJUSTMENT | 5 | 0 | 0 | 0 | 5 |

**Important:** These flags can overlap. For example, a return may also have a zero price. Therefore, these categories should not be treated as mutually exclusive totals.

## Performance Analysis

The project includes Spark performance and partition analysis.

Experiments cover:

- Comparing 4 and 8 partitions
- Using `repartition()`
- Configuring `spark.sql.shuffle.partitions`
- Testing `cache()`
- Measuring `groupBy()` operations
- Examining execution plans using `explain("formatted")`
- Understanding Spark operations such as `Exchange`, `HashAggregate`, and `Scan`

## Repository Structure

```text
customer-analytics-pyspark-pipeline/
│
├── processing/
│   ├── customer_business_analysis.py
│   ├── customer_data_analysis.py
│   ├── customer_data_cleaning.py
│   ├── customer_data_processing.py
│   ├── customer_partition_analysis.py
│   ├── customer_performance_test.py
│   ├── customer_spark_sql.py
│   ├── custome_pyspark_data_loader.py
│   ├── row_validation.py
│   └── tts.py
│
├── .gitignore
├── customer_analytics.sql
├── first_spark.py
├── incremetal_sql_loader.py
├── pipelinesss.py
├── test_spark.py
└── README.md
```

## Setup and Installation

### 1. Clone the Repository

```bash
git clone https://github.com/danisajjo1231-stack/customer-analytics-pyspark-pipeline.git
```

### 2. Navigate to the Project

```bash
cd customer-analytics-pyspark-pipeline
```

### 3. Create a Virtual Environment

```bash
python -m venv .venv
```

Activate it on Windows:

```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install Dependencies

Install the required libraries:

```bash
pip install pyspark pandas pyarrow pyodbc
```

### 5. Configure SQL Server

Update the SQL Server connection settings in the relevant Python loader scripts according to your local environment.

Create or configure the `CustomerInsightsDW` database and the required staging table before running the loaders.

### 6. Run the Pipeline

Run the relevant extraction, cleaning, processing, and loading scripts according to their dependencies.

**Note:** The original dataset is not included in this repository. You must obtain the Online Retail II dataset separately and configure the project paths before execution.

## Learning Outcomes

Through this project, I gained practical experience with:

- Building ETL pipelines using PySpark
- Processing more than one million retail records
- Data cleaning and validation
- Working with Bronze and Silver data layers
- Parquet storage and partitioning
- Spark execution plans and performance analysis
- Python and SQL Server integration
- Batch and incremental data loading
- SQL-based business analysis
- Git and GitHub version control

## Author

**Danish Muhammad**

Aspiring Data Engineer | Data Analyst

GitHub: [Danish311](https://github.com/Danish311)

---

*This project was developed as part of my hands-on learning journey in Data Engineering and large-scale data processing.*