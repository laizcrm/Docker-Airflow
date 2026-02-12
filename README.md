# Store Sales Pipeline

An Apache Airflow pipeline that validates store transaction data, generates sales analytics charts, loads data into MySQL, and sends email reports using Docker.

## Overview

This project runs a DAG (`validate_csv`) that:

1. **Checks if the CSV file exists** in the expected path
2. **Checks if the file is not empty**
3. **Validates minimum row count** (at least 2 lines)
4. **Displays column headers and date** from the first record
5. **Generates sales charts** (category sales, store sales, discount distribution, profit margins)
6. **Creates MySQL tables** for storing transaction data and aggregated results
7. **Loads cleaned CSV data** into MySQL `store_transactions` table
8. **Aggregates sales data** by category and by store location (runs in parallel)
9. **Sends email report** with the 4 charts attached via Gmail SMTP

![Airflow DAG](images/pipeline.png)

## Project Structure

```
sales_proj/
├── dags/
│   └── stores_dag.py              # Airflow DAG definition
├── script/
│   └── analysis_sales.py          # Sales analysis and chart generation
├── csv_files/
│   └── raw_store_transactions.csv # Source transaction data
├── sql_files/
│   ├── create_tables.sql          # Creates MySQL tables
│   ├── insert_sales_by_category.sql # Aggregates sales by category
│   └── insert_sales_by_store.sql  # Aggregates sales by store
├── plugins/                       # Airflow plugins
├── logs/                          # Airflow logs
├── docker-compose-LocalExecutor.yml  # Docker Compose (MySQL + LocalExecutor)
├── docker-compose.yml                # Docker Compose (Postgres + LocalExecutor)
└── requirements.txt
```

## Dataset

The CSV file (`raw_store_transactions.csv`) contains store transaction records with the following columns:

| Column | Description |
|---|---|
| STORE_ID | Store identifier |
| STORE_LOCATION | Store city/location |
| PRODUCT_CATEGORY | Product category (Electronics, Furniture, Kitchen, etc.) |
| PRODUCT_ID | Product identifier |
| MRP | Maximum Retail Price |
| CP | Cost Price |
| DISCOUNT | Discount applied |
| SP | Selling Price |
| Date | Transaction date |

## Generated Charts

The analysis script produces four charts saved to `csv_files/results/`:

- `01_sales_by_category.png` - Total sales by product category
- `02_sales_by_store.png` - Total sales by store location
- `03_discount_distribution.png` - Distribution of discounts applied
- `04_profit_margin_by_category.png` - Average profit margin (SP - CP) by category

## Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Getting Started

1. Start Docker Desktop

2. Run the containers:
   ```bash
   docker-compose -f docker-compose-LocalExecutor.yml up -d
   ```

3. Access the Airflow UI at [http://localhost:8080](http://localhost:8080)
   - **Username:** `admin`
   - **Password:** `admin`

4. Enable the `validate_csv` DAG and trigger a run

## Email Configuration (Gmail SMTP)

To enable email reports, configure the following in `docker-compose-LocalExecutor.yml` (scheduler service):

```yaml
- AIRFLOW__SMTP__SMTP_USER=your-email@gmail.com
- AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
- AIRFLOW__SMTP__SMTP_MAIL_FROM=your-email@gmail.com
```

Also update the recipient email in `dags/stores_dag.py`.

**How to get a Gmail App Password:**
1. Enable [2-Step Verification](https://myaccount.google.com/signinoptions/two-step-verification) on your Google account
2. Go to [App Passwords](https://myaccount.google.com/apppasswords)
3. Create a new app password and use the generated 16-character code

## Tech Stack

- **Apache Airflow 2.10.4** - Workflow orchestration
- **MySQL 8.0** - Airflow metadata database
- **Python** - DAG and analysis scripts
- **Pandas** - Data processing
- **Matplotlib** - Chart generation
- **Docker** - Containerization
