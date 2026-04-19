# Ecommerce Sales Batch Pipeline

![Python](https://img.shields.io/badge/Python-3.8-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-green)
![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-lightblue)

A batch data pipeline I built to simulate a real-world ecommerce data engineering workflow. It ingests data from 3 different sources (CSV files, PostgreSQL, REST API), transforms and validates it using Apache Spark, builds a star schema, and loads it into a PostgreSQL data warehouse — all orchestrated and scheduled by Apache Airflow.

I used the **Brazilian Olist E-Commerce dataset** (100k+ real orders) as the data source because it closely mirrors real-world ecommerce data with multiple related entities, realistic dirty data, and meaningful business metrics.

I built this to get hands-on experience with the kind of pipeline you'd actually see in a data engineering role — not just moving data around, but handling multiple sources, incremental loads, data quality checks, partitioned storage, and proper logging.

---

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Table | Rows | Source |
|-------|------|--------|
| fact_sales | 112,650 | Derived |
| dim_customers | 99,441 | CSV |
| dim_products | 32,951 | CSV + DB + API |
| dim_sellers | 3,095 | PostgreSQL |

---

## What it does

Every day Airflow triggers the pipeline which:

1. Pulls orders, customers, products and order items from Olist CSV files
2. Incrementally extracts sellers and inventory from PostgreSQL (only new records since last run using a watermark file)
3. Fetches live product metadata from the DummyJSON REST API
4. Transforms and cleans everything using Spark and writes to staging as Parquet
5. Validates data quality — fails the pipeline if row counts are 0 or key columns have nulls
6. Builds a star schema (fact_sales + 3 dims) and writes partitioned Parquet files
7. Loads everything into a PostgreSQL data warehouse via Spark JDBC

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │        Apache Airflow            │
                    │   (Orchestration & Scheduling)   │
                    └────────────┬────────────────────┘
                                 │ triggers & monitors
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌────────▼──────┐    ┌───────────▼───────┐    ┌─────────▼─────┐
│   CSV Files   │    │    PostgreSQL      │    │   REST API    │
│  (Olist:      │    │  Source DB         │    │  (DummyJSON   │
│  orders,      │    │  (sellers,         │    │   products)   │
│  customers,   │    │   inventory)       │    │               │
│  products,    │    │  incremental load  │    │               │
│  order_items) │    │                    │    │               │
└────────┬──────┘    └───────────┬───────┘    └─────────┬─────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       Raw Layer          │
                    │   /data/raw/             │
                    │  (CSV files as-is)       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Apache Spark         │
                    │   raw_to_staging.py      │
                    │  - dedup                 │
                    │  - null filtering        │
                    │  - type casting          │
                    │  - column mapping        │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Staging Layer        │
                    │   /data/staging/         │
                    │   (Parquet format)       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Data Validation       │
                    │   validate_data.py       │
                    │  - row count checks      │
                    │  - null checks on keys   │
                    │  - fails DAG if invalid  │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Apache Spark         │
                    │ staging_to_processed.py  │
                    │  - star schema build     │
                    │  - fact_sales join       │
                    │  - dim enrichment        │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Processed Layer       │
                    │   /data/processed/       │
                    │  (Parquet, partitioned   │
                    │   by year/month)         │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Apache Spark         │
                    │   load_to_postgres.py    │
                    │   (Spark JDBC write)     │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   PostgreSQL             │
                    │   Data Warehouse         │
                    │   (star schema tables)   │
                    └─────────────────────────┘
```

---

## Airflow DAG Flow

```
                    ┌─────────────────────────────────────┐
                    │        ecommerce_batch_pipeline      │
                    │        schedule: @daily              │
                    │        retries: 1 (after 5 mins)     │
                    └─────────────────────────────────────┘

  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
  │ extract_csv │   │ extract_db  │   │ extract_api │   ← parallel extraction
  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
         └─────────────────┼─────────────────┘
                           │
                  ┌────────▼────────┐
                  │  raw_to_staging │   ← Spark: clean + write Parquet
                  └────────┬────────┘
                           │
                  ┌────────▼────────┐
                  │  validate_data  │   ← fails DAG if quality checks fail
                  └────────┬────────┘
                           │
                  ┌────────▼──────────────┐
                  │  staging_to_processed │   ← Spark: star schema + partitioning
                  └────────┬─────────────┘
                           │
                  ┌────────▼────────┐
                  │ load_to_postgres│   ← Spark JDBC → PostgreSQL DW
                  └─────────────────┘
```

---

## Why these tools?

- **Airflow** — industry standard for orchestrating batch pipelines. DAG makes dependencies and scheduling explicit and visible
- **Spark** — handles 100k+ rows efficiently. Practiced Spark transformations and JDBC writes the way you'd use them on large datasets
- **Parquet** — columnar format, much faster for analytical queries than CSV. Also supports partitioning natively
- **Star schema** — standard for data warehouses. Keeps the fact table lean and dimensions reusable across queries
- **Watermark for incremental load** — simple file-based approach. In production you'd use a metadata table in the DB instead
- **Airflow LocalExecutor** — enough for a single-node setup. CeleryExecutor would be needed for distributed workers

---

## Project Structure

```
ecommerce-sales-batch-pipeline/
├── dags/
│   └── ecommerce_batch_pipeline.py   # Airflow DAG — defines task flow and schedule
├── jobs/
│   ├── extraction/
│   │   ├── extract_csv.py            # Copies CSV files to raw layer
│   │   ├── extract_db.py             # Incremental extraction from PostgreSQL using watermark
│   │   └── extract_api.py            # Fetches product metadata from DummyJSON API
│   ├── transformation/
│   │   ├── raw_to_staging.py         # Spark: cleans raw data → staging Parquet
│   │   └── staging_to_processed.py   # Spark: builds star schema → processed Parquet
│   ├── validation/
│   │   └── validate_data.py          # Row count + null checks, raises exception on failure
│   └── warehouse/
│       └── load_to_postgres.py       # Spark JDBC write to PostgreSQL data warehouse
├── data/
│   ├── raw/                          # Raw ingested files (CSV)
│   ├── staging/                      # Cleaned Parquet files
│   └── processed/                    # Star schema Parquet (fact_sales partitioned by year/month)
├── sql/
│   ├── source_db_init.sql            # Creates and seeds source DB tables (Olist sellers + inventory)
│   └── generate_sql.py               # Generates source_db_init.sql from Olist CSV files
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Data Warehouse Schema (Star Schema)

**fact_sales** *(partitioned by year, month)*
| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING | FK to order |
| product_id | STRING | FK to dim_products |
| customer_id | STRING | FK to dim_customers |
| quantity | INT | Item sequence number |
| order_date | DATE | Date of order |
| unit_price | DOUBLE | Price at time of sale |
| sale_amount | DOUBLE | quantity × unit_price |
| year | INT | Partition column |
| month | INT | Partition column |

**dim_customers** *(99,441 rows)*
| Column | Type |
|--------|------|
| customer_id | STRING |
| customer_name | STRING |
| email | STRING |
| signup_date | DATE |

**dim_products** *(32,951 rows — enriched from CSV + DB + API)*
| Column | Type | Source |
|--------|------|--------|
| product_id | STRING | CSV |
| product_name | STRING | Derived |
| category | STRING | CSV |
| price | DOUBLE | Derived from order_items |
| stock_quantity | INT | PostgreSQL |
| warehouse_location | STRING | PostgreSQL |
| brand | STRING | API |
| rating | DOUBLE | API |
| stock_status | STRING | Derived |

**dim_sellers** *(3,095 rows)*
| Column | Type |
|--------|------|
| seller_id | STRING |
| seller_name | STRING |
| city | STRING |
| state | STRING |

---

## Setup & Run

### Prerequisites
- Docker Desktop running
- Git
- Olist dataset downloaded from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Steps

**1. Clone the repo**
```bash
git clone <repo-url>
cd ecommerce-sales-batch-pipeline
```

**2. Place Olist CSV files in `data/raw/csv/`**
```
orders.csv         ← olist_orders_dataset.csv
customers.csv      ← olist_customers_dataset.csv
products.csv       ← olist_products_dataset.csv
order_items.csv    ← olist_order_items_dataset.csv
```

**3. Generate source DB SQL from Olist sellers data**
```bash
python sql/generate_sql.py
```

**4. Start all containers**
```bash
docker-compose up -d
```

**5. Wait ~2 minutes for Spark to finish installing dependencies**
```bash
docker-compose logs spark
```

**6. Initialize source database tables**

Windows (PowerShell):
```powershell
Get-Content sql/source_db_init.sql | docker exec -i ecommerce_postgres psql -U admin -d ecommerce_dw
```

Mac/Linux:
```bash
cat sql/source_db_init.sql | docker exec -i ecommerce_postgres psql -U admin -d ecommerce_dw
```

**7. Create Airflow admin user**
```bash
docker exec -it ecommerce_airflow_webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

**8. Open Airflow UI and trigger the DAG**
```
http://localhost:8080
Username: admin
Password: admin
```

Go to `ecommerce_batch_pipeline` → click ▶ to trigger manually.

---

## What I'd improve with more time

- Replace file-based watermark with a proper metadata table in PostgreSQL — more reliable and queryable
- Add Metabase or Superset container for visualizing the warehouse data directly
- Move DB credentials to Airflow Connections instead of hardcoding in scripts
- Add schema validation (check column types, not just nulls)
- Use `append` mode instead of `overwrite` for true incremental loads into the warehouse
- Add a Great Expectations integration for more robust data quality checks
