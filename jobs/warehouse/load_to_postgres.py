import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("LoadToPostgres").getOrCreate()

PROCESSED_PATH = "/app/data/processed"

DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
DB_NAME = os.getenv("POSTGRES_DB", "ecommerce_dw")

jdbc_url = f"jdbc:postgresql://ecommerce_postgres:5432/{DB_NAME}"
connection_properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

tables = ["dim_customers", "dim_products", "dim_sellers", "fact_sales"]

for table in tables:
    df = spark.read.parquet(f"{PROCESSED_PATH}/{table}")
    df.write.jdbc(url=jdbc_url, table=table, mode="overwrite", properties=connection_properties)
    logger.info(f"{table} loaded: {df.count()} rows")

logger.info("Processed data loaded to PostgreSQL successfully!")
spark.stop()
