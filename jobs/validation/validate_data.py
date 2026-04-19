import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("ValidateData").getOrCreate()

STAGING_PATH = "/app/data/staging"

checks = [
    ("customers",        "customer_id"),
    ("products",         "product_id"),
    ("orders",           "order_id"),
    ("order_items",      "order_id"),
    ("sellers",          "seller_id"),
    ("inventory",        "product_id"),
    ("product_metadata", "product_id"),
]

errors = []

for table, key_col in checks:
    df = spark.read.parquet(f"{STAGING_PATH}/{table}")
    row_count = df.count()
    null_count = df.filter(col(key_col).isNull()).count()

    if row_count == 0:
        errors.append(f"FAILED: {table} has 0 rows")
    else:
        logger.info(f"OK: {table} has {row_count} rows")

    if null_count > 0:
        errors.append(f"FAILED: {table}.{key_col} has {null_count} null values")
    else:
        logger.info(f"OK: {table}.{key_col} has no nulls")

if errors:
    for e in errors:
        logger.error(e)
    spark.stop()
    raise ValueError(f"Data validation failed with {len(errors)} error(s). Check logs.")

logger.info("All data validation checks passed successfully!")
spark.stop()
