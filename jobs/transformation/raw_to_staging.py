import os
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, concat, substring

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("RawToStagingPipeline")
    .getOrCreate()
)

RAW_CSV_PATH = "/app/data/raw/csv"
RAW_DB_PATH = "/app/data/raw/db"
RAW_API_PATH = "/app/data/raw/api"
STAGING_PATH = "/app/data/staging"


def delete_folder_if_exists(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)


customers = spark.read.csv(f"{RAW_CSV_PATH}/customers.csv", header=True, inferSchema=True)
customers = customers.dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull())
customers = customers.select(
    col("customer_id"),
    col("customer_unique_id").alias("customer_name"),
    concat(col("customer_unique_id"), lit("@email.com")).alias("email"),
    lit("2017-01-01").cast("date").alias("signup_date")
)
delete_folder_if_exists(f"{STAGING_PATH}/customers")
customers.write.mode("overwrite").parquet(f"{STAGING_PATH}/customers")
logger.info(f"Customers staged: {customers.count()} rows")


products_raw = spark.read.csv(f"{RAW_CSV_PATH}/products.csv", header=True, inferSchema=True)
order_items_raw = spark.read.csv(f"{RAW_CSV_PATH}/order_items.csv", header=True, inferSchema=True)

# Calculate average price per product from order_items
avg_price = order_items_raw.groupBy("product_id").avg("price").withColumnRenamed("avg(price)", "price")

products = products_raw.join(avg_price, "product_id", "left")
products = products.dropDuplicates(["product_id"]).filter(col("product_id").isNotNull())
products = products.select(
    col("product_id"),
    concat(col("product_category_name"), lit("_"), substring(col("product_id"), 1, 8)).alias("product_name"),
    col("product_category_name").alias("category"),
    col("price")
)
delete_folder_if_exists(f"{STAGING_PATH}/products")
products.write.mode("overwrite").parquet(f"{STAGING_PATH}/products")
logger.info(f"Products staged: {products.count()} rows")


orders = spark.read.csv(f"{RAW_CSV_PATH}/orders.csv", header=True, inferSchema=True)
orders = orders.dropDuplicates(["order_id"]).filter(col("order_id").isNotNull())
orders = orders.select(
    col("order_id"),
    col("customer_id"),
    col("order_status").alias("status"),
    to_date(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("order_date")
)
delete_folder_if_exists(f"{STAGING_PATH}/orders")
orders.write.mode("overwrite").parquet(f"{STAGING_PATH}/orders")
logger.info(f"Orders staged: {orders.count()} rows")


order_items = spark.read.csv(f"{RAW_CSV_PATH}/order_items.csv", header=True, inferSchema=True)
order_items = order_items.dropDuplicates(["order_id", "order_item_id"]).filter(col("order_item_id").isNotNull())
order_items = order_items.select(
    col("order_id"),
    col("order_item_id"),
    col("product_id"),
    col("seller_id"),
    col("price"),
    col("freight_value")
)
delete_folder_if_exists(f"{STAGING_PATH}/order_items")
order_items.write.mode("overwrite").parquet(f"{STAGING_PATH}/order_items")
logger.info(f"Order items staged: {order_items.count()} rows")


sellers = spark.read.csv(f"{RAW_DB_PATH}/sellers.csv", header=True, inferSchema=True)
sellers = sellers.dropDuplicates(["seller_id"]).filter(col("seller_id").isNotNull()).filter(col("seller_name").isNotNull())
delete_folder_if_exists(f"{STAGING_PATH}/sellers")
sellers.write.mode("overwrite").parquet(f"{STAGING_PATH}/sellers")
logger.info(f"Sellers staged: {sellers.count()} rows")


inventory = spark.read.csv(f"{RAW_DB_PATH}/inventory.csv", header=True, inferSchema=True)
inventory = inventory.dropDuplicates(["product_id"]).filter(col("product_id").isNotNull()).filter(col("stock_quantity").isNotNull())
inventory = inventory.withColumn("last_updated", to_date(col("last_updated"), "yyyy-MM-dd"))
delete_folder_if_exists(f"{STAGING_PATH}/inventory")
inventory.write.mode("overwrite").parquet(f"{STAGING_PATH}/inventory")
logger.info(f"Inventory staged: {inventory.count()} rows")


product_metadata = spark.read.csv(f"{RAW_API_PATH}/product_metadata.csv", header=True, inferSchema=True, multiLine=True, escape='"')
if "id" in product_metadata.columns:
    product_metadata = product_metadata.withColumnRenamed("id", "product_id")
product_metadata = product_metadata.dropDuplicates(["product_id"]).filter(col("product_id").isNotNull())
delete_folder_if_exists(f"{STAGING_PATH}/product_metadata")
product_metadata.write.mode("overwrite").parquet(f"{STAGING_PATH}/product_metadata")
logger.info(f"Product metadata staged: {product_metadata.count()} rows")


logger.info("Raw to Staging pipeline completed successfully!")
spark.stop()
