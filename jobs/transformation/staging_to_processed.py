import os
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("StagingToProcessedPipeline").getOrCreate()

STAGING_PATH = "/app/data/staging"
PROCESSED_PATH = "/app/data/processed"


def write_parquet(df, name, partition_by=None):
    path = f"{PROCESSED_PATH}/{name}"
    if os.path.exists(path):
        shutil.rmtree(path)
    if partition_by:
        df.write.mode("overwrite").partitionBy(*partition_by).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
    logger.info(f"{name} written: {df.count()} rows")


customers = spark.read.parquet(f"{STAGING_PATH}/customers")
products = spark.read.parquet(f"{STAGING_PATH}/products")
orders = spark.read.parquet(f"{STAGING_PATH}/orders")
order_items = spark.read.parquet(f"{STAGING_PATH}/order_items")
sellers = spark.read.parquet(f"{STAGING_PATH}/sellers")
inventory = spark.read.parquet(f"{STAGING_PATH}/inventory")
product_metadata = spark.read.parquet(f"{STAGING_PATH}/product_metadata")

dim_customers = customers.select("customer_id", "customer_name", "email", "signup_date")

dim_sellers = sellers.select("seller_id", "seller_name", "city", "state")

selected_metadata_cols = [c for c in ["product_id", "brand", "rating", "stock", "category"] if c in product_metadata.columns]
product_metadata_clean = product_metadata.select(*selected_metadata_cols)
if "category" in product_metadata_clean.columns:
    product_metadata_clean = product_metadata_clean.withColumnRenamed("category", "api_category")
if "stock" in product_metadata_clean.columns:
    product_metadata_clean = product_metadata_clean.withColumnRenamed("stock", "api_stock")

dim_products = (
    products.alias("p")
    .join(inventory.alias("i"), col("p.product_id") == col("i.product_id"), "left")
    .join(product_metadata_clean.alias("m"), col("p.product_id") == col("m.product_id"), "left")
    .select(
        col("p.product_id"), col("p.product_name"), col("p.category"), col("p.price"),
        col("i.stock_quantity"), col("i.warehouse_location"), col("i.last_updated"),
        col("m.brand").alias("brand") if "brand" in product_metadata_clean.columns else col("p.product_id").cast("string").alias("brand"),
        col("m.rating").alias("rating") if "rating" in product_metadata_clean.columns else col("p.product_id").cast("double").alias("rating"),
        col("m.api_stock").alias("api_stock") if "api_stock" in product_metadata_clean.columns else col("i.stock_quantity").alias("api_stock"),
        col("m.api_category").alias("api_category") if "api_category" in product_metadata_clean.columns else col("p.category").alias("api_category")
    )
)
dim_products = dim_products.withColumn(
    "stock_status",
    when(col("stock_quantity").isNull(), "UNKNOWN")
    .when(col("stock_quantity") == 0, "OUT_OF_STOCK")
    .when(col("stock_quantity") < 20, "LOW_STOCK")
    .otherwise("IN_STOCK")
)

fact_sales = (
    order_items.alias("oi")
    .join(orders.alias("o"), col("oi.order_id") == col("o.order_id"), "inner")
    .select(
        col("oi.order_id"), col("oi.product_id"), col("o.customer_id"),
        col("oi.order_item_id").alias("quantity"),
        col("o.order_date"), col("o.status"),
        col("oi.price").alias("unit_price"),
        col("oi.freight_value")
    )
)
fact_sales = (
    fact_sales
    .withColumn("sale_amount", col("quantity") * col("unit_price"))
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
)

write_parquet(dim_customers, "dim_customers")
write_parquet(dim_sellers, "dim_sellers")
write_parquet(dim_products, "dim_products")
write_parquet(fact_sales, "fact_sales", partition_by=["year", "month"])

logger.info("Staging to Processed pipeline completed successfully!")
spark.stop()
