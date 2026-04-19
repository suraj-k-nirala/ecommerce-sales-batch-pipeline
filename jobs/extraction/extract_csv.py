import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#  Use ABSOLUTE paths inside container
SOURCE_PATH = "/app/data/raw/csv"
TARGET_PATH = "/app/data/raw/csv_ingested"

FILES = [
    "customers.csv",
    "products.csv",
    "orders.csv",
    "order_items.csv"
]

# create target folder
os.makedirs(TARGET_PATH, exist_ok=True)

for file_name in FILES:
    source_file = os.path.join(SOURCE_PATH, file_name)
    target_file = os.path.join(TARGET_PATH, file_name)

    if os.path.exists(source_file):
        with open(source_file, "rb") as src, open(target_file, "wb") as dst:
            dst.write(src.read())
        logger.info(f"Copied: {file_name}")
    else:
        logger.warning(f"File not found: {source_file}")

logger.info("CSV extraction completed successfully!")