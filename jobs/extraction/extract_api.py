import os
import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://dummyjson.com/products"
OUTPUT_DIR = "/app/data/raw/api"
os.makedirs(OUTPUT_DIR, exist_ok=True)

response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    products = data["products"]
    df = pd.DataFrame(products)
    output_path = os.path.join(OUTPUT_DIR, "product_metadata.csv")
    df.to_csv(output_path, index=False)
    logger.info(f"API extraction completed successfully. {len(df)} products saved.")
else:
    logger.error(f"Failed to fetch data from API. Status code: {response.status_code}")