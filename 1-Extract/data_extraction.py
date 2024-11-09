import polars as pl
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataPipeline")

# Initialize Spark session for PySpark operations
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# Load configuration from external JSON file
def load_config(path="1-Exconfig.json"):
    try:
        with open(path, 'r') as file:
            config = json.load(file)
            logger.info(f"Configuration loaded successfully from {path}")
            return config
    except Exception as e:
        logger.error(f"Error loading config file at {path}: {e}")
        raise

# Parsing function to handle entries in 'contact_data' with error handling and logging
def parse_contact_data(entry):
    try:
        logger.info(f"Processing entry: {entry}")
        if entry in ["N/A", "0", ""]:
            logger.info(f"Entry is None, 0, or empty. Returning default dictionary for entry: {entry}")
            return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}

        cleaned_entry = entry.replace("'", '"')
        parsed_entry = json.loads(cleaned_entry)
        
        if isinstance(parsed_entry, list) and parsed_entry:
            parsed_entry = parsed_entry[0]

        if not isinstance(parsed_entry, dict):
            logger.warning(f"Parsed entry is not a dictionary. Returning default for entry: {entry}")
            return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}
        
        logger.info(f"Successfully parsed dictionary: {parsed_entry}")
        return {
            "contact_data": entry,
            "contact_name": parsed_entry.get("contact_name"),
            "contact_surname": parsed_entry.get("contact_surname"),
            "city": parsed_entry.get("city"),
            "cp": parsed_entry.get("cp")
        }
    except Exception as e:
        logger.error(f"Error parsing entry: {entry}. Error: {e}")
        return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}

# Function to load CSV data using Polars
def load_csv(path):
    try:
        data = pl.read_csv(path, separator=";")
        logger.info(f"CSV data loaded successfully from {path}")
        return data
    except Exception as e:
        logger.error(f"Error loading CSV file at {path}: {e}")
        raise

# Function to load JSON data
def load_json(path):
    try:
        with open(path, 'r') as file:
            content = json.load(file)
            logger.info(f"JSON data loaded successfully from {path}")
            return content
    except Exception as e:
        logger.error(f"Error loading JSON file at {path}: {e}")
        raise

# Function to process orders data
def process_orders_data(orders_df):
    try:
        orders_df = orders_df.with_columns([
            pl.col("order_id").cast(pl.Utf8),
            pl.col("date").cast(pl.Utf8),
            pl.col("company_id").cast(pl.Utf8),
            pl.col("company_name").cast(pl.Utf8),
            pl.col("crate_type").cast(pl.Utf8),
            pl.col("contact_data").cast(pl.Utf8),
            pl.col("salesowners").cast(pl.Utf8)
        ])
        
        orders_df = orders_df.fill_null("N/A").unique()
        logger.info("Orders data schema stabilized and duplicates removed.")
        
        orders_df = orders_df.with_columns(
            pl.col("contact_data").map_elements(lambda x: parse_contact_data(x), return_dtype=pl.Object).alias("new_col")
        )
        
        contact_data = pl.json_normalize(orders_df["new_col"]).unique()
        df_combined = orders_df.join(contact_data, how="inner", on="contact_data")
        orders_cleaned = df_combined.drop("contact_data", "salesowners", "new_col")
        
        logger.info("Orders data processing complete.")
        return orders_cleaned
    except Exception as e:
        logger.error(f"Error processing orders data: {e}")
        raise

# Split and explode salesowners, then add an order index
def process_salesowners_data(orders_df):
    try:
        salesowners_df = (
            orders_df
            .with_columns(
                pl.col("salesowners").str.split(", ").alias("salesowners_list")
            )
            .explode("salesowners_list")  # Explode into individual salesowners
            .with_columns(
                # Create a sequential index for each salesowner within the same order_id
                pl.col("salesowners_list").cum_count().over("order_id").alias("salesowners_order")
            )
            .rename({"salesowners_list": "salesowner"})  # Rename the exploded column
        )
        salesowners_df = salesowners_df.select(["order_id", "salesowner","salesowners_order"])
        logger.info("Salesowners data split and expanded successfully.")
        return salesowners_df
    except Exception as e:
        logger.error(f"Error processing salesowners data: {e}")
        raise

# Function to process invoice data
def process_invoice_data(invoice_content):
    try:
        invoice_data = pl.DataFrame(invoice_content)
        logger.info("Invoice data processed successfully.")
        return invoice_data
    except Exception as e:
        logger.error(f"Error processing invoice data: {e}")
        raise

# Main pipeline function
def main():
    config = load_config("1-Extract/config.json")
    
    # File paths from config
    orders_csv_path = config["orders_csv_path"]
    invoice_json_path = config["invoice_json_path"]
    output_paths = config["output_paths"]
    
    # Load orders CSV data
    orders_df = load_csv(orders_csv_path)
    
    # Load JSON file containing invoicing data
    invoice_content = load_json(invoice_json_path)["data"]["invoices"]

    # Process orders data
    orders_cleaned = process_orders_data(orders_df)

    # Process salesowners data
    salesowners_data = process_salesowners_data(orders_df)

    # Process invoice data
    invoice_data = process_invoice_data(invoice_content)

    # Save processed data
    try:
        orders_cleaned.write_parquet(output_paths["orders_parquet"])
        salesowners_data.write_parquet(output_paths["salesowners_parquet"])
        invoice_data.write_parquet(output_paths["invoice_parquet"])
        logger.info("Data saved successfully in parquet format.")
    except Exception as e:
        logger.error(f"Error saving parquet files: {e}")
        raise

if __name__ == "__main__":
    main()
