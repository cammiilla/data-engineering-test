import polars as pl
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Parsing function to handle entries in 'contact_data' with error handling and logging
def parse_contact_data(entry):
    """
    Parse the 'contact_data' field, handling possible issues like None or malformed JSON.

    Parameters:
    - entry: str or None. This represents the contact data in string format, which may contain JSON data.

    Returns:
    - dict: A dictionary containing parsed contact information with keys: 'contact_name', 'contact_surname', 'city', and 'cp'.
            If parsing fails, returns these fields as None.
    """
    try:
        # Log each entry being processed
        logger.info(f"Processing entry: {entry}")

        # Handle None or invalid entries directly
        if entry in ["N/A", "0", ""]:
            logger.info(f"Entry is None, 0, or empty. Returning default dictionary for entry: {entry}")
            return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}

        # Clean entry and parse as JSON
        cleaned_entry = entry.replace("'", '"')  # Replace single quotes with double quotes for JSON compatibility
        parsed_entry = json.loads(cleaned_entry)  # Parse the JSON string
        
        # If parsed entry is a list, extract the first dictionary in the list
        if isinstance(parsed_entry, list) and parsed_entry:
            parsed_entry = parsed_entry[0]

        # Ensure parsed entry is a dictionary
        if not isinstance(parsed_entry, dict):
            logger.warning(f"Parsed entry is not a dictionary. Returning default for entry: {entry}")
            return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}
        
        # Log successful parsing
        logger.info(f"Successfully parsed dictionary: {parsed_entry}")

        # Return parsed data, handling missing keys by setting them to None
        return {
            "contact_data": entry,
            "contact_name": parsed_entry.get("contact_name"),
            "contact_surname": parsed_entry.get("contact_surname"),
            "city": parsed_entry.get("city"),
            "cp": parsed_entry.get("cp")
        }
    except Exception as e:
        logger.error(f"Error parsing entry: {entry}. Error: {e}")
        # Return default dictionary with None values on parsing error
        return {"contact_data": entry, "contact_name": None, "contact_surname": None, "city": None, "cp": None}


# Initialize Spark session for PySpark operations
spark = SparkSession.builder.appName("DataExtraction").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataExtraction")

# File path for orders data
orders_csv_path = "resources/orders.csv"

# Read the orders CSV file using Polars with ';' as the separator
orders_raw = pl.read_csv(orders_csv_path, separator=";")

# Apply a consistent schema by casting columns to specific data types
orders_stable_schema = orders_raw.with_columns([
    pl.col("order_id").cast(pl.Utf8),      # Cast 'order_id' to String
    pl.col("date").cast(pl.Utf8),          # Cast 'date' to String
    pl.col("company_id").cast(pl.Utf8),    # Cast 'company_id' to String
    pl.col("company_name").cast(pl.Utf8),  # Cast 'company_name' to String
    pl.col("crate_type").cast(pl.Utf8),    # Cast 'crate_type' to String
    pl.col("contact_data").cast(pl.Utf8),  # Cast 'contact_data' to String
    pl.col("salesowners").cast(pl.Utf8)    # Cast 'salesowners' to String
])

# Fill null values in DataFrame with 'N/A'
orders_stable_schema = orders_stable_schema.fill_null("N/A")

# Remove duplicate rows to ensure data uniqueness
orders_stable_schema = orders_stable_schema.unique()

# Path to invoicing data JSON file
invoice_json_path = "resources/invoicing_data.json"

# Open and load JSON file containing invoicing data
with open(invoice_json_path, 'r') as file:
    content = json.load(file)

# Extract relevant data from nested JSON structure
invoice_json_content = content['data']['invoices']

# Verify all dictionaries in the list have the same keys
reference_keys = set(invoice_json_content[0].keys())
all_have_same_keys = all(set(d.keys()) == reference_keys for d in invoice_json_content)

# Log whether all entries have consistent keys
if all_have_same_keys:
    logger.info("All dictionaries have the same keys.")
else:
    logger.error("Not all dictionaries have the same keys.")

# Expand contact data in the DataFrame
orders_expand_columns = orders_stable_schema.with_columns(
    pl.col("contact_data").map_elements(lambda x: parse_contact_data(x), return_dtype=pl.Object).alias('new_col')
)

# Normalize nested JSON data in 'new_col' to separate columns for each contact field
contact_data = pl.json_normalize(orders_expand_columns['new_col'])

# Ensure uniqueness in expanded contact data
contact_data_unique = contact_data.unique()

# Merge expanded contact data with the original orders data
df_combined = orders_expand_columns.join(contact_data_unique, how="inner", on="contact_data")

# Drop redundant columns to finalize cleaned orders DataFrame
orders = df_combined.drop("contact_data", "salesowners", "new_col")

# Create a new DataFrame with 'order_id' and expanded 'salesowners' column
orders_id_per_salesowners = orders_stable_schema.select(["order_id", "salesowners"])

# Split 'salesowners' on commas, expand each into individual rows
orders_id_per_salesowners = (
    orders_id_per_salesowners.with_columns(pl.col("salesowners").str.split(",").alias("names"))
                             .explode("names")
)

# Rename columns for clarity
orders_id_per_salesowners = orders_id_per_salesowners.drop("salesowners")
orders_id_per_salesowners.rename({"names": "salesowners"})

