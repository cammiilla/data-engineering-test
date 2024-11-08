import argparse
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import sys

# Setup basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to load credentials from a JSON file
def load_credentials(credentials_path):
    try:
        with open(credentials_path, 'r') as file:
            credentials = json.load(file)
            logger.info("Credentials loaded successfully.")
            return credentials
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        sys.exit(1)

# Function to establish database connection
def connect_to_db(credentials):
    try:
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            database=credentials["database"],
            user=credentials["user"],
            password=credentials["password"]
        )
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        sys.exit(1)

# Function to create schema if it doesn't exist
def create_schema_if_not_exists(conn, schema_name):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
            conn.commit()
            logger.info(f"Schema '{schema_name}' verified or created.")
    except Exception as e:
        logger.error(f"Error creating schema '{schema_name}': {e}")
        conn.rollback()

# Function to drop and create tables with error handling
def drop_and_create_table(conn, drop_query, create_query):
    try:
        with conn.cursor() as cur:
            # Drop the table if it exists
            cur.execute(drop_query)
            # Create the table
            cur.execute(create_query)
            conn.commit()
            logger.info("Table created successfully.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()

# Function to load parquet data into a specified table
def load_data(conn, parquet_path, insert_query, table_name):
    try:
        # Load data from parquet
        data_df = pd.read_parquet(parquet_path)
        data = [tuple(row) for row in data_df.values]
        
        # Insert data into the table
        with conn.cursor() as cur:
            cur.executemany(insert_query, data)
            conn.commit()
            logger.info(f"Data loaded successfully into table '{table_name}' from {parquet_path}.")
    except Exception as e:
        logger.error(f"Error loading data into table '{table_name}' from {parquet_path}: {e}")
        conn.rollback()

# Main function to execute the process
def main(credentials_path):
    # Load credentials from file
    credentials = load_credentials(credentials_path)

    # Connect to the PostgreSQL database
    conn = connect_to_db(credentials)

    # Ensure the target schema exists
    target_schema = "finance_erp"
    create_schema_if_not_exists(conn, target_schema)

    # Drop and create tables in the target schema
    table_operations = [
        (f"""DROP TABLE IF EXISTS {target_schema}.invoices;""",
         f"""CREATE TABLE {target_schema}.invoices (
            id VARCHAR(255) PRIMARY KEY,
            orderId VARCHAR(255),
            companyId VARCHAR(255),
            grossValue VARCHAR(255),
            vat VARCHAR(255) NOT NULL
        );"""),
        
        (f"""DROP TABLE IF EXISTS {target_schema}.orders;""",
         f"""CREATE TABLE {target_schema}.orders (
            order_id VARCHAR(255),
            date VARCHAR(255),
            company_id VARCHAR(255),
            company_name VARCHAR(255),
            crate_type VARCHAR(255),
            contact_name VARCHAR(255),
            contact_surname VARCHAR(255),
            city VARCHAR(255),
            cp VARCHAR(255)
        );"""),
        
        (f"""DROP TABLE IF EXISTS {target_schema}.sales_owners;""",
         f"""CREATE TABLE {target_schema}.sales_owners (
            order_id VARCHAR(255),
            salesowners VARCHAR(255)
        );""")
    ]
    
    for drop_query, create_query in table_operations:
        drop_and_create_table(conn, drop_query, create_query)

    # Load data into each table
    data_loads = [
        (credentials['invoices_parquet_path'], f'INSERT INTO {target_schema}.invoices (id, orderId, companyId, grossValue, vat) VALUES (%s, %s, %s, %s, %s)', "invoices"),
        (credentials['orders_parquet_path'], f'INSERT INTO {target_schema}.orders (order_id, date, company_id, company_name, crate_type, contact_name, contact_surname, city, cp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', "orders"),
        (credentials['sales_owners_path'], f'INSERT INTO {target_schema}.sales_owners (order_id, salesowners) VALUES (%s, %s)', "sales_owners")
    ]
    
    for parquet_path, insert_query, table_name in data_loads:
        load_data(conn, parquet_path, insert_query, table_name)
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    # Command-line argument to specify the path to credentials.json
    parser = argparse.ArgumentParser(description="Load data into PostgreSQL tables from Parquet files.")
    parser.add_argument("--credentials_path", type=str, required=True, help="Path to the credentials JSON file")
    
    args = parser.parse_args()
    main(args.credentials_path)
