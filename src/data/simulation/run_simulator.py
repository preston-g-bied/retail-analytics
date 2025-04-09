# src/data/simulation/run_simulator.py

"""
Script to run data simulators and save generated data.
"""

import os
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from src.data.simulation.retail_simulator import RetailSimulator

from src.utils.config_loader import load_config_with_env_vars

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_to_postgres(data: Dict[str, pd.DataFrame], connection_string: str) -> None:
    """
    Save generated data to PostgreSQL database.
    
    Args:
        data: Dictionary of DataFrames to save
        connection_string: SQLAlchemy connection string
    """
    import sqlalchemy

    # create database engine
    engine = sqlalchemy.create_engine(connection_string)

    # map dataframes to staging tables
    table_mapping = {
        "customers": "staging.stg_customer",
        "products": "staging.stg_product",
        "transactions": "staging.stg_transaction",
        "transaction_items": "staging.stg_transaction_item"
    }

    # rename columns to match staging tables
    column_mappings = {
        "customers": {
            "customer_key": "customer_key",
            "first_name": "first_name",
            "last_name": "last_name",
            "email": "email",
            "phone": "phone",
            "created_at": "create_date",
            "updated_at": "update_date"
        },
        "products": {
            "product_key": "product_key",
            "product_name": "product_name",
            "description": "description",
            "category": "category",
            "subcategory": "subcategory",
            "brand": "brand",
            "supplier": "supplier",
            "unit_price": "unit_price",
            "cost_price": "cost_price"
        },
        "transactions": {
            "transaction_key": "transaction_key",
            "created_at": "transaction_date",
            "customer_id": "customer_key",  # this needs to be replaced with customer_key
            "total_amount": "total_amount",
            "discount_amount": "discount_amount",
            "tax_amount": "tax_amount",
            "shipping_amount": "shipping_amount",
            "payment_method": "payment_method",
            "channel": "channel",
            "is_return": "is_return"
        },
        "transaction_items": {
            "transaction_id": "transaction_key",  # this needs to be replaced with transaction_key
            "product_id": "product_key",  # this needs to be replaced with product_key
            "quantity": "quantity",
            "unit_price": "unit_price",
            "discount_amount": "discount_amount",
            "tax_amount": "tax_amount",
            "line_total": "line_total"
        }
    }

    # process and save each dataframe
    for df_name, df in data.items():
        if df_name in table_mapping:
            # create a copy to avoid modifying the original
            df_copy = df.copy()

            # map values that need to be joined from other tables
            if df_name == "transactions":
                # map customer_id to customer_key
                customer_map = data["customers"][["customer_id", "customer_key"]].set_index("customer_id")
                df_copy["customer_key"] = df_copy["customer_id"].map(customer_map["customer_key"])

            elif df_name == "transaction_items":
                # map transaction_id to transaction_key
                transaction_map = data["transactions"][["transaction_id", "transaction_key"]].set_index("transaction_id")
                df_copy["transaction_key"] = df_copy["transaction_id"].map(transaction_map["transaction_key"])
                
                # map product_id to product_key
                product_map = data["products"][["product_id", "product_key"]].set_index("product_id")
                df_copy["product_key"] = df_copy["product_id"].map(product_map["product_key"])

            # select and rename columns
            if df_name in column_mappings:
                # get old columns to keep and their new names
                columns_to_keep = list(column_mappings[df_name].keys())
                new_column_names = list(column_mappings[df_name].values())

                # select columns and rename
                df_copy = df_copy[columns_to_keep].rename(columns=column_mappings[df_name])

                # add batch ID and source
                now = datetime.now()
                batch_id = now.strftime("%Y%m%d%H%M%S")
                df_copy["source"] = "simulator"
                df_copy["batch_id"] = batch_id
                df_copy["processed_at"] = now

                # save to database
                table_name = table_mapping[df_name]
                df_copy.to_sql(
                    name=table_name.split('.')[-1],
                    schema=table_name.split('.')[0],
                    con=engine,
                    if_exists='append',
                    index=False
                )

                logger.info(f"Saved {len(df_copy)} records to {table_name}")

def save_to_mongodb(data: Dict[str, pd.DataFrame], connection_string: str) -> None:
    """
    Save generated data to MongoDB database.
    
    Args:
        data: Dictionary of DataFrames to save
        connection_string: MongoDB connection string
    """
    import pymongo

    # connect to MongoDB
    client = pymongo.MongoClient(connection_string)
    db = client.retail_analytics

    # map dataframes to collections
    collection_mapping = {
        "products": db.products,
        "customers": db.customers
    }

    # process and save each dataframe
    for df_name, df in data.items():
        if df_name in collection_mapping:
            collection = collection_mapping[df_name]

            # convert DataFrame to list of dictionaries
            records = df.to_dict(orient='records')

            # add source and timestamp
            for record in records:
                record["source"] = "simulator"
                record["created_at"] = datetime.now()

            # insert into collection
            result = collection.insert_many(records)

            logger.info(f"Saved {len(result.inserted_ids)} records to {df_name} collection")

def main():
    """Main function to run the simulator."""
    parser = argparse.ArgumentParser(description="Run retail data simulator")

    parser.add_argument(
        '--config',
        type=str,
        default='config/simulation/retail_simulator.json',
        help='Path to simulator configuration'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/simulated',
        help='Directory to save generated data'
    )

    parser.add_argument(
        '--format',
        type=str,
        choices=['csv', 'json', 'parquet'],
        default='csv',
        help='Output file format'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for reproducability'
    )

    parser.add_argument(
        '--postgres',
        action='store_true',
        help='Save data to PostgreSQL'
    )

    parser.add_argument(
        '--mongodb',
        action='store_true',
        help='Save data to MongoDB'
    )

    args = parser.parse_args()

    # load config if file exists
    config = None
    if os.path.isfile(args.config):
        config = load_config_with_env_vars(args.config)

    # create simulator
    simulator = RetailSimulator("retail", config=config, seed=args.seed)

    # run simulation
    logger.info("Starting data simulation")
    data = simulator.generate()

    # save metadata
    metadata_path = os.path.join(args.output_dir, "metadata", f"retail_simulator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    simulator.save_metadata(metadata_path)

    # save data to files
    logger.info(f"Saving data to {args.output_dir} in {args.format} format")
    saved_files = simulator.save_data(data, args.output_dir, args.format)

    for data_name, file_path in saved_files.items():
        logger.info(f"Saved {data_name} data to {file_path}")

    # save to PostgreSQL if requested
    if args.postgres:
        from dotenv import load_dotenv
        load_dotenv()

        postgres_conn = os.getenv('POSTGRES_CONNECTION_STRING',
                                  'postgresql://retail_user:retail_password@localhost:5432/retail_analytics')
        
        logger.info("Saving data to PostgreSQL")
        save_to_postgres(data, postgres_conn)

    # save to MongoDB if requested
    if args.mongodb:
        from dotenv import load_dotenv
        load_dotenv()

        mongodb_conn = os.getenv('MONGODB_CONNECTION_STRING',
                                 'mongodb://retail_user:retail_password@localhost:27017/retail_analytics')
        
        logger.info("Saving data to MongoDB")
        save_to_mongodb(data, mongodb_conn)

    logger.info("Simulation completed successfully")

if __name__ == "__main__":
    main()