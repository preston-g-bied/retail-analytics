# src/data/database_tests.py

"""
Comprehensive script for testing database connections and data loading.
"""

import os
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import time

import pandas as pd
import psycopg2
import pymongo
import redis

from src.data.database import (
    PostgresConnection, MongoDBConnection,
    get_postgres_cursor, get_redis_connection
)

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseTester:
    """
    Utility class for testing database connections and data loading.
    """

    def __init__(self):
        """Initialize database tester."""
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "postgres": {"status": "not_tested"},
            "mongodb": {"status": "not_tested"},
            "redis": {"status": "not_tested"}
        }

    def test_postgres_connection(self) -> bool:
        """
        Test connection to PostgreSQL.
        
        Returns:
            True if connection successful, False otherwise
        """
        logger.info("Testing PostgreSQL connection...")
        start_time = time.time()
        try:
            with PostgresConnection as conn:
                cursor = get_postgres_cursor(conn)

                # test query
                cursor.execute("SELECT version()")
                version = cursor.fetchone()

                elapsed_time = time.time() - start_time

                self.results["postgres"] = {
                    "status": "success",
                    "version": version["version"] if version else None,
                    "response_time_ms": round(elapsed_time * 1000, 2)
                }

                logger.info(f"PostgreSQL connection successful: {version}")
                return True
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.results["postgres"] = {
                "status": "error",
                "error": str(e),
                "response_time_ms": round(elapsed_time * 1000, 2)
            }
            logger.error(f"PostgreSQL connection failed: {e}")
            return False
        
    def test_postgres_tables(self) -> Dict[str, int]:
        """
        Test PostgreSQL database tables and count records.
        
        Returns:
            Dictionary mapping table names to record counts
        """
        logger.info("Testing PostgreSQL tables...")

        table_counts = {}

        try:
            with PostgresConnection() as conn:
                cursor = get_postgres_cursor(conn)

                # get schemas
                cursor.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """)
                schemas = [row["schema_name"] for row in cursor.fetchall()]

                # get tables for each schema
                for schema in schemas:
                    cursor.execute(f"""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = '{schema}'
                    """)
                    tables = [row["table_name"] for row in cursor.fetchall()]

                    # count records in each table
                    for table in tables:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                            count = cursor.fetchone()["count"]
                            table_counts[f"{schema}.{table}"] = count
                        except Exception as e:
                            logger.warning(f"Error counting records in {schema}.{table}: {e}")
                            table_counts[f"{schema}.{table}"] = f"Error: {str(e)}"

                self.results["postgres"]["tables"] = table_counts
                logger.info(f"Found {len(table_counts)} tables in PostgreSQL")
                return table_counts
            
        except Exception as e:
            logger.error(f"Error testing PostgreSQL tables: {e}")
            self.results["postgres"]["tables_error"] = str(e)
            return {}
        
    def test_mongodb_connection(self) -> bool:
        """
        Test connection to MongoDB.
        
        Returns:
            True if connection successful, False otherwise
        """
        logger.info("Testing MongoDB connection...")
        start_time = time.time()

        try:
            with MongoDBConnection as db:
                # test command
                status = db.command("serverStatus")

                elapsed_time = time.time() - start_time

                self.results["mongodb"] = {
                    "status": "success",
                    "version": status.get("version"),
                    "response_time_ms": round(elapsed_time * 1000, 2)
                }

                logger.info(f"MongoDB connection successful: version {status.get('version')}")
                return True
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.results["mongodb"] = {
                "status": "error",
                "error": str(e),
                "response_time_ms": round(elapsed_time * 100, 2)
            }
            logger.error(f"MongoDB connection failed: {e}")
            return False
        
    def test_mongodb_collections(self) -> Dict[str, int]:
        """
        Test MongoDB collections and count documents.
        
        Returns:
            Dictionary mapping collection names to document counts
        """
        logger.info("Testing MongoDB collections...")

        collection_counts = {}

        try:
            with MongoDBConnection() as db:
                # get all collections
                collections = db.list_collection_names()

                # count documents in each collection
                for collection in collections:
                    try:
                        count = db[collection].count_documents({})
                        collection_counts[collection] = count
                    except Exception as e:
                        logger.warning(f"Error counting documents in {collection}: {e}")
                        collection_counts[collection] = f"Error: {str(e)}"

                self.results["mongodb"]["collections"] = collection_counts
                logger.info(f"Found {len(collection_counts)} collections in MongoDB")
                return collection_counts
            
        except Exception as e:
            logger.error(f"Error testing MongoDB collections: {e}")
            self.results["mongodb"]["collections_error"] = str(e)
            return {}
        
    def test_redis_connection(self) -> bool:
        """
        Test connection to Redis.
        
        Returns:
            True if connection successful, False otherwise
        """
        logger.info("Testing Regis connection...")
        start_time = time.time()

        try:
            r = get_redis_connection()

            # test ping
            response = r.ping()

            elapsed_time = time.time() - start_time

            self.results["redis"] = {
                "status": "success",
                "ping_response": response,
                "response_time_ms": round(elapsed_time * 1000, 2)
            }

            logger.info("Redis connection successful")
            return True
        
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.results["redis"] = {
                "status": "error",
                "error": str(e),
                "response_time_ms": round(elapsed_time * 1000, 2)
            }
            logger.error(f"Redis connection failed: {e}")
            return False
        
    def test_all_connections(self) -> Dict[str, Any]:
        """
        Test all database connections.
        
        Returns:
            Results dictionary
        """
        # test PostgreSQL
        postgres_ok = self.test_postgres_connection()
        if postgres_ok:
            self.test_postgres_tables()

        # test MongoDB
        mongodb_ok = self.test_mongodb_connection()
        if mongodb_ok:
            self.test_mongodb_collections()

        # test Redis
        redis_ok = self.test_redis_connection()

        # overall status
        self.results["all_connections_ok"] = postgres_ok and mongodb_ok and redis_ok

        return self.results
    
def load_data_to_postgres(file_path: str, table_name: str, schema: str = "staging") -> Dict[str, Any]:
    """
    Load data from a CSV file to a PostgreSQL table.
    
    Args:
        file_path: Path to the CSV file
        table_name: Name of the table to load data into
        schema: Database schema
        
    Returns:
        Results of the operation
    """
    logger.info(f"Loading data from {file_path} to {schema}.{table_name}...")

    results = {
        "file_path": file_path,
        "table_name": f"{schema}.{table_name}",
        "status": "not_started"
    }

    start_time = time.time()

    try:
        # load CSV data
        df = pd.read_csv(file_path)
        results["record_count"] = len(df)

        # add metadata columns if not present
        if "source" not in df.columns:
            df["source"] = "file_import"

        if "batch_id" not in df.columns:
            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
            df["batch_id"] = batch_id

        if "processed_at" not in df.columns:
            df["processed_at"] = datetime.now()

        # connect to PostgreSQL
        with PostgresConnection() as conn:
            # check if table exists
            cursor = get_postgres_cursor(conn)
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'           
                )
            """)
            table_exists = cursor.fetchone()["exists"]

            if not table_exists:
                results["status"] = "error"
                results["error"] = f"Table {schema}.{table_name} does not exist"
                logger.error(results["error"])
                return results
            
            # insert data
            try:
                # create temporary table with compatible structure
                temp_table = f"temp_{table_name}_{batch_id}"

                # get column list from the main table
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                """)
                columns = {row["column_name"]: row["data_type"] for row in cursor.fetchall()}

                # filter DataFrame to only include columns that exist in the table
                df_filtered = df[[col for col in df.columns if col in columns]]

                # write to temporary table
                # use pandas to_sql with metold='multi' for improved performance
                from sqlalchemy import create_engine
                conn_str = os.getenv('POSTGRES_CONNECTION_STRING')
                engine = create_engine(conn_str)
                df_filtered.to_sql(
                    temp_table,
                    engine,
                    schema=schema,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )

                # insert from temporary table to main table
                column_list = ", ".join(df_filtered.columns)
                cursor.execute(f"""
                    INSERT INTO {schema}.{table_name} ({column_list})
                    SELECT {column_list} FROM {schema}.{temp_table}
                """)
                
                # drop temporary table
                cursor.execute(f"DROP TABLE {schema}.{temp_table}")

                # commit transaction
                conn.commit()

                # update results
                elapsed_time = time.time() - start_time
                results["status"] = "success"
                results["inserted_records"] = len(df_filtered)
                results["columns"] = df_filtered.columns.tolist()
                results["time_taken_s"] = round(elapsed_time, 2)

                logger.info(f"Successfully inserted {len(df_filtered)} records into {schema}.{table_name}")

            except Exception as e:
                conn.rollback()
                elapsed_time = time.time() - start_time
                results["status"] = "error"
                results["error"] = str(e)
                results["time_taken_s"] = round(elapsed_time, 2)
                logger.error(f"Error inserting data: {e}")

        return results
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        results["status"] = "error"
        results["error"] = str(e)
        results["time_taken_s"] = round(elapsed_time, 2)
        logger.error(f"Error loading data from {file_path}: {e}")
        return results
    
def load_data_to_mongodb(file_path: str, collection_name: str) -> Dict[str, Any]:
    """
    Load data from a CSV file to a MongoDB collection.
    
    Args:
        file_path: Path to the CSV file
        collection_name: Name of the collection to load data into
        
    Returns:
        Results of the operation
    """
    logger.info(f"Loading data from {file_path} to {collection_name} collection...")

    results = {
        "file_path": file_path,
        "collection_name": collection_name,
        "status": "not_started"
    }

    start_time = time.time()

    try:
        # load CSV data
        df = pd.read_csv(file_path)
        results["record_count"] = len(df)

        # convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')

        # add metadata
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        for record in records:
            record["source"] = "file_import"
            record["batch_id"] = batch_id
            record["imported_at"] = datetime.now()

        # connect to MongoDB
        with MongoDBConnection() as db:
            # insert data
            try:
                result = db[collection_name].insert_many(records)

                # update results
                elapsed_time = time.time() - start_time
                results["status"] = "success"
                results["inserted_records"] = len(result.inserted_ids)
                results["time_taken_s"] = round(elapsed_time, 2)
                
                logger.info(f"Successfully inserted {len(result.inserted_ids)} records into {collection_name}")

            except Exception as e:
                elapsed_time = time.time() - start_time
                results["status"] = "error"
                results["error"] = str(e)
                results["time_taken_s"] = round(elapsed_time, 2)
                logger.error(f"Error inserting data: {e}")

        return results
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        results["status"] = "error"
        results["error"] = str(e)
        results["time_taken_s"] = round(elapsed_time, 2)
        logger.error(f"Error loading data from {file_path}: {e}")
        return results
    
def load_simulated_data_to_databases(simulated_dir: str) -> Dict[str, Any]:
    """
    Load simulated data to both PostgreSQL and MongoDB.
    
    Args:
        simulated_dir: Directory containing simulated data
    
    Returns:
        Dictionary with results of the operations
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "postgres_results": [],
        "mongodb_results": []
    }

    # map file patterns to tables/collections
    postgres_mappings = [
        {"pattern": "retail_customers_", "table": "stg_customer"},
        {"pattern": "retail_products_", "table": "stg_product"},
        {"pattern": "retail_transactions_", "table": "stg_transaction"},
        {"pattern": "retail_transaction_items_", "table": "stg_transaction_item"}
    ]
    
    mongodb_mappings = [
        {"pattern": "retail_customers_", "collection": "customers"},
        {"pattern": "retail_products_", "collection": "products"}
    ]

    # find files in the directory
    files = os.listdir(simulated_dir)

    # load data to PostgreSQL
    for mapping in postgres_mappings:
        pattern = mapping["pattern"]
        table = mapping["table"]

        # find matching files
        matching_files = [f for f in files if f.startswith(pattern) and f.endswith(".csv")]

        if not matching_files:
            logger.warning(f"No files matching pattern '{pattern}' found in '{simulated_dir}'")
            continue

        # use the most recent file
        most_recent = sorted(matching_files, reverse=True)[0]
        file_path = os.path.join(simulated_dir, most_recent)

        # load data
        result = load_data_to_postgres(file_path, table)
        results["postgres_results"].append(result)

    for mapping in mongodb_mappings:
        pattern = mapping["pattern"]
        collection = mapping["collection"]

        # find matching files
        matching_files = [f for f in files if f.startswith(pattern) and f.endswith(".csv")]

        if not matching_files:
            logger.warning(f"No files matching pattern '{pattern}' found in {simulated_dir}")
            continue

        # use the mot recent file
        most_recent = sorted(matching_files, reverse=True)[0]
        file_path = os.path.join(simulated_dir, most_recent)

        # load data
        result = load_data_to_mongodb(file_path, collection)
        results["mongodb_results"].append(result)

    # summarize results
    results["postgres_success"] = all(r["status"] == "success" for r in results["postgres_results"])
    results["mongodb_success"] = all(r["status"] == "success" for r in results["mongodb_results"])
    results["all_success"] = results["postgres_success"] and results["mongodb_success"]

    return results

def main():
    """
    Main function for testing database connections and data loading.
    """
    parser = argparse.ArgumentParser(description="Test database connections and load data")

    parser.add_argument(
        '--connections-only',
        action='store_true',
        help='Test database connections only, don\'t load data'
    )

    parser.add_argument(
        '--load-data',
        action='store_true',
        help='Load simulated data to databased'
    )

    parser.add_argument(
        '--simulated-dir',
        type=str,
        default='data/simulated',
        help='Directory containing simulated data'
    )

    parser.add_argument(
        '--results-file',
        type=str,
        default='tests/results/database_test_results.json',
        help='Path to save test results'
    )

    args = parser.parse_args()

    # create results directory
    os.makedirs(os.path.dirname(args.results_file), exist_ok=True)

    # test database connections
    tester = DatabaseTester()
    connection_results = tester.test_all_connections()

    # print connection results summary
    logger.info("\nDatabase Connection Results:")
    logger.info(f"  PostgreSQL: {connection_results['postgres']['status']}")
    logger.info(f"  MongoDB: {connection_results['mongodb']['status']}")
    logger.info(f"  Redis: {connection_results['redis']['status']}")
    logger.info(f"  All connections OK: {connection_results['all_connections_ok']}")

    # if only testing connections, save results and exit
    if args.connections_only:
        with open(args.results_file, 'w') as f:
            json.dump(connection_results, f, indent=2)

        logger.info(f"Connection test results saved to {args.results_file}")
        return
    
    # if all connections are good and we want to load data
    if connection_results['all_connections_ok'] and args.load_data:
        # check if simulated data directory exists
        if not os.path.isdir(args.simulated_dir):
            logger.error(f"Simulated data directory {args.simulated_dir} does not exist")
            return
        
        # load data
        logger.info("\nLoading simulated data to databases...")
        data_load_results = load_simulated_data_to_databases(args.simulated_dir)

        # print data load results summary
        logger.info("\nData Loading Results:")
        logger.info(f"  PostgreSQL success: {data_load_results['postgres_success']}")
        logger.info(f"  MongoDB success: {data_load_results['mongodb_success']}")
        logger.info(f"  All loading operations successful: {data_load_results['all_success']}")

        # combine results
        combined_results = {
            "connection_tests": connection_results,
            "data_loading": data_load_results
        }

        # save results
        with open(args.results_file, 'w') as f:
            json.dump(combined_results, f, indent=2)

        logger.info(f"Combined test results saved to {args.results_file}")
    else:
        # save connection results only
        with open(args.results_file, 'w') as f:
            json.dump(connection_results, f, indent=2)
        
        logger.info(f"Connection test results saved to {args.results_file}")

if __name__ == "__main__":
    main()