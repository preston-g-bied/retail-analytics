# src/data/database.py

"""
Database connection utilities for the retail analytics project.
Provides functions to connect to PostgreSQL, MongoDB, and Redis
"""

import os
import json
import logging
from typing import Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
import pymongo
import redis
from dotenv import load_dotenv

from src.utils.config_loader import load_config_with_env_vars

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# load environment variables
load_dotenv()

def load_config(config_type: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file or environment variables.

    Args:
        config_type: Type of configuration to load ('database', 'connectors', etc.)

    Returns:
        Dictionary containing configuration
    """
    # first try to load from environment variable
    env_var_name = f"RETAIL_ANALYRICS_{config_type.upper()}_CONFIG"
    config_path = os.getenv(env_var_name)

    # if not found, use default path
    if not config_path:
        config_path = f"config/{config_type}/{config_type}.json"

    try:
        return load_config_with_env_vars(config_path)
    except FileNotFoundError:
        logger.warning(f"Configuration file {config_path} not found. Checking for example config.")
        # try to load example config
        example_path = f"config/{config_type}/{config_type}.example.json"
        try:
            with open(example_path, 'r') as f:
                config = json.load(f)
                logger.warning(f"Using example configuration. Please create a proper {config_type}.json file.")
                return config
        except FileNotFoundError:
            logger.error(f"No configuration found for {config_type}. Please create a configuration file.")
            raise

def get_postgres_connection(db_name: Optional[str] = None):
    """
    Get a connection to PostgreSQL database.

    Args:
        db_name: Optional database name to override configuration

    Returns:
        PostgreSQL connection object
    """
    try:
        # load configuration
        config = load_config("database")
        pg_config = config["postgresql"]

        # override database name if provided
        if db_name:
            pg_config["database"] = db_name

        # connect to database
        connection = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"]
        )

        logger.info(f"Connected to PostgrSQL database: {pg_config['database']}")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

def get_postgres_cursor(connection, cursor_factory=RealDictCursor):
    """
    Get a cursor from a PostgreSQL connection.

    Args:
        connection: PostgreSQL connection object
        cursor_factory: Type of cursor to create

    Returns:
        PostgreSQL cursor object
    """
    return connection.cursor(cursor_factory=cursor_factory)

def get_mongodb_client():
    """
    Get a MongoDB client.
    
    Returns:
        MongoDB client object
    """
    try:
        # load configuration
        config = load_config("database")
        mongo_config = config["mongodb"]

        # connect to MongoDB
        client = pymongo.MongoClient(
            host=mongo_config["host"],
            port=int(mongo_config["port"]),
            username=mongo_config["user"],
            password=mongo_config["password"]
        )

        # test connection
        client.admin.command('ping')

        logger.info(f"Connected to MongoDB at {mongo_config['host']}:{mongo_config['port']}")
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise

def get_mongodb_db(client=None, db_name=None):
    """
    Get a MongoDB database.
    
    Args:
        client: MongoDB client object (optional, will create one if not provided)
        db_name: Name of database to connect to (optional, will use config value if not provided)
        
    Returns:
        MongoDB database object
    """
    if client is None:
        client = get_mongodb_client()

    # load configuration
    config = load_config("database")

    # use provided db_name or get from config
    db_name = db_name or config["mongodb"]["database"]

    return client[db_name]

def get_redis_connection():
    """
    Get a Redis connection.
    
    Returns:
        Redis connection object
    """
    try:
        # load configuration
        config = load_config("database")
        redis_config = config["redis"]

        # connect to Redis
        r = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            db=redis_config["db"],
            password=redis_config["password"],
            decode_responses=True
        )

        # test connection
        r.ping()

        logger.info(f"Connected to Redis at {redis_config['host']}:{redis_config['port']}")
        return r
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        raise

# example usage with context managers
class PostgresConnection:
    """Context manager for PostgreSQL connections"""

    def __init__(self, db_name=None):
        self.db_name = db_name
        self.conn = None

    def __enter__(self):
        self.conn = get_postgres_connection(self.db_name)
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

class MongoDBConnection:
    """Context manager for MongoDB connections"""
    
    def __init__(self, db_name=None):
        self.db_name = db_name
        self.client = None
        
    def __enter__(self):
        self.client = get_mongodb_client()
        return get_mongodb_db(self.client, self.db_name)
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")