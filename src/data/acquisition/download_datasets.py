# src/data/acquisition/download_datasets.py

"""
Script to download external datasets using configured connectors.
"""

import os
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from glob import glob
from typing import Dict, List, Any

from src.data.connectors.factory import ConnectorFactory
from src.utils.config_loader import load_config_with_env_vars
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_all_dataset_configs(config_dir: str) -> List[Dict[str, Any]]:
    """
    Load all dataset configuration files from a directory.
    
    Args:
        config_dir: Directory containing configuration files
        
    Returns:
        List of configuration dictionaries
    """
    configs = []
    config_files = glob(os.path.join(config_dir, "*.json"))

    for config_file in config_files:
        try:
            config = load_config_with_env_vars(config_file)
            configs.append(config)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration from {config_file}: {e}")

    return configs

def download_kaggle_datasets(configs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Download datasets using Kaggle connector configurations.
    
    Args:
        configs: List of Kaggle connector configurations
        
    Returns:
        Dictionary mapping connector names to download results
    """
    results = {}

    for config in configs:
        if config.get("type") != "kaggle":
            continue

        name = config.get("name")
        logger.info(f"Processing Kaggle dataset: {name}")

        try:
            connector = ConnectorFactory.create_connector_from_config(config)

            if connector.connect():
                download_results = connector.extract()
                results[name] = download_results

                # save meta
                metadata_dir = os.path.join("data", "metadata", "downloads")
                os.makedirs(metadata_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                metadata_path = os.path.join(metadata_dir, f"{name}_{timestamp}.json")
                connector.save_metadata(metadata_path)

                logger.info(f"Downloaded {len(download_results)} files for {name}")
            else:
                logger.error(f"Failed to connect using {name} connector")
        except Exception as e:
            logger.error(f"Error downloading {name} dataset: {e}")

    return results

def fetch_api_data(configs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch data from APIs using API connector configurations.
    
    Args:
        configs: List of API connector configurations
        
    Returns:
        Dictionary mapping connector names to API results
    """
    results = {}

    for config in configs:
        if config.get("type") != "api":
            continue

        name = config.get("name")
        logger.info(f"Processing API: {name}")

        try:
            connector = ConnectorFactory.create_connector_from_config(config)

            if connector.connect():
                api_results = {}

                # extract data from each endpoint
                for endpoint in connector.config.get("endpoints", {}).keys():
                    try:
                        df = connector.extract(endpoint=endpoint, output_format="dataframe")

                        # save to file
                        output_dir = os.path.join("data", "raw", "external", name)
                        os.makedirs(output_dir, exist_ok=True)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_path = os.path.join(output_dir, f"{endpoint}_{timestamp}.csv")

                        connector.save_data(df, output_path, "csv")
                        api_results[endpoint] = {
                            "record_count": len(df),
                            "file_path": output_path
                        }

                        logger.info(f"Fetched {len(df)} records from {endpoint} endpoint")
                    except Exception as e:
                        logger.error(f"Error fetching data from {endpoint} endpoint: {e}")

                results[name] = api_results

                # save metadata
                metadata_dir = os.path.join("data", "metadata", "downloads")
                os.makedirs(metadata_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                metadata_path = os.path.join(metadata_dir, f"{name}_{timestamp}.json")
                connector.save_metadata(metadata_path)
            else:
                logger.error(f"Failed to connect using {name} connector")
        except Exception as e:
            logger.error(f"Error processing {name} API: {e}")

    return results

def main():
    """
    Main function to download all datasets.
    """
    parser = argparse.ArgumentParser(description="Download external datasets using configured connectors")

    parser.add_argument(
        '--config-dir',
        type=str,
        default='config/data_sources',
        help='Directory containing dataset configurations'
    )

    parser.add_argument(
        '--kaggle-only',
        action='store_true',
        help='Download only Kaggle datasets'
    )

    parser.add_argument(
        '--api-only',
        action='store_true',
        help='Fetch only API data'
    )

    args = parser.parse_args()

    # ensure config directory exists
    if not os.path.isdir(args.config_dir):
        logger.error(f"Configuration directory {args.config_dir} does not exist")
        return
    
    # load all dataset configurations
    configs = load_all_dataset_configs(args.config_dir)
    logger.info(f"Loaded {len(configs)} dataset configurations")

    # process based on options
    if args.kaggle_only:
        download_kaggle_datasets(configs)
    elif args.api_only:
        fetch_api_data(configs)
    else:
        # process all types
        download_kaggle_datasets(configs)
        fetch_api_data(configs)

    logger.info("Dataseet download process completed")

if __name__ == "__main__":
    main()