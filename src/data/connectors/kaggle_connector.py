# src/data/connectors/kaggle_connector.py

"""
Kaggle connector for downloading datasets from Kaggle.
"""

import os
import json
import logging
import zipfile
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from src.data.connectors.base import DataConnector

# set up logging
logger = logging.getLogger(__name__)

class KaggleConnector(DataConnector):
    """
    Connector for downloading datasets from Kaggle.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Kaggle connector.
        
        Args:
            name: Name of the connector
            config: Configuration containing Kaggle credentials and dataset information
                   Expected format:
                   {
                       "username": "kaggle_username",
                       "key": "kaggle_api_key",
                       "datasets": [
                           {
                               "owner": "dataset_owner",
                               "dataset": "dataset_name",
                               "file_name": "optional_specific_file.csv",
                               "destination": "path/to/save/data"
                           }
                       ]
                   }
        """
        super().__init__(name, config)
        self.api = KaggleApi()
        self.authenticated = False

        # update metadata
        self.metadata.update({
            "data_source": "kaggle",
            "datasets": self.config.get("datasets", [])
        })

    def connect(self) -> bool:
        """
        Authenticate with Kaggle API.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            # check if credentials are provided in config
            if "username" in self.config and "key" in self.config:
                # create kaggle.json file
                kaggle_dir = os.path.join(os.path.expanduser("~"), ".kaggle")
                os.makedirs(kaggle_dir, exist_ok=True)

                with open(os.path.join(kaggle_dir, "kaggle.json"), "w") as f:
                    json.dump({
                        "username": self.config["username"],
                        "key": self.config["key"]
                    }, f)

                # set permissions
                os.chmod(os.path.join(kaggle_dir, "kaggle.json"), 0o600)

                logger.info("Created Kaggle credentials file")

            # authenticate
            self.api.authenticate()
            self.authenticated = True

            logger.info("Successfully authenticated with Kaggle API")
            return True
        except Exception as e:
            logger.error(f"Failed to authenticate with Kaggle API: {e}")
            self.metadata.update({
                "status": "error",
                "error": f"Authentication failed: {str(e)}"
            })
            return False
        
    def extract(self, **kwargs) -> List[Dict[str, str]]:
        """
        Download datasets from Kaggle.
        
        Args:
            **kwargs: Optional parameters:
                - datasets: List of dataset specifications to override config
                - unzip: Whether to unzip downloaded files (default: True)
                
        Returns:
            List of dictionaries with dataset info and local file paths
        """
        if not self.authenticated and not self.connect():
            raise RuntimeError("Not authenticated with Kaggle API")

        # get parameters
        datasets = kwargs.get("datasets") or self.config.get("datasets", [])
        unzip = kwargs.get("unzip", True)

        if not datasets:
            logger.warning("No datasets specified for download")
            return []
        
        # start extraction
        self.metadata.update({
            "extraction_date": datetime.now().isoformat(),
            "status": "in_progress"
        })

        # generate batch ID
        batch_id = self.generate_batch_id()

        # track downloaded datasets
        downloaded_datasets = []

        for dataset_info in datasets:
            owner = dataset_info.get("owner")
            dataset = dataset_info.get("dataset")
            file_name = dataset_info.get("file_name")
            destination = dataset_info.get("destination", "data/raw/external")

            if not owner or not dataset:
                logger.warning(f"Missing owner or dataset name in {dataset_info}")
                continue

            dataset_path = f"{owner}/{dataset}"

            try:
                logger.info(f"Downloading dataset: {dataset_path}")

                # create temp directory for download
                with tempfile.TemporaryDirectory() as temp_dir:
                    if file_name:
                        # download specific file
                        self.api.dataset_download_file(
                            dataset_path,
                            file_name=file_name,
                            path=temp_dir
                        )
                        download_path = os.path.join(temp_dir, file_name)
                    else:
                        # download entire dataset
                        self.api.dataset_download_files(
                            dataset_path,
                            path=temp_dir,
                            unzip=False
                        )
                        # dataset is downloaded as a zip file
                        download_path = os.path.join(temp_dir, f"{dataset}.zip")

                    # create destination directory if it doesn't exist
                    os.makedirs(destination, exist_ok=True)

                    # process the downloaded file(s)
                    if unzip and download_path.endswith(".zip"):
                        # extract files from zip
                        extracted_files = []
                        with zipfile.ZipFile(download_path, 'r') as zip_ref:
                            # list of files in the zip
                            file_list = zip_ref.namelist()

                            # extract all files
                            zip_ref.extractall(destination)

                            # track extracted files
                            for file in file_list:
                                extracted_path = os.path.join(destination, file)
                                extracted_files.append({
                                    "file_name": file,
                                    "local_path": extracted_path,
                                    "dataset": dataset_path
                                })

                        downloaded_datasets.extend(extracted_files)
                        logger.info(f"Extracted {len(extracted_files)} files from {dataset_path} to {destination}")
                    else:
                        # copy the file to destination
                        file_name = os.path.basename(download_path)
                        dest_path = os.path.join(destination, file_name)

                        # use pandas to read and save if it's a recognized format
                        file_ext = os.path.splitext(file_name)[1].lower()

                        if file_ext in ['.csv', '.json', '.xlsx', '.parquet']:
                            if file_ext == '.csv':
                                df = pd.read_csv(download_path)
                                df.to_csv(dest_path, index=False)
                            elif file_ext == '.json':
                                df = pd.read_json(download_path)
                                df.to_json(dest_path)
                            elif file_ext == '.xlsx':
                                df = pd.read_excel(download_path)
                                df.to_excel(dest_path, index=False)
                            elif file_ext == '.parquet':
                                df = pd.read_parquet(download_path)
                                df.to_parquet(dest_path, index=False)

                            downloaded_datasets.append({
                                "file_name": file_name,
                                "local_path": dest_path,
                                "dataset": dataset_path,
                                "record_count": len(df)
                            })
                        else:
                            # binary copy for other file types
                            with open(download_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
                                dest_file.write(src_file.read())

                            downloaded_datasets.append({
                                "file_name": file_name,
                                "local_path": dest_path,
                                "dataset": dataset_path
                            })

                logger.info(f"Successfully downloaded dataset: {dataset_path}")
            
            except Exception as e:
                logger.error(f"Failed to download dataset {dataset_path}: {e}")
                downloaded_datasets.append({
                    "dataset": dataset_path,
                    "error": str(e),
                    "status": "failed"
                })

        # update metadata
        total_records = sum(ds.get("record_count", 0) for ds in downloaded_datasets if "record_count" in ds)
        failed_count = sum(1 for ds in downloaded_datasets if ds.get("status") == "failed")

        self.metadata.update({
            "status": "completed" if failed_count == 0 else "completed_with_errors",
            "record_count": total_records,
            "batch_id": batch_id,
            "downloaded_files": len(downloaded_datasets),
            "failed_downloads": failed_count,
            "completion_time": datetime.now().isoformat()
        })

        # save dataset info with batch_id
        for ds in downloaded_datasets:
            if "status" not in ds:
                ds["status"] = "success"
            ds["batch_id"] = batch_id
        
        return downloaded_datasets
    
    def list_datasets(self, search_term: str = None, author: str = None) -> List[Dict[str, Any]]:
        """
        Search for datasets on Kaggle.
        
        Args:
            search_term: Optional search term
            author: Optional author name
            
        Returns:
            List of matching datasets
        """
        if not self.authenticated and not self.connect():
            raise RuntimeError("Not authenticated with Kaggle API")
        
        try:
            datasets = self.api.dataset_list(search=search_term, user=author)
            return [
                {
                    "ref": f"{ds.ref}",
                    "title": ds.title,
                    "size": ds.size,
                    "lastUpdated": ds.lastUpdated,
                    "downloadCount": ds.downloadCount,
                    "voteCount": ds.voteCount,
                    "usabilityRating": ds.usabilityRating
                }
                for ds in datasets
            ]
        except Exception as e:
            logger.error(f"Failed to list datasets: {e}")
            return []