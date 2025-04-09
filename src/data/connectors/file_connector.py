# src/data/connectors/file_connector.py

"""
File connector for reading local CSV, JSON, Excel, and other file formats.
"""

import os
import glob
import logging
from datetime import datetime
from typing import Dict, List, Any, Union, Optional, Tuple

import pandas as pd

from src.data.connectors.base import DataConnector

# set up logging
logger = logging.getLogger(__name__)

class FileConnector(DataConnector):
    """
    Connector for reading data from local files.
    Supports CSV, JSON, Excel, Parquet, and other formats supported by pandas.
    """
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize file connector.
        
        Args:
            name: Name of the connector
            config: Configuration dictionary
                   Expected format:
                   {
                       "files": [
                           {
                               "path": "path/to/file.csv",
                               "format": "csv", # csv, json, excel, parquet
                               "read_options": {
                                   "sep": ",",
                                   "encoding": "utf-8"
                               }
                           }
                       ],
                       "directories": [
                           {
                               "path": "path/to/directory",
                               "pattern": "*.csv",
                               "format": "csv",
                               "read_options": {
                                   "sep": ",",
                                   "encoding": "utf-8"
                               }
                           }
                       ]
                   }
        """
        super().__init__(name, config)

        # update metadata
        self.metadata.update({
            "data_source": "local_files",
            "files": self.config.get("files", []),
            "directories": self.config.get("directories", [])
        })

    def connect(self) -> bool:
        """
        Verify that specified files and directories exist.
        
        Returns:
            True if all paths exist, False otherwise
        """
        files = self.config.get("files", [])
        directories = self.config.get("directories", [])

        missing_files = []
        missing_dirs = []

        # check files
        for file_info in files:
            file_path = file_info.get("path")
            if file_path and not os.path.isfile(file_path):
                missing_files.append(file_path)

        # check directories
        for dir_info in directories:
            dir_path = dir_info.get("path")
            if dir_path and not os.path.isdir(dir_path):
                missing_dirs.append(dir_path)

        if missing_files or missing_dirs:
            logger.warning(f"Missing files: {missing_files}")
            logger.warning(f"Missing directories: {missing_dirs}")
            self.metadata.update({
                "status": "warning",
                "missing_files": missing_files,
                "missing_directories": missing_dirs
            })
            return False
        
        logger.info("All specified files and directories exist")
        return True
    
    def _get_matching_files(self) -> List[Tuple[str, str, Dict[str, Any]]]:
        """
        Get list of files matching the configuration.
        
        Returns:
            List of tuples (file_path, format, read_options)
        """
        matching_files = []

        # add explicitly specified files
        for file_info in self.config.get("files", []):
            file_path = file_info.get("path")
            file_format = file_info.get("format") or self._detect_format(file_path)
            read_options = file_info.get("read_options", {})

            if file_path and os.path.isfile(file_path):
                matching_files.append((file_path, file_format, read_options))

        # add files from directories
        for dir_info in self.config.get("directories", []):
            dir_path = dir_info.get("path")
            pattern = dir_info.get("pattern", "*.*")
            dir_format = dir_info.get("format")
            read_options = dir_info.get("read_options", {})

            if dir_path and os.path.isdir(dir_path):
                # get matching files
                glob_pattern = os.path.join(dir_path, pattern)
                for file_path in glob.glob(glob_pattern):
                    file_format = dir_format or self._detect_format(file_path)
                    matching_files.append((file_path, file_format, read_options))

        return matching_files

    def _detect_format(self, file_path: str) -> str:
        """
        Detect file format from file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected format
        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        if ext == '.csv':
            return 'csv'
        elif ext == '.json':
            return 'json'
        elif ext in ['.xls', '.xlsx', '.xlsm']:
            return 'excel'
        elif ext == '.parquet':
            return 'parquet'
        elif ext == '.pickle' or ext == '.pkl':
            return 'pickle'
        elif ext == '.hdf' or ext == '.h5':
            return 'hdf'
        elif ext == '.feather':
            return 'feather'
        elif ext == '.txt':
            return 'csv'  # assuming text files are CSV-like
        else:
            logger.warning(f"Unknown file extension for {file_path}, will attempt to infer format")
            return 'csv'  # default to CSV
        
    def _read_file(self, file_path: str, file_format: str, read_options: Dict[str, Any]) -> pd.DataFrame:
        """
        Read file into pandas DataFrame.
        
        Args:
            file_path: Path to the file
            file_format: Format of the file
            read_options: Additional read options
            
        Returns:
            DataFrame containing file data
        """
        logger.info(f"Reading {file_format} file: {file_path}")
        
        try:
            if file_format == 'csv':
                return pd.read_csv(file_path, **read_options)
            elif file_format == 'json':
                return pd.read_json(file_path, **read_options)
            elif file_format == 'excel':
                return pd.read_excel(file_path, **read_options)
            elif file_format == 'parquet':
                return pd.read_parquet(file_path, **read_options)
            elif file_format == 'pickle':
                return pd.read_pickle(file_path, **read_options)
            elif file_format == 'hdf':
                key = read_options.pop('key', None)
                return pd.read_hdf(file_path, key=key, **read_options)
            elif file_format == 'feather':
                return pd.read_feather(file_path, **read_options)
            else:
                logger.warning(f"Unsupported format: {file_format}, trying as CSV")
                return pd.read_csv(file_path, **read_options)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise

    def extract(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Extract data from files.
        
        Args:
            **kwargs: Additional parameters:
                - files: List of file specifications to override config
                - directories: List of directory specifications to override config
                
        Returns:
            Dictionary mapping file paths to DataFrames
        """
        # override config if parameters provided
        if "files" in kwargs:
            self.config["files"] = kwargs["files"]
        if "directories" in kwargs:
            self.config["directories"] = kwargs["directories"]

        # start extraction
        self.metadata.update({
            "extraction_date": datetime.now().isoformat(),
            "status": "in_progress"
        })

        # generate batch ID
        batch_id = self.generate_batch_id()

        # get matching files
        matching_files = self._get_matching_files()

        if not matching_files:
            logger.warning("No matching files found")
            self.metadata.update({
                "status": "warning",
                "warning": "No matching files found",
                "batch_id": batch_id,
                "completion_time": datetime.now().isoformat()
            })
            return {}
        
        # read files
        results = {}
        total_records = 0
        success_count = 0
        failed_files = []

        for file_path, file_format, read_options in matching_files:
            try:
                df = self._read_file(file_path, file_format, read_options)
                results[file_path] = df
                total_records += len(df)
                success_count += 1

                logger.info(f"Successfully read {len(df)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                failed_files.append({"file": file_path, "error": str(e)})

        # update metadata
        self.metadata.update({
            "status": "completed" if not failed_files else "completed_with_errors",
            "record_count": total_records,
            "batch_id": batch_id,
            "processed_files": success_count,
            "failed_files": failed_files,
            "completion_time": datetime.now().isoformat()
        })

        return results