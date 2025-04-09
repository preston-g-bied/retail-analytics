# src/data/connectors/base.py

"""
Base connector interface for data acquisition
"""

from abc import ABC, abstractmethod
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Generator
import os
import json
import hashlib
import pandas as pd

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataConnector(ABC):
    """
    Abstract base class for all data connectors.
    Defines interface for data extraction from various sources.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the data connector.
        
        Args:
            name: Name of the connector
            config: Configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self.metadata = {
            "connector_name": name,
            "connector_type": self.__class__.__name__,
            "extraction_date": None,
            "data_source": None,
            "record_count": 0,
            "status": "initialized",
            "error": None
        }
        logger.info(f"Initialized {self.__class__.__name__} with name: {name}")

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def extract(self, **kwargs) -> Union[pd.DataFrame, List[Dict[str, Any]], Generator]:
        """
        Extract data from the source.
        
        Args:
            **kwargs: Additional parameters for extraction
            
        Returns:
            Extracted data as DataFrame, list of dictionaries, or generator
        """
        pass

    def save_data(self, data: Union[pd.DataFrame, List[Dict[str, Any]]],
                  destination: str, file_format: str = "csv") -> str:
        """
        Save extracted data to disk.
        
        Args:
            data: Data to save
            destination: Path where to save the data
            file_format: Format to save the data (csv, json, parquet)
            
        Returns:
            Path to the saved file
        """
        # create destination directory if it doesn't exist
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        # generate timestamp for file name if destination is a directory
        if os.path.isdir(destination):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.name}_{timestamp}.{file_format}"
            destination = os.path.join(destination, filename)

        # save data based on its type and desired format
        if isinstance(data, pd.DataFrame):
            if file_format == "csv":
                data.to_csv(destination, index=False)
            elif file_format == "json":
                data.to_json(destination, orient="records", lines=True)
            elif file_format == "parquet":
                data.to_parquet(destination, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        elif isinstance(data, list):
            if file_format == "json":
                with open(destination, 'w') as f:
                    json.dump(data, f, indent=2)
            elif file_format == "csv":
                pd.DataFrame(data).to_csv(destination, index=False)
            else:
                raise ValueError(f"Unsupported file format for list data: {file_format}")
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")
        
        logger.info(f"Saved {len(data) if hasattr(data, '__len__') else 'unknown number of'} records to {destination}")
        return destination
    
    def generate_batch_id(self) -> str:
        """
        Generate a unique batch ID for tracking the data extraction.
        
        Returns:
            Unique batch ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_string = f"{self.name}_{timestamp}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def update_metadata(self, **kwargs) -> None:
        """
        Update metadata with additional information.
        
        Args:
            **kwargs: Key-value pairs to update metadata
        """
        self.metadata.update(kwargs)

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get current metadata.
        
        Returns:
            Metadata dictionary
        """
        return self.metadata
    
    def save_metadata(self, file_path: str) -> None:
        """
        Save metadata to file.
        
        Args:
            file_path: Path to save metadata
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)
        logger.info(f"Saved metadata to {file_path}")