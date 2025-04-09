# src/data/simulation/base_simulator.py

"""
Base class for data simulators that generate synthetic data.
"""

import os
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import pandas as pd
import numpy as np

from src.utils.config_loader import load_config_with_env_vars

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseSimulator:
    """
    Base class for data simulators.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None, seed: Optional[int] = None):
        """
        Initialize the simulator.
        
        Args:
            name: Name of the simulator
            config: Configuration dictionary
            seed: Random seed for reproducibility
        """
        self.name = name
        self.config = config or {}
        self.seed = seed

        # set random seed if provided
        if seed is not None:
            np.random.seed(seed)

        # metadata to track simulation runs
        self.metadata = {
            "simulator_name": name,
            "simulator_type": self.__class__.__name__,
            "simulation_id": str(uuid.uuid4()),
            "simulation_date": None,
            "seed": seed,
            "status": "initialized",
            "record_counts": {},
            "config": self.config,
            "error": None
        }

        logger.info(f"Initialized {self.__class__.__name__} with name: {name}")

    def load_config(self, config_path: str) -> None:
        """
        Load configuration from a file.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            self.config = load_config_with_env_vars(config_path)
            self.metadata["config"] = self.config
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def save_data(self, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
                  destination: str, file_format: str = "csv") -> Dict[str, str]:
        """
        Save generated data to files.
        
        Args:
            data: Data to save, either a single DataFrame or dictionary of DataFrames
            destination: Directory where to save the data
            file_format: Format to save the data (csv, json, parquet)
            
        Returns:
            Dictionary mapping data names to file paths
        """
        # create destination directory if it doesn't exist
        os.makedirs(destination, exist_ok=True)

        # generate timestamp for file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        saved_files = {}

        # handle both single DataFrame and dictionary of DataFrames
        if isinstance(data, pd.DataFrame):
            data_dict = {"data": data}
        else:
            data_dict = data

        for data_name, df in data_dict.items():
            # generate file name
            filename = f"{self.name}_{data_name}_{timestamp}.{file_format}"
            file_path = os.path.join(destination, filename)

            # save data based on format
            if file_format == "csv":
                df.to_csv(file_path, index=False)
            elif file_format == "json":
                df.to_json(file_path, orient="records", lines=True)
            elif file_format == "parquet":
                df.to_parquet(file_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            saved_files[data_name] = file_path
            logger.info(f"Saved {len(df)} records of {data_name} to {file_path}")

        return saved_files
    
    def save_metadata(self, file_path: str) -> None:
        """
        Save metadata to file.
        
        Args:
            file_path: Path to save metadata
        """
        # ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # save metadata
        with open(file_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)

        logger.info(f"Saved metadata to {file_path}")

    def generate(self):
        """
        Generate synthetic data.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement generate() method")