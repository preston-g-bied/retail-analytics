# src/data/connectors/factory.py

"""
Factory for creating and managing data connectors.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List, Type

from src.data.connectors.base import DataConnector
from src.data.connectors.kaggle_connector import KaggleConnector
from src.data.connectors.api_connector import APIConnector
from src.data.connectors.file_connector import FileConnector

from src.utils.config_loader import load_config_with_env_vars

# set up logging
logger = logging.getLogger(__name__)

class ConnectorFactory:
    """
    Factory for creating and managing data connectors.
    """

    # mapping of connector types to connector classes
    CONNECTOR_TYPES = {
        "kaggle": KaggleConnector,
        "api": APIConnector,
        "file": FileConnector
    }

    @classmethod
    def register_connector_type(cls, type_name: str, connector_class: Type[DataConnector]) -> None:
        """
        Register a new connector type.
        
        Args:
            type_name: Name of the connector type
            connector_class: Connector class
        """
        cls.CONNECTOR_TYPES[type_name] = connector_class
        logger.info(f"Registered connector type: {type_name}")

    @classmethod
    def create_connector(cls, name: str, connector_type: str, config: Optional[Dict[str, Any]] = None) -> DataConnector:
        """
        Create a connector of the specified type.
        
        Args:
            name: Name for the connector instance
            connector_type: Type of connector to create
            config: Configuration for the connector
            
        Returns:
            Connector instance
            
        Raises:
            ValueError: If connector type is not supported
        """
        if connector_type not in cls.CONNECTOR_TYPES:
            supported_types = list(cls.CONNECTOR_TYPES.keys())
            raise ValueError(f"Unsupported connector type: {connector_type}. Supported types: {supported_types}")
        
        connector_class = cls.CONNECTOR_TYPES[connector_type]
        connector = connector_class(name, config)

        logger.info(f"Created {connector_type} connector with name: {name}")

        return connector
    
    @classmethod
    def create_connector_from_config(cls, config_path: str) -> DataConnector:
        """
        Create a connector from a configuration file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Connector instance
            
        Raises:
            FileNotFoundError: If config file not found
            ValueError: If config is invalid
        """
        try:
            config = load_config_with_env_vars(config_path)

            name = config.get("name")
            connector_type = config.get("type")
            connector_config = config.get("config", {})

            if not name or not connector_type:
                raise ValueError("Configuration must include 'name' and 'type'")
            
            return cls.create_connector(name, connector_type, connector_config)
        
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in configuration file: {config_path}")
            raise ValueError(f"Invalid JSON in configuration file: {config_path}")
        
    @classmethod
    def create_connectors_from_directory(cls, config_dir: str) -> Dict[str, DataConnector]:
        """
        Create connectors from all JSON configuration files in a directory.
        
        Args:
            config_dir: Directory containing connector configuration files
            
        Returns:
            Dictionary mapping connector names to connector instances
        """
        connectors = {}

        if not os.path.isdir(config_dir):
            logger.warning(f"Configuration directory not found: {config_dir}")
            return connectors
        
        for filename in os.listdir(config_dir):
            if filename.endswith('.json'):
                config_path = os.path.join(config_dir, filename)
                try:
                    connector = cls.create_connector_from_config(config_path)
                    connectors[connector.name] = connector
                except Exception as e:
                    logger.error(f"Error creating connector from {config_path}: {e}")

        logger.info(f"Created {len(connectors)} connectors from directory: {config_dir}")
        return connectors