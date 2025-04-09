# src/data/connectors/api_connector.py

"""
General-purpose API connector for fetching data from REST APIs.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Union, Optional, Generator

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.data.connectors.base import DataConnector

# set up logging
logger = logging.getLogger(__name__)

class APIConnector(DataConnector):
    """
    Connector for fetching data from REST APIs.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize API connector.
        
        Args:
            name: Name of the connector
            config: Configuration dictionary with API settings
                   Expected format:
                   {
                       "base_url": "https://api.example.com/v1",
                       "endpoints": {
                           "products": "/products",
                           "customers": "/customers"
                       },
                       "auth": {
                           "type": "api_key", # or "basic", "oauth", "bearer"
                           "api_key": "your_api_key",
                           "api_key_name": "X-API-Key", # header name
                           "username": "", # for basic auth
                           "password": "", # for basic auth
                           "token": "" # for bearer auth
                       },
                       "headers": {
                           "Content-Type": "application/json",
                           "Accept": "application/json"
                       },
                       "params": {
                           "default": {
                               "limit": 100
                           },
                           "products": {
                               "category": "electronics"
                           }
                       },
                       "rate_limit": {
                           "requests_per_second": 5,
                           "max_retries": 3,
                           "retry_backoff_factor": 0.5
                       },
                       "pagination": {
                           "type": "offset", # or "cursor", "page"
                           "limit_param": "limit",
                           "offset_param": "offset", # or "cursor", "page"
                           "results_path": "data", # JSON path to results array
                           "total_count_path": "meta.total" # JSON path to total count
                       }
                   }
        """
        super().__init__(name, config)
        self.session = None
        
        # update metadata
        self.metadata.update({
            "data_source": f"api_{name}",
            "base_url": self.config.get("base_url", ""),
            "endpoints": list(self.config.get("endpoints", {}).keys())
        })

    def connect(self) -> bool:
        """
        Establish connection to the API by creating a session.
        
        Returns:
            True if session creation successful, False otherwise
        """
        try:
            # create session
            self.session = requests.Session()

            # configure retry strategy
            rate_limit = self.config.get("rate_limit", {})
            max_retries = rate_limit.get("max_retries", 3)
            backoff_factor = rate_limit.get("retry_backoff_factor", 0.5)

            retry = Retry(
                total=max_retries,
                backoff_factor=backoff_factor,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

            # set default headers
            default_headers = {
                "User-Agent": f"RetailAnalytics/{self.name}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            headers = {**default_headers, **self.config.get("headers", {})}
            self.session.headers.update(headers)

            # set up authentication
            auth = self.config.get("auth", {})
            auth_type = auth.get("type")

            if auth_type == "api_key":
                key_name = auth.get("api_key_name", "X-API-Key")
                api_key = auth.get("api_key", "")
                self.session.headers.update({key_name: api_key})

            elif auth_type == "basic":
                username = auth.get("username", "")
                password = auth.get("password", "")
                self.session.auth(username, password)

            elif auth_type == "bearer":
                token = auth.get("token", "")
                self.session.headers.update({"Authorization": f"Bearer {token}"})

            elif auth_type == "oauth":
                # for simplicity, assuming token is already obtained
                token = auth.get("token", "")
                self.session.headers.update({"Authorization": f"Bearer {token}"})

            logger.info(f"Successfully created API session for {self.name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to create API session: {e}")
            self.metadata.update({
                "status": "error",
                "error": f"Connection failed: {str(e)}"
            })
            return False
        
    def _get_url(self, endpoint_name: str) -> str:
        """
        Build full URL for the specified endpoint.
        
        Args:
            endpoint_name: Name of the endpoint as defined in config
            
        Returns:
            Full URL
        """
        base_url = self.config.get("base_url", "")
        endpoints = self.config.get("endpoints", {})
        endpoint = endpoints.get(endpoint_name, "")

        # handle case where endpoint is a full URL
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        
        # remove trailing slash for base_url if exists
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        # add leading slash to endpoint if not exists
        if endpoint and not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        return f"{base_url}{endpoint}"
    
    def _handle_pagination(self, endpoint_name: str, params: Dict[str, Any]) -> Generator:
        """
        Handle paginated API requests.
        
        Args:
            endpoint_name: Name of the endpoint
            params: Request parameters
            
        Yields:
            Data from each page of results
        """
        if not self.session and not self.connect():
            raise RuntimeError("Not connected to API")
        
        pagination = self.config.get("pagination", {})
        pagination_type = pagination.get("type", "offset")

        if pagination_type not in ["offset", "cursor", "page"]:
            raise ValueError(f"Unsupported pagination type: {pagination_type}")
        
        limit_param = pagination.get("limit_param", "limit")
        offset_param = pagination.get("offset_param",
                                      "offset" if pagination_type == "offset" else
                                      "cursor" if pagination_type == "cursor" else
                                      "page")
        
        results_path = pagination.get("results_path", "")
        total_count_path = pagination.get("total_count_path", "")

        # get limit value or use default
        limit = params.get(limit_param, 100)

        # initialize pagination variables
        if pagination_type == "offset":
            current_offset = 0
            params[offset_param] = current_offset
        elif pagination_type == "page":
            current_page = 1
            params[offset_param] = current_page
        else:   # cursor
            next_cursor = None

        total_count = None
        more_pages = True

        rate_limit = self.config.get("rate_limit", {})
        requests_per_second = rate_limit.get("requests_per_second", 5)
        min_wait = 1.0 / requests_per_second if requests_per_second > 0 else 0

        while more_pages:
            # update pagination parameter
            if pagination_type == "offset":
                params[offset_param] = current_offset
            elif pagination_type == "page":
                params[offset_param] == current_page
            elif pagination_type == "cursor" and next_cursor:
                params[offset_param] = next_cursor

            # make request
            url = self._get_url(endpoint_name)
            start_time = time.time()

            logger.info(f"Fetching page from {url} with params: {params}")
            response = self.session.get(url, params=params)

            # check for successful response
            response.raise_for_status()

            # parse response
            data = response.json()

            # extract results based on results_path
            if results_path:
                results = data
                for key in results_path.split('.'):
                    results = results.get(key, [])
            else:
                results = data

            # get total count if available
            if total_count is None and total_count_path:
                total_count_obj = data
                try:
                    for key in total_count_path.split('.'):
                        total_count_obj = total_count_obj.get(key)
                    total_count = int(total_count_obj)
                except (TypeError, ValueError):
                    logger.warning(f"Failed to extract total count from response using path: {total_count_path}")

            # yield results
            yield results

            # update pagination for next request
            if pagination_type == "offset":
                current_offset += limit
                more_pages = len(results) == limit

                # if we have total count, use it to determine if more pages
                if total_count is not None:
                    more_pages = current_offset < total_count

            elif pagination_type == "page":
                current_page += 1
                more_pages = len(results) == limit

                # if we have total count, use it to determine if more pages
                if total_count is not None:
                    more_pages = (current_page - 1) * limit < total_count

            elif pagination_type == "cursor":
                # look for next cursor in response
                next_cursor_path = pagination.get("next_cursor_path", "")
                if next_cursor_path:
                    cursor_obj = data
                    try:
                        for key in next_cursor_path.split('.'):
                            cursor_obj = cursor_obj.get(key)
                        next_cursor = cursor_obj
                        more_pages = next_cursor is not None and next_cursor != ""
                    except (TypeError, ValueError):
                        logger.warning("Failed to extract next cursor from response")
                        more_pages = False
                else:
                    more_pages = False

            # rate limiting
            elapsed = time.time() - start_time
            if elapsed < min_wait:
                time.sleep(min_wait - elapsed)

    def extract(self, **kwargs) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Extract data from the API.
        
        Args:
            **kwargs: Additional parameters:
                - endpoint: Name of the endpoint to fetch from
                - params: Additional request parameters
                - output_format: Output format ('dataframe' or 'dict', default: 'dataframe')
                - flatten: Whether to flatten nested JSON (default: False)
                
        Returns:
            Extracted data as DataFrame or list of dictionaries
        """
        if not self.session and not self.connect():
            raise RuntimeError("Not connected to API")
        
        # get parameters
        endpoint_name = kwargs.get("endpoint")
        params = kwargs.get("params", {})
        output_format = kwargs.get("output_format", "dataframe")
        flatten = kwargs.get("flatten", False)

        if not endpoint_name:
            raise ValueError("Endpoint name must be specified")
        
        # start extraction
        self.metadata.update({
            "extraction_date": datetime.now().isoformat(),
            "status": "in_progress",
            "endpoint": endpoint_name
        })

        # generate batch ID
        batch_id = self.generate_batch_id()

        # get default params from config
        default_params = self.config.get("params", {}).get("default", {})
        endpoint_params = self.config.get("params", {}).get(endpoint_name, {})

        # merge params
        merged_params = {**default_params, **endpoint_params, **params}

        try:
            # check if pagination is configured
            if "pagination" in self.config:
                # collect all pages
                all_results = []
                for page_results in self._handle_pagination(endpoint_name, merged_params):
                    all_results.extend(page_results)
                results = all_results
            else:
                # single request
                url = self._get_url(endpoint_name)
                logger.info(f"Making request to {url} with params: {merged_params}")

                response = self.session.get(url, params=merged_params)
                response.raise_for_status()

                data = response.json()

                # extract results if path specified
                results_path = self.config.get("pagination", {}).get("results_path", "")
                if results_path:
                    results = data
                    for key in results_path.split('.'):
                        results = results.get(key, [])
                else:
                    results = data

            # convert to requested format
            if output_format == "dataframe":
                if flatten:
                    # flatten nested JSON before converting to DataFrame
                    flattened_results = []
                    for item in results:
                        flattened_item = {}
                        self._flatten_json(item, flattened_item)
                        flattened_results.append(flattened_item)
                    df = pd.DataFrame(flattened_results)
                else:
                    df = pd.DataFrame(results)

                # update metadata
                self.metadata.update({
                    "status": "completed",
                    "record_count": len(df),
                    "batch_id": batch_id,
                    "columns": df.columns.tolist(),
                    "completion_time": datetime.now().isoformat()
                })

                return df
            else:
                # update metadata
                self.metadata.update({
                    "status": "completed",
                    "record_count": len(results),
                    "batch_id": batch_id,
                    "completion_time": datetime.now().isoformat()
                })

                return results
            
        except Exception as e:
            logger.error(f"Failed to extract data from API: {e}")
            self.metadata.update({
                "status": "error",
                "error": f"Extraction failed: {str(e)}",
                "batch_id": batch_id,
                "completion_time": datetime.now().isoformat()
            })
            raise

    def _flatten_json(self, nested_json: Dict[str, Any], flattened: Dict[str, Any], prefix: str = "") -> None:
        """
        Flatten nested JSON by creating dot-notation keys.
        
        Args:
            nested_json: Nested JSON object
            flattened: Output dictionary to store flattened structure
            prefix: Current key prefix
        """
        for key, value in nested_json.items():
            if prefix:
                new_key = f"{prefix}.{key}"
            else:
                new_key = key

            if isinstance(value, dict):
                self._flatten_json(value, flattened, new_key)
            elif isinstance(value, list):
                # for simplicity, convert lists to string
                # more complex handling could be added here
                flattened[new_key] = json.dumps(value)
            else:
                flattened[new_key] = value