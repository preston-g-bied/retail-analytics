# src/data/simulation/verify_simulator.py

"""
Script to verify that data simulator output matches the defined data model.
Performs validation checks on generated data to ensure quality and consistency.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype

from src.data.simulation.retail_simulator import RetailSimulator

from src.utils.config_loader import load_config_with_env_vars

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataModelVerifier:
    """
    Utility class for verifying that simulated data matches the data model.
    """

    def __init__(self, data_dictionary_path: str = "docs/data_dictionary.md"):
        """
        Initialize with data dictionary path.
        
        Args:
            data_dictionary_path: Path to the data dictionary markdown file
        """
        self.data_dictionary_path = data_dictionary_path
        self.expected_schemas = self._parse_data_dictionary()
        self.verification_results = {
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "entity_results": {}
        }

    def _parse_data_dictionary(self) -> Dict[str, Dict[str, Any]]:
        """
        Parse the data dictionary markdown file to extract expected schemas.
        
        Returns:
            Dictionary mapping entity names to their expected schemas
        """
        try:
            with open(self.data_dictionary_path, 'r') as f:
                content = f.read()

            schemas = {}

            # extract PostgreSQL table definitions
            tables = {
                "customers": "retail.dim_customer",
                "products": "retail.dim_product",
                "locations": "retail.dim_location",
                "transactions": "retail.fact_transaction",
                "transaction_items": "retail.fact_transaction_item"
            }

            for entity_name, table_name in tables.items():
                # find table schema section
                table_start = content.find(f"**PostgreSQL Schema ({table_name})**")
                if table_start == -1:
                    logger.warning(f"Could not find schema for {entity_name} in data dictionary")
                    continue

                # find table section
                table_end = content.find("###", table_start)
                if table_end == -1:
                    table_end = len(content)

                table_section = content[table_start:table_end]

                # extract columns from markdown table
                columns = []
                lines = table_section.split("\n")
                for line in lines:
                    if "|" in line and "---" not in line and "Attribute" not in line:
                        parts = [p.strip() for p in line.split("|")]
                        if len(parts) >= 6:
                            col_name = parts[1]
                            col_type = parts[2]
                            required = "required" in parts[4].lower() or "not null" in parts[4].lower()
                            columns.append({
                                "name": col_name,
                                "type": col_type,
                                "required": required
                            })

                schemas[entity_name] = {
                    "table_name": table_name,
                    "columns": columns
                }

            return schemas
        
        except Exception as e:
            logger.error(f"Error parsing data dictionary: {e}")
            return {}
        
    def verify_simulated_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Verify that simulated data matches the expected schemas.
        
        Args:
            data: Dictionary mapping entity names to DataFrames
            
        Returns:
            Verification results
        """
        if not self.expected_schemas:
            logger.error("No expected schemas found. Cannot verify data.")
            return {
                "passed": False,
                "error": "No expected schemas found"
            }
        
        all_passed = True

        for entity_name, df in data.items():
            if entity_name not in self.expected_schemas:
                logger.warning(f"No schema defined for {entity_name}, skipping verification")
                continue

            logger.info(f"Verifying {entity_name} data...")

            # verify data structure and quality
            entity_passed, entity_results = self._verify_entity(entity_name, df, data)
            self.verification_results["entity_results"][entity_name] = entity_results

            if not entity_passed:
                all_passed = False

        self.verification_results["passed"] = all_passed

        return self.verification_results
    
    def _verify_entity(self, entity_name: str, df: pd.DataFrame, data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict[str, Any]]:
        """
        Verify a single entity's DataFrame against its expected schema.
        
        Args:
            entity_name: Name of the entity
            df: DataFrame to verify
            
        Returns:
            Tuple of (passed, results)
        """
        expected_schema = self.expected_schemas[entity_name]

        # prepare results structure
        results = {
            "record_count": len(df),
            "passed": True,
            "warnings": [],
            "errors": [],
            "column_issues": {},
            "relationship_issues": [],
            "data_quality_issues": []
        }

        # 1. check if all required columns are present
        for column in expected_schema["columns"]:
            col_name = column["name"]

            if col_name not in df.columns:
                results["errors"].append(f"Required column '{col_name}' is missing")
                results["passed"] = False
                continue

            # track column-specific issues
            results["column_issues"][col_name] = []

            # check for nulls in required columns
            if column["required"] and df[col_name].isnull().any():
                null_count = df[col_name].isnull().sum()
                results["errors"].append(f"Column '{col_name}' has {null_count} null values but is required")
                results["column_issues"][col_name].apend(f"Contains {null_count} null values")
                results["passed"] = False

            # check data types - basic compatibility checks
            if "INTEGER" in column["type"].upper() or "SERIAL" in column["type"].upper():
                if not is_numeric_dtype(df[col_name]):
                    results["warnings"].append(f"Column '{col_name}' should be integer but has type {df[col_name].dtype}")
                    results["column_issues"][col_name].apend(f"Type mispatch: expected integer-like, got {df[col_name].dtype}")

            elif "DECIMAL" in column["type"].upper() or "NUMERIC" in column["type"].upper():
                if not is_numeric_dtype(df[col_name]):
                    results["warnings"].append(f"Column '{col_name}' should be decimal but has type {df[col_name].dtype}")
                    results["column_issues"][col_name].append(f"Type mismatch: expected numeric, got {df[col_name].dtype}")
            
            elif "VARCHAR" in column["type"].upper() or "TEXT" in column["type"].upper():
                if df[col_name].dtype != 'object' and not pd.api.types.is_string_dtype(df[col_name]):
                    results["warnings"].append(f"Column '{col_name}' should be string but has type {df[col_name].dtype}")
                    results["column_issues"][col_name].append(f"Type mismatch: expected string-like, got {df[col_name].dtype}")
            
            elif "DATE" in column["type"].upper() or "TIMESTAMP" in column["type"].upper():
                if not pd.api.types.is_datetime64_dtype(df[col_name]) and df[col_name].dtype != 'object':
                    results["warnings"].append(f"Column '{col_name}' should be date/timestamp but has type {df[col_name].dtype}")
                    results["column_issues"][col_name].append(f"Type mismatch: expected datetime-like, got {df[col_name].dtype}")

        # 2. check for extra columns not in schema
        extra_columns = [col for col in df.columns if col not in [c["name"] for c in expected_schema["columns"]]]
        if extra_columns:
            results["warnings"].append(f"Extra columns found: {extra_columns}")

        # 3. check for unique constraints
        # for simplicity, assume that primary keys and columns with "key" in name should be unique
        for column in expected_schema["columns"]:
            col_name = column["name"]
            if col_name in df.columns and ("_id" in col_name or "_key" in col_name):
                if not df[col_name].is_unique:
                    duplicate_count = len(df) - df[col_name].nunique()
                    results["errors"].append(f"Column '{col_name}' has {duplicate_count} duplicate values but should be unique")
                    results["column_issues"][col_name].append(f"Contains {duplicate_count} duplicate values")
                    results["passed"] = False

        # 4. check specific entity constraints and relationships
        if entity_name == "transactions":
            # check if all customer_ids exist in customers
            if "customers" in data and "customer_id" in df.columns:
                invalid_customers = df[~df["customer_id"].isin(data["customers"]["customer_id"])]
                if not invalid_customers.empty:
                    results["errors"].append(f"Found {len(invalid_customers)} transactions with invalid customer_id references")
                    results["relationship_issues"].append(f"{len(invalid_customers)} transactions with invalid customer_id references")
                    results["passed"] = False
            
            # check for negative amounts
            for amount_col in ["total_amount", "discount_amount", "tax_amount", "shipping_amount"]:
                if amount_col in df.columns:
                    negative_amounts = df[df[amount_col] < 0]
                    if not negative_amounts.empty:
                        results["warnings"].append(f"Found {len(negative_amounts)} rows with negative {amount_col}")
                        results["data_quality_issues"].append(f"{len(negative_amounts)} rows with negative {amount_col}")
        
        elif entity_name == "transaction_items":
            # check if all transaction_ids exist in transactions
            if "transactions" in data and "transaction_id" in df.columns:
                invalid_transactions = df[~df["transaction_id"].isin(data["transactions"]["transaction_id"])]
                if not invalid_transactions.empty:
                    results["errors"].append(f"Found {len(invalid_transactions)} items with invalid transaction_id references")
                    results["relationship_issues"].append(f"{len(invalid_transactions)} items with invalid transaction_id references")
                    results["passed"] = False
            
            # check if all product_ids exist in products
            if "products" in data and "product_id" in df.columns:
                invalid_products = df[~df["product_id"].isin(data["products"]["product_id"])]
                if not invalid_products.empty:
                    results["errors"].append(f"Found {len(invalid_products)} items with invalid product_id references")
                    results["relationship_issues"].append(f"{len(invalid_products)} items with invalid product_id references")
                    results["passed"] = False
            
            # check for negative quantities
            if "quantity" in df.columns:
                negative_quantities = df[df["quantity"] <= 0]
                if not negative_quantities.empty:
                    results["errors"].append(f"Found {len(negative_quantities)} items with zero or negative quantity")
                    results["data_quality_issues"].append(f"{len(negative_quantities)} items with zero or negative quantity")
                    results["passed"] = False
        
        # remove empty column issues
        results["column_issues"] = {k: v for k, v in results["column_issues"].items() if v}

        # overall result
        if results["errors"]:
            results["passed"] = False

        if results["passed"]:
            if results["warnings"]:
                logger.info(f"Verification of {entity_name} passed with {len(results['warnings'])} warnings")
            else:
                logger.info(f"Verification of {entity_name} passed with no issues")

        else:
            logger.warning(f"Verification of {entity_name} failed with {len(results['errors'])} errors")

        return results["passed"], results
    
def verify_simulator_output(output_dir: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Verify simulator output files in a directory.
    
    Args:
        output_dir: Directory containing simulator output files
        config_path: Optional path to simulator configuration
        
    Returns:
        Verification results
    """
    # find the latest simulator output files
    entity_files = {
        "customers": None,
        "products": None,
        "locations": None,
        "transactions": None,
        "transaction_items": None
    }

    # look for each entity's most recent file
    for entity in entity_files.keys():
        pattern = f"retail_{entity}_*.csv"
        matching_files = sorted(
            [f for f in os.listdir(output_dir) if f.startswith(f"retail_{entity}_")],
            reverse=True
        )

        if matching_files:
            entity_files[entity] = os.path.join(output_dir, matching_files[0])

    # check if all required files were found
    missing_files = [entity for entity, file_path in entity_files.items() if file_path is None]
    if missing_files:
        logger.error(f"Missing simulator output files for {', '.join(missing_files)}")
        return {
            "passed": False,
            "error": f"Missing simulator output files for: {', '.join(missing_files)}"
        }
    
    # load data from files
    data = {}
    for entity, file_path in entity_files.items():
        try:
            data[entity] = pd.read_csv(file_path)
            logger.info(f"Loaded {len(data[entity])} records from {file_path}")
        except Exception as e:
            logger.error(f"Error loading {entity} data from {file_path}: {e}")
            return {
                "passed": False,
                "error": f"Error loading {entity} data: {str(e)}"
            }
        
    # verify data against model
    verifier = DataModelVerifier()
    verification_results = verifier.verify_simulated_data(data)

    # if we have config, also verify against configuration
    if config_path and os.path.isfile(config_path):
        try:
            config = load_config_with_env_vars(config_path)

            # add config verification results
            verification_results["config_verification"] = verify_against_config(data, config)
        except Exception as e:
            logger.error(f"Error verifying against config: {e}")
            verification_results["config_verification"] = {
                "passed": False,
                "error": str(e)
            }

    return verification_results

def verify_against_config(data: Dict[str, pd.DataFrame], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Verify that simulated data matches configuration parameters.
    
    Args:
        data: Dictionary mapping entity names to DataFrames
        config: Simulator configuration
        
    Returns:
        Verification results
    """
    results = {
        "passed": True,
        "checks": []
    }

    # check number of customers
    if "customers" in data and "num_customers" in config:
        expected = config["num_customers"]
        actual = len(data["customers"])
        check = {
            "check": "num_customers",
            "expected": expected,
            "actual": actual,
            "passed": abs(expected - actual) <= max(0.05 * expected, 5)  # allow 5% or 5 record difference
        }
        results["checks"].append(check)
        if not check["passed"]:
            results["passed"] = False

    # check number of products
    if "products" in data and "num_products" in config:
        expected = config["num_products"]
        actual = len(data["products"])
        check = {
            "check": "num_products",
            "expected": expected,
            "actual": actual,
            "passed": abs(expected - actual) <= max(0.05 * expected, 5)  # allow 5% or 5 record difference
        }
        results["checks"].append(check)
        if not check["passed"]:
            results["passed"] = False

    # check number of locations
    if "locations" in data and "num_locations" in config:
        expected = config["num_locations"]
        actual = len(data["locations"])
        check = {
            "check": "num_locations",
            "expected": expected,
            "actual": actual,
            "passed": abs(expected - actual) <= max(0.05 * expected, 5)  # allow 5% or 5 record difference
        }
        results["checks"].append(check)
        if not check["passed"]:
            results["passed"] = False

    # check price ranges
    if "products" in data and "transaction_params" in config and "price_range" in config["transaction_params"]:
        price_range = config["transaction_params"]["price_range"]
        min_price = price_range.get("min", 0)
        max_price = price_range.get("max", float('inf'))
        
        actual_min = data["products"]["unit_price"].min()
        actual_max = data["products"]["unit_price"].max()
        
        check = {
            "check": "price_range",
            "expected": f"{min_price} - {max_price}",
            "actual": f"{actual_min} - {actual_max}",
            "passed": actual_min >= min_price and actual_max <= max_price
        }
        results["checks"].append(check)
        if not check["passed"]:
            results["passed"] = False
    
    # check date range
    if "transactions" in data and "time_range" in config:
        # parse dates from config
        try:
            start_date = pd.to_datetime(config["time_range"]["start"])
            end_date = pd.to_datetime(config["time_range"]["end"])
            
            # ensure created_at is datetime
            if "created_at" in data["transactions"].columns:
                created_at = pd.to_datetime(data["transactions"]["created_at"])
                
                actual_start = created_at.min()
                actual_end = created_at.max()
                
                check = {
                    "check": "transaction_date_range",
                    "expected": f"{start_date} - {end_date}",
                    "actual": f"{actual_start} - {actual_end}",
                    "passed": actual_start >= start_date and actual_end <= end_date
                }
                results["checks"].append(check)
                if not check["passed"]:
                    results["passed"] = False
        except Exception as e:
            logger.warning(f"Error checking date range: {e}")
    
    return results

def main():
    """
    Main function to verify simulator output.
    """
    parser = argparse.ArgumentParser(description="Verify data simulator output")

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/simulated',
        help='Directory containing simulator output files'
    )

    parser.add_argument(
        '--config',
        type=str,
        help='Path to simulator configuration file'
    )

    parser.add_argument(
        '--results-file',
        type=str,
        default='tests/results/verification_results.json',
        help='Path to save verification results'
    )

    parser.add_argument(
        '--run_simulator',
        action='store_true',
        help='Run simulator before verification'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for simulator'
    )

    args = parser.parse_args()

    # run simulator if requested
    if args.run_simulator:
        logger.info("Running data simulator...")

        simulator_config = None
        if args.config and os.path.isfile(args.config):
            try:
                simulator_config = load_config_with_env_vars(args.config)
            except Exception as e:
                logger.error(f"Error loading simulator config: {e}")
                return
        
        simulator = RetailSimulator("retail", config=simulator_config, seed=args.seed)
        data = simulator.generate()

        # save output
        os.makedirs(args.output_dir, exist_ok=True)
        saved_files = simulator.save_data(data, args.output_dir, "csv")

        logger.info(f"Simulator output saved to {args.output_dir}")

    # verify simulator output
    verification_results = verify_simulator_output(args.output_dir, args.config)

    # print summary
    if verification_results.get("passed", False):
        logger.info("Simulator output verifications PASSED")
    else:
        logger.warning("Simulator output verification FAILED")
        if "error" in verification_results:
            logger.error(f"Error: {verification_results['error']}")

    # save results
    with open(args.result_file, 'w') as f:
        json.dump(verification_results, f, indent=2)

    logger.info(f"Verification results saved to {args.results_file}")

    # exit with appropriate code
    if verification_results.get("passed", False):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()