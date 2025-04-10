# tests/kaggle_connector_test.py

from src.data.connectors.factory import ConnectorFactory

import os
from dotenv import load_dotenv
load_dotenv()

# create a Kaggle connector for a retail dataset
kaggle_connector = ConnectorFactory.create_connector(
    name="retail-dataset",
    connector_type="kaggle",
    config={
        "username": os.environ.get('KAGGLE_USERNAME'),
        "key": os.environ.get("KAGGLE_API_KEY"),
        "datasets": [
            {
                "owner": "carrie1",
                "dataset": "ecommerce-data",
                "destination": "data/raw/external/commerce"
            }
        ]
    }
)

# test connection and authentication
connection_successful = kaggle_connector.connect()
print(f"Connection successful: {connection_successful}")

# extract data (download datasets)
if connection_successful:
    downloaded_datasets = kaggle_connector.extract()

    print(f"Downloaded {len(downloaded_datasets)} datasets")
    for ds in downloaded_datasets:
        print(f"Dataset: {ds.get('dataset')}")
        print(f"Local path: {ds.get('local_path')}")
        print(f"Status: {ds.get('status')}")
        print()