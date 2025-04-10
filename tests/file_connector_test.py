# tests/file_connector_test.py

from src.data.connectors.factory import ConnectorFactory

# create a file connector for the simulated customer data
file_connector = ConnectorFactory.create_connector(
    name="customer-data",
    connector_type="file",
    config={
        "files": [
            {
                "path": "data/simulated/retail_customers_20250409_183954.csv",
                "format": "csv",
                "read_options": {"sep": ",", "encoding": "utf-8"}
            }
        ]
    }
)

# test connection (verifies files exist)
connection_successful = file_connector.connect()
print(f"Connection successful: {connection_successful}")

# extract data
if connection_successful:
    data = file_connector.extract()

    # check the first few records
    for file_path, df in data.items():
        print(f"\nFile: {file_path}")
        print(f"Record count: {len(df)}")
        print("Sample data:")
        print(df.head())

    # save metadata about this extraction
    file_connector.save_metadata("data/metadata/file_connector_test.json")