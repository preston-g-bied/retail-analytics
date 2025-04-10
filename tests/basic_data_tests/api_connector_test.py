# tests/basic_data_tests/api_connector_test.py

from src.data.connectors.factory import ConnectorFactory

# create an API connector for a public e-commerce API
api_connector = ConnectorFactory.create_connector(
    name="product-api",
    connector_type="api",
    config={
        "base_url": "https://fakestoreapi.com",
        "endpoints": {
            "products": "/products",
            "categories": "/products/categories"
        },
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        "pagination": {
            "type": "offset",
            "limit_param": "limit",
            "offset_patam": "offset"
        }
    }
)

# test connection
connection_successful = api_connector.connect()
print(f"Connection successful: {connection_successful}")

# extract product data
if connection_successful:
    # get products
    products_df = api_connector.extract(endpoint="products", output_format="dataframe")
    print(f"Retrieved {len(products_df)} products")
    print(products_df.head())

    # get categories
    categories_df = api_connector.extract(endpoint="categories", output_format="dataframe")
    print(f"Retrieved {len(categories_df)} categories")
    print(categories_df)

    # save product data to a file
    output_path = api_connector.save_data(products_df, "data/raw/external/products.csv", "csv")
    print(f"Saved product data to {output_path}")