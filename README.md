# E-commerce Predictive Analytics Platform

A comprehensive analytics platform for retail and e-commerce data, featuring customer segmentation, product recommendation, and demand forecasting capabilities.

## Project Overview

This project implements a full-stack data analytics platform that ingests retail transaction data from multiple sources, transforms it into a structured format optimized for analytics, and provides machine learning models for business intelligence.

### Key Features

- **Data Integration**: Connect to various data sources including files, APIs, and Kaggle datasets
- **Data Warehouse**: Structured retail data model in PostgreSQL with complementary MongoDB collections
- **Machine Learning**: Customer segmentation, product recommendation, and demand forecasting
- **Visualization Dashboard**: Interactive analytics views for different user roles
- **Real-time Processing**: Stream processing for timely insights

## Getting Started

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Kaggle API credentials (for external datasets)
- Git

### Setup

1. **Clone the repository**

```bash
git clone <repository-url>
cd retail-analytics
```

2. **Set up environment variables**

Copy the example environment file and modify as needed:

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration settings, including database credentials and API keys.

3. **Start the database services**

```bash
docker-compose up -d
```

This will start PostgreSQL, MongoDB, Redis, and web-based database admin tools.

4. **Create a virtual environment and install dependencies**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
pip install -r requirements.txt
```

5. **Run data simulator to generate test data**

```bash
python src/data/simulation/run_simulator.py --postgres --mongodb
```

### Project Structure

```
retail-analytics/
├── data/                  # Data storage and versioning
├── notebooks/             # Jupyter notebooks for exploration
├── src/                   # Source code
│   ├── data/              # Data processing modules
│   ├── features/          # Feature engineering
│   ├── models/            # ML models
│   ├── visualization/     # Visualization modules
│   └── app/               # Web application
├── tests/                 # Unit and integration tests
├── config/                # Configuration files
├── docs/                  # Documentation
└── README.md              # This file
```

## Usage

### Data Simulation

Generate synthetic retail data for development and testing:

```bash
python src/data/simulation/run_simulator.py --output-dir data/simulated --format csv
```

### Data Connectors

Connect to external data sources using the connector framework:

```python
from src.data.connectors.factory import ConnectorFactory

# Create a file connector
file_connector = ConnectorFactory.create_connector(
    name="csv-connector",
    connector_type="file",
    config={
        "files": [{"path": "data/raw/customers.csv", "format": "csv"}]
    }
)

# Extract data
data = file_connector.extract()
```

### Database Access

Access the databases using the provided utilities:

```python
from src.data.database import PostgresConnection, MongoDBConnection

# PostgreSQL
with PostgresConnection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM retail.dim_customer LIMIT 10")
    results = cursor.fetchall()

# MongoDB
with MongoDBConnection() as db:
    customers = db.customers.find().limit(10)
```

## Development Roadmap

The project follows an 8-phase development approach:

1. **Foundation**: Project setup and data acquisition
2. **Data Engineering Pipeline**: ETL framework and data warehouse
3. **Feature Engineering**: Customer and product feature development
4. **Machine Learning Systems**: Recommendation engine and forecasting models
5. **MLOps Infrastructure**: Model management and deployment
6. **Web Application**: Dashboard and visualization
7. **Advanced Features**: Real-time analytics and experimentation
8. **Documentation**: Technical documentation and portfolio presentation

For detailed timeline, see `docs/Development Roadmap.md`.

## License

[MIT License](LICENSE)

## Acknowledgments

- Datasets used from Kaggle and other public sources
- Inspired by real-world retail analytics systems