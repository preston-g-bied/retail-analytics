# Retail Analytics Platform Data Dictionary

This document provides detailed information about the data entities, their attributes, and relationships in the Retail Analytics Platform. This data dictionary is essential for understanding the data model and ensuring consistent use of data across the platform.

## Core Entities

The data model is built around four primary entities:

1. **Customers**: Individuals who make purchases
2. **Products**: Items available for purchase
3. **Transactions**: Records of purchases
4. **Locations**: Physical or virtual places where transactions occur

## Database Organization

The data is stored in multiple databases:

- **PostgreSQL**: Houses structured relational data in a star schema for analytics
- **MongoDB**: Stores semi-structured data like product details, customer profiles, and browsing history
- **Redis**: Caches frequently accessed data and supports real-time features

## Entity Definitions

### Customer

Represents an individual who has made at least one purchase or created an account.

**PostgreSQL Schema (retail.dim_customer)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| customer_id | INTEGER | Primary key | 1, 2, 3 | Auto-increment |
| customer_key | VARCHAR(50) | Natural key from source system | 'CUST-001', UUID | Unique |
| first_name | VARCHAR(100) | Customer's first name | 'John', 'Mary' | |
| last_name | VARCHAR(100) | Customer's last name | 'Smith', 'Johnson' | |
| email | VARCHAR(255) | Customer's email address | 'john.smith@example.com' | Unique |
| phone | VARCHAR(20) | Customer's phone number | '+1-555-123-4567' | |
| created_at | TIMESTAMP | Account creation date | '2023-01-15 13:45:30' | |
| updated_at | TIMESTAMP | Last update timestamp | '2023-06-22 09:12:45' | |
| is_active | BOOLEAN | Whether customer is active | TRUE, FALSE | Default: TRUE |

**MongoDB Schema (customers collection)**

Contains extended customer information including:

```json
{
  "customer_key": "CUST-001",
  "first_name": "John",
  "last_name": "Smith",
  "email": "john.smith@example.com",
  "contact": {
    "phone": "+1-555-123-4567",
    "alternative_email": "j.smith@workmail.com"
  },
  "addresses": [
    {
      "type": "billing",
      "is_default": true,
      "address_line1": "123 Main St",
      "address_line2": "Apt 4B",
      "city": "New York",
      "state": "NY",
      "postal_code": "10001",
      "country": "USA"
    },
    {
      "type": "shipping",
      "is_default": true,
      "address_line1": "123 Main St",
      "address_line2": "Apt 4B",
      "city": "New York",
      "state": "NY",
      "postal_code": "10001",
      "country": "USA"
    }
  ],
  "preferences": {
    "communication_preferences": {
      "email_subscribed": true,
      "sms_subscribed": false
    },
    "favorite_categories": ["electronics", "books"]
  },
  "demographics": {
    "birth_date": "1985-05-12T00:00:00Z",
    "gender": "male",
    "income_bracket": "75k-100k"
  },
  "created_at": "2023-01-15T13:45:30Z",
  "updated_at": "2023-06-22T09:12:45Z",
  "source": "web_registration",
  "is_active": true
}
```

### Product

Represents an item that can be purchased by a customer.

**PostgreSQL Schema (retail.dim_product)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| product_id | INTEGER | Primary key | 1, 2, 3 | Auto-increment |
| product_key | VARCHAR(50) | Natural key from source system | 'PROD-001', UUID | Unique |
| product_name | VARCHAR(255) | Name of the product | 'Wireless Headphones' | Required |
| description | TEXT | Product description | 'Noise-canceling wireless...' | |
| category | VARCHAR(100) | Main product category | 'Electronics', 'Clothing' | |
| subcategory | VARCHAR(100) | Product subcategory | 'Headphones', 'T-shirts' | |
| brand | VARCHAR(100) | Product brand | 'Sony', 'Nike' | |
| supplier | VARCHAR(100) | Product supplier | 'Electronics Wholesale Inc' | |
| unit_price | DECIMAL(10,2) | Current selling price | 99.99, 25.50 | Required |
| cost_price | DECIMAL(10,2) | Cost to the business | 65.00, 12.75 | |
| created_at | TIMESTAMP | When product was added | '2023-01-10 09:30:00' | |
| updated_at | TIMESTAMP | Last update timestamp | '2023-06-15 14:22:18' | |
| is_active | BOOLEAN | Whether product is active | TRUE, FALSE | Default: TRUE |

**MongoDB Schema (products collection)**

Contains extended product information including:

```json
{
  "product_key": "PROD-001",
  "name": "Wireless Headphones",
  "description": "Noise-canceling wireless headphones with 20-hour battery life",
  "category": "Electronics",
  "subcategory": "Headphones",
  "brand": "Sony",
  "attributes": {
    "color": "Black",
    "connectivity": "Bluetooth 5.0",
    "battery_life": "20 hours",
    "weight": "250g",
    "noise_canceling": true
  },
  "images": [
    "https://example.com/images/headphones_main.jpg",
    "https://example.com/images/headphones_side.jpg"
  ],
  "price": {
    "current": 99.99,
    "currency": "USD",
    "history": [
      {
        "price": 129.99,
        "effective_date": "2023-01-10T00:00:00Z"
      },
      {
        "price": 99.99,
        "effective_date": "2023-03-15T00:00:00Z"
      }
    ]
  },
  "inventory": {
    "quantity": 45,
    "warehouse_location": "NYC-WH3"
  },
  "reviews": [
    {
      "user_id": "CUST-057",
      "rating": 5,
      "review_text": "Great sound quality and comfortable to wear for long periods.",
      "review_date": "2023-02-20T14:35:12Z",
      "helpful_votes": 12
    }
  ],
  "created_at": "2023-01-10T09:30:00Z",
  "updated_at": "2023-06-15T14:22:18Z",
  "source": "product_catalog"
}
```

### Transaction

Represents a purchase made by a customer.

**PostgreSQL Schema (retail.fact_transaction)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| transaction_id | INTEGER | Primary key | 1, 2, 3 | Auto-increment |
| transaction_key | VARCHAR(50) | Natural key from source system | 'TRX-001', UUID | Unique |
| customer_id | INTEGER | Foreign key to dim_customer | 1, 2, 3 | |
| date_id | INTEGER | Foreign key to dim_date | 20230115 | YYYYMMDD format |
| location_id | INTEGER | Foreign key to dim_location | 1, 2, 3 | |
| total_amount | DECIMAL(12,2) | Total transaction amount | 156.98, 25.00 | Required |
| discount_amount | DECIMAL(12,2) | Total discount applied | 10.00, 0.00 | Default: 0 |
| tax_amount | DECIMAL(12,2) | Total tax amount | 12.55, 2.00 | Default: 0 |
| shipping_amount | DECIMAL(12,2) | Shipping cost | 5.99, 0.00 | Default: 0 |
| payment_method | VARCHAR(50) | Method of payment | 'credit_card', 'paypal' | |
| channel | VARCHAR(50) | Sales channel | 'web', 'mobile_app', 'in_store' | |
| transaction_time | TIME | Time of transaction | '13:45:30' | |
| is_return | BOOLEAN | Whether this is a return | TRUE, FALSE | Default: FALSE |
| created_at | TIMESTAMP | Transaction timestamp | '2023-01-15 13:45:30' | |

### Transaction Item

Represents an individual item within a transaction.

**PostgreSQL Schema (retail.fact_transaction_item)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| transaction_item_id | INTEGER | Primary key | 1, 2, 3 | Auto-increment |
| transaction_id | INTEGER | Foreign key to fact_transaction | 1, 2, 3 | |
| product_id | INTEGER | Foreign key to dim_product | 1, 2, 3 | |
| quantity | INTEGER | Quantity purchased | 1, 2, 5 | Required |
| unit_price | DECIMAL(10,2) | Price at time of purchase | 99.99, 25.50 | Required |
| discount_amount | DECIMAL(10,2) | Discount applied to item | 10.00, 0.00 | Default: 0 |
| tax_amount | DECIMAL(10,2) | Tax applied to item | 8.00, 2.04 | Default: 0 |
| line_total | DECIMAL(12,2) | Total amount for line item | 99.99, 51.00 | Required |
| created_at | TIMESTAMP | When item was added | '2023-01-15 13:45:30' | |

### Location

Represents a geographical location where transactions occur.

**PostgreSQL Schema (retail.dim_location)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| location_id | INTEGER | Primary key | 1, 2, 3 | Auto-increment |
| country | VARCHAR(100) | Country | 'USA', 'Canada' | |
| region | VARCHAR(100) | Region/province | 'Northeast', 'Ontario' | |
| state | VARCHAR(100) | State/territory | 'NY', 'CA' | |
| city | VARCHAR(100) | City | 'New York', 'Toronto' | |
| postal_code | VARCHAR(20) | Postal/zip code | '10001', 'M5V 2H1' | |
| created_at | TIMESTAMP | When location was added | '2023-01-01 00:00:00' | |
| updated_at | TIMESTAMP | Last update timestamp | '2023-01-01 00:00:00' | |

### Date (Dimension)

Represents a calendar date for time-based analysis.

**PostgreSQL Schema (retail.dim_date)**

| Attribute | Data Type | Description | Sample Values | Notes |
|-----------|-----------|-------------|---------------|-------|
| date_id | INTEGER | Primary key | 20230115 | YYYYMMDD format |
| full_date | DATE | Actual date | '2023-01-15' | |
| day_of_week | INTEGER | Day of week (0-6) | 0, 1, 2 | 0=Sunday, 6=Saturday |
| day_name | VARCHAR(10) | Name of day | 'Monday', 'Tuesday' | |
| day_of_month | INTEGER | Day number in month | 1, 2, ..., 31 | |
| day_of_year | INTEGER | Day number in year | 1, 2, ..., 365 | |
| week_of_year | INTEGER | Week number | 1, 2, ..., 53 | |
| month_number | INTEGER | Month number | 1, 2, ..., 12 | |
| month_name | VARCHAR(10) | Name of month | 'January', 'February' | |
| quarter | INTEGER | Quarter | 1, 2, 3, 4 | |
| year | INTEGER | Year | 2023, 2024 | |
| is_weekend | BOOLEAN | Whether it's a weekend | TRUE, FALSE | |
| is_holiday | BOOLEAN | Whether it's a holiday | TRUE, FALSE | |
| holiday_name | VARCHAR(100) | Name of holiday | 'Christmas', 'Labor Day' | |

### Browsing History (MongoDB Only)

Represents a customer's browsing activity on the e-commerce platform.

**MongoDB Schema (browsing_history collection)**

```json
{
  "customer_key": "CUST-001",
  "product_key": "PROD-001",
  "timestamp": "2023-01-15T13:30:45Z",
  "session_id": "SESSION-12345",
  "device": "mobile",
  "os": "iOS",
  "browser": "Safari",
  "ip_address": "192.168.1.1",
  "referer": "https://www.google.com/search",
  "time_spent_seconds": 120,
  "actions": [
    {
      "action_type": "view",
      "timestamp": "2023-01-15T13:30:45Z"
    },
    {
      "action_type": "add_to_cart",
      "timestamp": "2023-01-15T13:32:30Z"
    }
  ]
}
```

### Shopping Cart (MongoDB Only)

Represents a customer's shopping cart.

**MongoDB Schema (carts collection)**

```json
{
  "cart_id": "CART-12345",
  "customer_key": "CUST-001",
  "session_id": "SESSION-12345",
  "status": "active",
  "items": [
    {
      "product_key": "PROD-001",
      "quantity": 1,
      "price_at_add": 99.99,
      "added_at": "2023-01-15T13:32:30Z"
    },
    {
      "product_key": "PROD-015",
      "quantity": 2,
      "price_at_add": 25.50,
      "added_at": "2023-01-15T13:35:10Z"
    }
  ],
  "created_at": "2023-01-15T13:30:00Z",
  "last_activity": "2023-01-15T13:35:10Z",
  "conversion_info": {
    "converted_at": "2023-01-15T13:45:30Z",
    "transaction_key": "TRX-001"
  }
}
```

## Entity Relationships

The following diagram illustrates the relationships between the core entities in the PostgreSQL database:

```
dim_customer 1 --- * fact_transaction
dim_product 1 --- * fact_transaction_item
dim_location 1 --- * fact_transaction
dim_date 1 --- * fact_transaction
fact_transaction 1 --- * fact_transaction_item
```

### Key Relationships

1. A **Customer** can have many **Transactions**
2. A **Transaction** can have many **Transaction Items**
3. A **Product** can appear in many **Transaction Items**
4. A **Location** can have many **Transactions**
5. A **Date** can have many **Transactions**

## Staging Tables

The PostgreSQL database includes staging tables in the `staging` schema:

- `staging.stg_customer`: For loading customer data
- `staging.stg_product`: For loading product data  
- `staging.stg_transaction`: For loading transaction data
- `staging.stg_transaction_item`: For loading transaction item data

These tables have a similar structure to their dimensional counterparts but include additional fields for ETL processing:

- `source`: Source system identifier
- `batch_id`: Unique identifier for the load batch
- `processed_at`: Timestamp when the record was processed

## Data Flows

1. **Data Acquisition**:
   - External data is loaded into staging tables
   - Simulated data is generated for testing

2. **Data Processing**:
   - Staging data is validated and transformed
   - Records are inserted into dimensional tables
   - Fact tables are populated last

3. **Data Access**:
   - PostgreSQL provides structured data for analytics
   - MongoDB stores detailed, nested data
   - Redis caches frequently accessed data

## Data Quality Rules

The following data quality rules are enforced:

1. **Primary Keys**: All primary keys must be unique and not null
2. **Foreign Keys**: All foreign keys must reference existing records
3. **Required Fields**: Certain fields (marked as required) must not be null
4. **Data Types**: Values must conform to their specified data types
5. **Date Consistency**: Transaction dates must exist in the date dimension
6. **Price Validity**: All price fields must be non-negative
7. **Quantity Validity**: All quantity fields must be positive integers

## Data Refresh Schedule

- **Transactions**: Near real-time updates
- **Customers**: Daily updates
- **Products**: Daily updates
- **Locations**: Weekly updates
- **Date Dimension**: Pre-populated for multiple years

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2023-07-01 | Data Team | Initial version |
| 1.1 | 2023-07-15 | Data Team | Added MongoDB schemas |
| 1.2 | 2023-08-01 | Data Team | Added data quality rules |