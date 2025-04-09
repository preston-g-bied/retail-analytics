# src/data/simulation/retail_simulator.py

"""
Simulator for generating synthetic retail data including customers, products, and transactions.
"""

import random
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np
from faker import Faker

from src.data.simulation.base_simulator import BaseSimulator

# set up logging
logger = logging.getLogger(__name__)

class RetailSimulator(BaseSimulator):
    """
    Simulator for generating synthetic retail data.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None, seed: Optional[int] = None):
        """
        Initialize the retail simulator.
        
        Args:
            name: Name of the simulator
            config: Configuration dictionary
            seed: Random seed for reproducibility
        """
        super().__init__(name, config, seed)

        # initialize Faker with the same seed
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)

        # store generated data
        self.customers = None
        self.products = None
        self.transactions = None
        self.transaction_items = None
        self.locations = None

        # default configuration
        default_config = {
            "num_customers": 1000,
            "num_products": 500,
            "num_categories": 20,
            "num_brands": 50,
            "num_locations": 100,
            "time_range": {
                "start": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                "end": datetime.now().strftime("%Y-%m-%d")
            },
            "transaction_params": {
                "mean_transactions_per_customer": 5,
                "std_transactions_per_customer": 3,
                "mean_items_per_transaction": 3,
                "std_items_per_transaction": 2,
                "price_range": {
                    "min": 5.0,
                    "max": 500.0
                },
                "discount_probability": 0.3,
                "max_discount_percent": 30,
                "tax_rate": 0.08
            },
            "customer_params": {
                "inactive_probability": 0.1
            },
            "product_params": {
                "discontinued_probability": 0.05
            }
        }

        # merge default config with provided config
        if config:
            self._merge_configs(default_config, config)
        self.config = default_config

    def _merge_configs(self, default_config: Dict[str, Any], user_config: Dict[str, Any]) -> None:
        """
        Recursively merge user config into default config.
        
        Args:
            default_config: Default configuration to update
            user_config: User provided configuration
        """
        for key, value in user_config.items():
            if isinstance(value, dict) and key in default_config and isinstance(default_config[key], dict):
                self._merge_configs(default_config[key], value)
            else:
                default_config[key] = value

    def _generate_customers(self) -> pd.DataFrame:
        """
        Generate synthetic customer data.
        
        Returns:
            DataFrame with customer data
        """
        logger.info(f"Generating {self.config['num_custimers']} customers")

        customers = []
        for i in range(self.config['num_customers']):
            is_active = random.random() > self.config['customer_params']['inactive_probability']
            created_date = self.faker.date_time_between(
                start_date=self.config['time_range']['start'],
                end_date=self.config['time_range']['end']
            )

            customer = {
                'customer_id': i + 1,
                'customer_key': str(uuid.uuid4()),
                'first_name': self.faker.first_name(),
                'last_name': self.faker.last_name(),
                'email': self.faker.email(),
                'phone': self.faker.phone_number(),
                'created_at': created_date,
                'updated_at': self.faker.date_time_between(
                    start_date=created_date,
                    end_date=self.config['time_range']['end']
                ),
                'is_active': is_active
            }
            customers.append(customer)

        return pd.DataFrame(customers)
    
    def _generate_products(self) -> pd.DataFrame:
        """
        Generate synthetic product data.
        
        Returns:
            DataFrame with product data
        """
        logger.info(f"Generating {self.config['num_products']} products")

        # generate categories
        categories = []
        for i in range(self.config['num_categories']):
            category_name = self.faker.word()
            # generate 1-3 subcategories per category
            num_subcategories = random.randint(1, 3)
            subcategories = [f"{category_name} {self.faker.word()}" for _ in range(num_subcategories)]
            categories.extend([(category_name, subcategory) for subcategory in subcategories])

        # generate brands
        brands = [self.faker.company() for _ in range(self.config['num_brands'])]

        # generate products
        products = []
        for i in range(self.config['num_products']):
            is_active = random.random() > self.config['product_params']['discontinued_probability']
            created_date = self.faker.date_time_between(
                start_date=self.config['time_range']['start'],
                end_date=self.config['time_range']['end']
            )

            # randomly select category and subcategory
            category, subcategory = random.choice(categories)

            # randomly select brand
            brand = random.choice(brands)

            # generate price within range
            price_range = self.config['transaction_params']['price_range']
            unit_price = round(random.uniform(price_range['min'], price_range['max']), 2)

            # cost price is typically 40-70% of unit price
            cost_price = round(unit_price * random.uniform(0.4, 0.7), 2)

            product = {
                'product_id': i + 1,
                'product_key': str(uuid.uuid4()),
                'product_name': f"{brand} {self.faker.word()} {random.choice(['Pro', 'Lite', 'Max', 'Ultra', ''])}".strip(),
                'description': self.faker.text(max_nb_chars=200),
                'category': category,
                'subcategory': subcategory,
                'brand': brand,
                'supplier': self.faker.company(),
                'unit_price': unit_price,
                'cost_price': cost_price,
                'created_at': created_date,
                'updated_at': self.faker.date_time_between(
                    start_date=created_date,
                    end_date=self.config['time_range']['end']
                ),
                'is_active': is_active
            }
            products.append(product)

        return pd.DataFrame(products)
    
    def _generate_locations(self) -> pd.DataFrame:
        """
        Generate synthetic location data.
        
        Returns:
            DataFrame with location data
        """
        logger.info(f"Generating {self.config['num_locations']} locations")

        locations = []
        for i in range(self.config['num_locations']):
            country = self.faker.country()
            created_date = self.faker.date_time_between(
                start_date=self.config['time_range']['start'],
                end_date=self.config['time_range']['end']
            )

            location = {
                'location_id': i + 1,
                'country': country,
                'region': self.faker.state(),
                'state': self.faker.state_abbr(),
                'city': self.faker.city(),
                'postal_code': self.faker.postcode(),
                'created_at': created_date,
                'updated_at': self.faker.date_time_between(
                    start_date=created_date,
                    end_date=self.config['time_range']['end']
                )
            }
            locations.append(location)

        return pd.DataFrame(location)
    
    def _generate_transactions(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate synthetic transaction and transaction item data.
        
        Returns:
            Tuple of (transactions DataFrame, transaction items DataFrame)
        """
        if self.customers is None or self.products is None or self.locations is None:
            raise ValueError("Customers, products, and locations must be generated before transactions")
        
        logger.info("Generating transactions and transaction items")

        # generate transactions per customer
        transactions = []
        transaction_items = []
        transaction_id_counter = 1
        transaction_item_id_counter = 1

        # parse date strings to datetime objects
        start_date = datetime.strptime(self.config['time_range']['start'], "%Y-%m-%d")
        end_date = datetime.strptime(self.config['time_range']['end'], "%Y-%m-%d")

        # get parameters
        mean_trans = self.config['transaction_params']['mean_transactions_per_customer']
        std_trans = self.config['transaction_params']['std_transactions_per_customer']
        mean_items = self.config['transaction_params']['mean_items_per_transaction']
        std_items = self.config['transaction_params']['std_items_per_transaction']

        # get active customers
        active_customers = self.customers[self.customers['is_active']].copy()

        # generate transactions
        for _, customer in active_customers.iterrows():
            # randomly determine number of transactions for this customer
            num_transactions = max(1, int(np.random.normal(mean_trans, std_trans)))

            for _ in range(num_transactions):
                # generate transaction date between start and end date, but after customer creation
                customer_created = customer['created_at']
                if isinstance(customer_created, str):
                    customer_created = datetime.fromisoformat(customer_created.replace('Z', '+00:00'))

                trans_date = self.faker.date_time_between(
                    start_date=max(start_date, customer_created),
                    end_date=end_date
                )

                # conver date to integer format YYYYMMDD for date_id
                date_id = int(trans_date.strftime('%Y%m%d'))

                # randomly select location
                location = self.locations.sample(1).iloc[0]

                # randomly determine if this is a return
                is_return = random.random() < 0.05

                # generate transaction
                transaction = {
                    'transaction_id': transaction_id_counter,
                    'transaction_key': str(uuid.uuid4()),
                    'customer_id': customer['customer_id'],
                    'date_id': date_id,
                    'location_id': location['location_id'],
                    'total_amount': 0.0,    # will be updated after generating items
                    'discount_amount': 0.0,     # will be updated after generating items
                    'tax_amount': 0.0,      # will be updated after generating items
                    'shipping_amount': random.uniform(0, 15) if not is_return else 0.0,
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']),
                    'channel': random.choice(['web', 'mobile_app', 'in_store', 'phone']),
                    'transaction_time': trans_date.time(),
                    'is_return': is_return,
                    'created_at': trans_date
                }

                # generate transaction items
                num_items = max(1, int(np.random.normal(mean_items, std_items)))
                total_amount = 0.0
                total_discount = 0.0
                total_tax = 0.0

                # randomly select products
                # for simplicity, we'll allow duplicates in the random sample
                selected_products = self.products.sample(n=num_items, replace=True)

                for _, product in selected_products.iterrows():
                    # randomly determine quantity (usually 1-3)
                    quantity = random.randint(1, 3)

                    # get unit price
                    unit_price = product['unit_price']

                    # randomly determine if discount applies
                    discount_amt = 0.0
                    if random.random() < self.config['transaction_params']['discount_probability']:
                        discount_percent = random.uniform(5, self.config['transaction_params']['max_discount_percent'])
                        discount_amt = round(unit_price * (discount_percent / 100) * quantity, 2)

                    # calculate line total
                    line_total = round(unit_price * quantity - discount_amt, 2)

                    # calculate tax
                    tax_amt = round(line_total * self.config['transaction_params']['tax_rate'], 2)

                    # update totals
                    total_amount += line_total
                    total_discount += discount_amt
                    total_tax += tax_amt

                    # create transaction item
                    transaction_item = {
                        'transaction_item_id': transaction_item_id_counter,
                        'transaction_id': transaction_id_counter,
                        'product_id': product['product_id'],
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'discount_amount': discount_amt,
                        'tax_amount': tax_amt,
                        'line_total': line_total,
                        'created_at': trans_date
                    }

                    transaction_items.append(transaction_item)
                    transaction_item_id_counter += 1

                # update transaction with calculated totals
                transaction['total_amount'] = round(total_amount, 2)
                transaction['discount_amount'] = round(total_discount, 2)
                transaction['tax_amount'] = round(total_tax, 2)

                # add transaction to list
                transactions.append(transaction)
                transaction_id_counter += 1
        
        logger.info(f"Generated {len(transactions)} transactions and {len(transaction_items)} transaction items")

        return pd.DataFrame(transactions), pd.DataFrame(transaction_items)
    
    def generate(self) -> Dict[str, pd.DataFrame]:
        """
        Generate all synthetic retail data.
        
        Returns:
            Dictionary of DataFrames containing generated data
        """
        # update metadata
        self.metadata.update({
            "simulation_date": datetime.now().isoformat(),
            "status": "in_progress"
        })

        try:
            # generate data
            self.customers = self._generate_customers()
            self.products = self._generate_products()
            self.locations = self._generate_locations()
            self.transactions, self.transaction_items = self._generate_transactions()

            # update metadata
            self.metadata.update({
                "status": "completed",
                "record_counts": {
                    "customers": len(self.customers),
                    "products": len(self.products),
                    "locations": len(self.locations),
                    "transactions": len(self.transactions),
                    "transaction_items": len(self.transaction_items)
                },
                "completion_time": datetime.now().isoformat()
            })

            # return all generated data
            return {
                "customers": self.customers,
                "products": self.products,
                "locations": self.locations,
                "transactions": self.transactions,
                "transaction_items": self.transaction_items
            }
        
        except Exception as e:
            logger.error(f"Error generating data: {e}")
            self.metadata.update({
                "status": "error",
                "error": str(e),
                "completion_time": datetime.now().isoformat()
            })
            raise

    def simulate_transactions_for_date_range(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        Simulate transactions for a specific date range.
        Useful for simulating new data after initial generation.
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dictionary with transaction and transaction item DataFrames
        """
        # store original time range
        original_time_range = self.config['time_range'].copy()

        # update time range for simulation
        self.config['time_range'] = {
            'start': start_date,
            'end': end_date
        }

        try:
            # generate transactions for the specified period
            transactions, transaction_items = self._generate_transactions()

            # restore original time range
            self.config['time_range'] = original_time_range

            return {
                "transactions": transactions,
                "transaction_items": transaction_items
            }
        except Exception as e:
            # restore original time range
            self.config['time_range'] = original_time_range
            logger.error(f"Error simulating transactions: {e}")
            raise