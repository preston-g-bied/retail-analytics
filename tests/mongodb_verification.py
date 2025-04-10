# tests/mongodb_verification.py

from src.data.database import MongoDBConnection

# Connect to MongoDB using context manager
with MongoDBConnection() as db:
    # Get collection names
    collection_names = db.list_collection_names()
    print(f"Collections in MongoDB: {collection_names}")
    
    # Count documents in each collection
    for collection_name in collection_names:
        count = db[collection_name].count_documents({})
        print(f"  {collection_name}: {count} documents")
    
    # Get sample customer data
    customers = db.customers.find().limit(5)
    
    print("\nSample customer data from MongoDB:")
    for customer in customers:
        print(f"  {customer.get('customer_key')} - {customer.get('first_name')} {customer.get('last_name')}")
        # Print some nested data to verify structure
        if 'addresses' in customer:
            for address in customer.get('addresses', []):
                print(f"    Address: {address.get('city')}, {address.get('country')}")
        if 'preferences' in customer:
            prefs = customer.get('preferences', {})
            fav_categories = prefs.get('favorite_categories', [])
            print(f"    Favorite categories: {', '.join(fav_categories)}")

    # Get sample product data
    products = db.products.find().limit(5)
    
    print("\nSample product data from MongoDB:")
    for product in products:
        print(f"  {product.get('product_key')} - {product.get('name')}")
        # Print price information
        price_info = product.get('price', {})
        current_price = price_info.get('current', 'N/A')
        print(f"    Current price: {current_price}")
        # Print any reviews
        for review in product.get('reviews', [])[:2]:  # Show just first 2 reviews
            print(f"    Review: {review.get('rating')} stars - {review.get('review_text')[:50]}...")