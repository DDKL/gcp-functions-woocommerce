import base64
import functions_framework
import os
import json
import requests
from google.cloud import storage
from woocommerce import API

# Initialize

storage_client = storage.Client()
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)

key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

wcapi = API(
  url="https://iheartcats.com",
  consumer_key=key,
  consumer_secret=secret,
  wp_api=True,
  version="wc/v3",
  timeout=30
)


# Function to retrieve processed orders
def get_processed_orders():
    try:
        blob = bucket.blob('iHeartCats/Orders/Unprocessed/processed_orders.txt')
        return blob.download_as_text().splitlines()
    except Exception as e:
        print(f"Error retrieving processed orders: {e}")
        return []

# Function to update the list of processed orders
def update_processed_orders(processed_orders, order_id):
    try:
        blob = bucket.blob('iHeartCats/Orders/Unprocessed/processed_orders.txt')
        processed_orders.append(order_id)
        blob.upload_from_string('\n'.join(processed_orders))
    except Exception as e:
        print(f"Error updating processed orders: {e}")

# Function to process orders
def process_orders(event, context):
    # Retrieve the current list of processed orders
    processed_orders = get_processed_orders()

    # Fetch orders from the API
    orders = wcapi.get("orders").json()

    for order in orders:
        order_id = order['id']  # Assuming each order has a unique 'id'
        if order_id not in processed_orders:
            # Store the order in the bucket
            blob = bucket.blob(f'iHeartCats/Orders/Unprocessed/{order_id}.json')
            blob.upload_from_string(json.dumps(order))
            # Update the list of processed orders
            update_processed_orders(processed_orders, order_id)

