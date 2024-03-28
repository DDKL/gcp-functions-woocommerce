import base64
import functions_framework
import os
import json
import requests
from google.cloud import storage
from woocommerce import API

# Initialize

storage_client = storage.Client()
bucket = storage_client.get_bucket('hlm-woocommerce')

key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

wcapi = API(
  url="https://cannanine.com",
  consumer_key=key,
  consumer_secret=secret,
  wp_api=True,
  version="wc/v3"
)


# Function to process orders
def process_orders(event, context):

    # Fetch orders from the API
    orders = wcapi.get("orders").json()

    for order in orders:
        order_id = order['id']  # Assuming each order has a unique 'id'

        # Store the order in the bucket
        blob = bucket.blob(f'Cannanine/Orders/Unprocessed/{order_id}.json')
        #blob.upload_from_string(str(order))
        blob.upload_from_string(json.dumps(order))