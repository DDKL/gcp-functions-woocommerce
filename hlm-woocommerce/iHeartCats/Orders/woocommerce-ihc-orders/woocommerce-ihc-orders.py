import functions_framework
import os
import json
from google.cloud import storage
from woocommerce import API
from datetime import datetime

# Initialize
storage_client = storage.Client()
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)
site_name = 'iHeartCats'

key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

current_year, current_month = datetime.now().year, datetime.now().month

wcapi = API(
  url="https://iheartcats.com",
  consumer_key=key,
  consumer_secret=secret,
  wp_api=True,
  version="wc/v3",
  timeout=60
)


# Function to process orders
def process_orders(event, context):

    print(f"Processing orders for site: {site_name}")

    
    # Fetch orders from the API
    orders = wcapi.get("orders", params={
        "per_page": 25
    }).json()

    print(f"Fetched {len(orders)} orders from WooCommerce for site: {site_name}")

    if len(orders) > 0:
        for order in orders:
            order_id = order['id']  # Assuming each order has a unique 'id'
            blob = bucket.blob(f'{site_name}/Orders/Unprocessed/{current_year}/{current_month}/{order_id}.json')
            # Store the order in the bucket
            try:
                blob.upload_from_string(json.dumps(order))
            except Exception as e:
                print(f"Error uploading order {order_id}: {e}")
    else:
        print("No orders found")