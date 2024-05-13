import base64
import functions_framework
import os
import json
import requests
import calendar
from google.cloud import storage
from google.cloud import firestore
from woocommerce import API
from datetime import datetime, timedelta

# Initialize

storage_client = storage.Client()
db = firestore.Client()

bucket_name = "hlm-woocommerce"
bucket = storage_client.get_bucket(bucket_name)

key = "ck_22493394838b7d2dd4328bcc5b935c6ed6b9314e"
secret = "cs_0f74c57be049c2aaf6ae0218c108ac386e1bf231"
site_name = "iheartdogs"
site_url = "https://iheartdogs.com"

wcapi = API(
  url=site_url,
  consumer_key=key,
  consumer_secret=secret,
  wp_api=True,
  version="wc/v3",
  timeout=60
)


# Function to process historical orders
def list_historical_orders(event, context):

    print(f"Processing orders for site: {site_name}")

    state_doc_ref = db.collection(f'{site_name}-processing_state').document('woocommerce_orders')

    state_doc = state_doc_ref.get()


    # Check if the document exists
    if state_doc.exists:
        last_processed_page = state_doc.to_dict().get('last_processed_page', 0)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_page = 0

    # Fetch the next batch of orders in ascending order
    current_page = last_processed_page + 1

    month = 4
    year = 2024

     # Determine the current date
    num_days = calendar.monthrange(year,month)[1]
    end_date = datetime(year,month,num_days)
    # end_date_str = end_date.strftime('%Y-%m-%dT23:59:59Z') #GMT
    end_date_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    start_date = datetime(year,month,1)
    # start_date_str = start_date.strftime('%Y-%m-%dT00:00:00Z') #GMT
    start_date_str = start_date.strftime('%Y-%m-%dT00:00:00Z')

    # Fetch orders from the API
    orders = wcapi.get("orders", params={
        "after": start_date_str,
        "before": end_date_str,
        "page": current_page, 
        "order": "asc",
        "per_page": 100
    }).json()

    print(f"Fetched {len(orders)} orders from WooCommerce for site: {site_name}")

    if len(orders) > 0:
        for order in orders:
            order_id = order['id']  # Assuming each order has a unique 'id'
            blob = bucket.blob(f'{site_name}/Orders/Unprocessed/{year}/{month}/{order_id}.json')
            try:
                blob.upload_from_string(json.dumps(order))
            except Exception as e:
                print(f"Error uploading order {order_id}: {e}")
    else:
        print("No orders found")
        
    # Update the state in Firestore
    state_doc_ref.set({'last_processed_page': current_page})

    # Handle cases where there are no more orders
    if len(orders) < 100:
        print(f"ALL ORDERS COMPLETED for {site_name}")
