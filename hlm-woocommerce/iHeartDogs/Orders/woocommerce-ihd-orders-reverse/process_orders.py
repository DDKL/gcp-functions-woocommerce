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

bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)

key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

wcapi = API(
  url='https://iheartdogs.com',
  consumer_key=key,
  consumer_secret=secret,
  wp_api=True,
  version="wc/v3",
  timeout=60
)


# Function to process orders
def process_orders(event, context):

    # Decode the Pub/Sub message
    if 'data' in event:
        message_data = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(message_data)
        site_name = data.get('site_name') if data else 'iHeartDogs'
    else:
        print("No data found in event")
        return

    print(f"Processing orders for site: {site_name}")

    state_doc_ref = db.collection(f'{site_name}-processing_state').document('woocommerce_orders')
    month_doc_ref = db.collection(f'{site_name}-processing_state').document('month')
    year_doc_ref = db.collection(f'{site_name}-processing_state').document('year')
    state_doc = state_doc_ref.get()
    month_doc = month_doc_ref.get()
    year_doc = year_doc_ref.get()

    # Check if the document exists
    if state_doc.exists:
        last_processed_page = state_doc.to_dict().get('last_processed_page', 0)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_page = 0

    if month_doc.exists:
        last_processed_month = month_doc.to_dict().get('last_processed_month', 1)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_month = 1

    if year_doc.exists:
        last_processed_year = year_doc.to_dict().get('last_processed_year', 2023)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_year = 2023

    # Fetch the next batch of orders in ascending order
    current_page = last_processed_page + 1

    month = last_processed_month
    year = last_processed_year

     # Determine the current date
    num_days = calendar.monthrange(year,month)[1]
    end_date = datetime(year,month,num_days)
    end_date_str = end_date.strftime('%Y-%m-%dT23:59:59Z')

    start_date = datetime(year,month,1)
    start_date_str = start_date.strftime('%Y-%m-%dT00:00:00Z')

    # Fetch orders from the API
    orders = wcapi.get("orders", params={
        "after": start_date_str,
        "before": end_date_str,
        "page": current_page, 
        "order": "asc",
        "per_page": 20
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
    if len(orders) < 20:
        state_doc_ref.set({'last_processed_page': 0})
        print(f"ALL ORDERS COMPLETED for {site_name}")
        # Move to the next month
        if month == 12:
            next_year = year + 1
            year_doc_ref.set({'last_processed_year': next_year})
            month_doc_ref.set({'last_processed_month': 1})
        else:
            month += 1
            month_doc_ref.set({'last_processed_month': month})