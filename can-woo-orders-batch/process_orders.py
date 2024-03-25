import base64
import functions_framework
import functions_framework
import os
import json
import requests
from google.cloud import storage
from google.cloud import firestore
from woocommerce import API

# Initialize

storage_client = storage.Client()
db = firestore.Client()

bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)

key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

wcapi = API(
  url='https://cannanine.com',
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
        site_name = data.get('site_name') if data else 'CAN'
    else:
        print("No data found in event")
        return

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

    # Fetch orders from the API
    orders = wcapi.get("orders", params={"page": current_page, "order": "asc"}).json()

    print(f"Fetched {len(orders)} orders from WooCommerce for site: {site_name}")


    for order in orders:
        order_id = order['id']  # Assuming each order has a unique 'id'
        blob = bucket.blob(f'{site_name}/Orders/Unprocessed/{order_id}.json')
        try:
            blob.upload_from_string(json.dumps(order))
        except Exception as e:
            print(f"Error uploading order {order_id}: {e}")

    # Update the state in Firestore
    state_doc_ref.set({'last_processed_page': current_page})

    # Handle cases where there are no more orders
    if len(orders) < 10:
        state_doc_ref.set({'last_processed_page': 0})
        print(f"ALL ORDERS COMPLETED for {site_name}")