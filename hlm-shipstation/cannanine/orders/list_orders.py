import os
import requests
import json
from google.cloud import storage
import functions_framework
from datetime import datetime

# Initialize Google Cloud Storage client
storage_client = storage.Client()

authorization_key = os.environ.get('authorization_key')
store_id = os.environ.get('store_id')
site_name = os.environ.get('site_name')
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)
url = f"https://ssapi.shipstation.com/orders?storeId={store_id}&sortBy=CreateDate&sortDir=DESC&pageSize=50"
headers = {
    'Host': 'ssapi.shipstation.com',
    'Authorization': f'Basic {authorization_key}'
}

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def list_orders(event):
    response = requests.get(url, headers=headers)
    orders = response.json().get('orders', [])

    for order in orders:
        order_id = order['orderNumber']
        createDate = order.get('createDate')
        
        if createDate:
            # Parse the createDateStart to extract year and month
            createDate = datetime.strptime(createDate, '%Y-%m-%dT%H:%M:%S.%f')
            year = createDate.year
            month = createDate.month
            # Create the blob path based on year and month
            blob_path = f'{site_name}/orders/unprocessed/{year}/{month}/{order_id}.json'
        else:
            # Default path if createDateStart is not available
            blob_path = f'{site_name}/orders/unprocessed/{order_id}.json'
        
        # Create blob for each order named with the orderId
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(order))

    print("Processed {} orders".format(len(orders)))
