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
url = f"https://ssapi.shipstation.com/shipments?storeId={store_id}&sortBy=CreateDate&sortDir=DESC&pageSize=50"
headers = {
    'Host': 'ssapi.shipstation.com',
    'Authorization': f'Basic {authorization_key}'
}

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def list_shipments(event):
    response = requests.get(url, headers=headers)
    shipments = response.json().get('shipments', [])

    for shipment in shipments:
        shipment_id = shipment['shipmentId']
        createDate = shipment.get('createDate')
        
        if createDate:
            # Parse the createDateStart to extract year and month
            createDate = datetime.strptime(createDate, '%Y-%m-%dT%H:%M:%S.%f')
            year = createDate.year
            month = createDate.month
            # Create the blob path based on year and month
            blob_path = f'{site_name}/shipments/unprocessed/{year}/{month}/{shipment_id}.json'
        else:
            # Default path if createDateStart is not available
            blob_path = f'{site_name}/shipments/unprocessed/{shipment_id}.json'
        
        # Create blob for each shipment named with the shipmentId
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(shipment))

    print("Processed {} shipments".format(len(shipments)))
