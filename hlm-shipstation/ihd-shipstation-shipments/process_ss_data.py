import os
import requests
import json
from google.cloud import storage
import functions_framework

# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.get_bucket('hlm-shipstation')

authorization_key = os.environ.get('authorization_key')
store_id = os.environ.get('store_id')
url = f"https://ssapi.shipstation.com/shipments?storeId={store_id}&sortBy=ShipDate&sortDir=DESC"
headers = {
    'Host': 'ssapi.shipstation.com',
    'Authorization': f'Basic {authorization_key}'
}

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def process_ss_data(cloud_event):

    response = requests.get(url, headers=headers)
    shipments = response.json().get('shipments', [])

    for shipment in shipments:
        shipment_id = shipment['shipmentId']
        # Create blob for each shipment named with the shipmentId
        blob = bucket.blob(f'iheartdogs/shipments/unprocessed/{shipment_id}.json')
        blob.upload_from_string(json.dumps(shipment))

    print("Processed {} shipments".format(len(shipments)))
