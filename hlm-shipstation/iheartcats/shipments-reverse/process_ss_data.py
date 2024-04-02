import os
import requests
import json
from google.cloud import storage
from google.cloud import firestore
import functions_framework
import calendar
from datetime import date

# Initialize Google Cloud Storage/Firestore client
storage_client = storage.Client()
bucket = storage_client.get_bucket('hlm-shipstation')
db = firestore.Client()

authorization_key = os.environ.get('authorization_key')
store_id = os.environ.get('store_id')

site_name = 'iheartcats'

headers = {
    'Host': 'ssapi.shipstation.com',
    'Authorization': f'Basic {authorization_key}'
}

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def process_ss_data(cloud_event):

    print(f"Processing ShipStation shipments for site: {site_name}")

    state_doc_ref = db.collection(f'{site_name}-processing_state').document('shipstation_shipments')
    month_doc_ref = db.collection(f'{site_name}-processing_state').document('shipstation_month')
    year_doc_ref = db.collection(f'{site_name}-processing_state').document('shipstation_year')
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
        last_processed_year = year_doc.to_dict().get('last_processed_year', 2020)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_year = 2023

    # Fetch the next batch of orders in ascending order
    current_page = last_processed_page + 1

    month = last_processed_month
    year = last_processed_year

     # Determine number of days in current year/month
    num_days = calendar.monthrange(year,month)[1]
    end_date = date(year,month,num_days)

    # Start from Jan 2024
    start_date = date(year,month,1)

    # shipDateStart, shipDateEnd, pageSize, page, includeShipmentItems
    # url = f"https://ssapi.shipstation.com/shipments?storeId={store_id}&sortBy=ShipDate&sortDir=DESC"
    url = f"https://ssapi.shipstation.com/shipments?storeId={store_id}&sortBy=ShipDate&shipDateStart={start_date}&shipDateEnd={end_date}&page={current_page}&includeShipmentItems=true"

    response = requests.get(url, headers=headers)
    shipments = response.json().get('shipments', [])

    for shipment in shipments:
        orderNumber = shipment['orderNumber']
        # Create blob for each shipment named with the orderNumber
        blob = bucket.blob(f'{site_name}/shipments/unprocessed/{year}/{month}/{orderNumber}.json')
        try:
            blob.upload_from_string(json.dumps(shipment))
        except Exception as e:
            print(f"Error uploading order {orderNumber}: {e}")
    # Update the state in Firestore
    state_doc_ref.set({'last_processed_page': current_page})

    # Handle cases where there are no more orders
    if len(shipments) < 50:
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

    print("Processed {} shipments".format(len(shipments)))
