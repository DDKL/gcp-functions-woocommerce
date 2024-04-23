import functions_framework
import os
import json
import pytz
from google.cloud import firestore
from google.cloud import scheduler_v1
from google.cloud import storage
from woocommerce  import API
from dateutil     import parser
from datetime     import datetime, timedelta

# Initialize
storage_client     = storage.Client()
scheduler_client   = scheduler_v1.CloudSchedulerClient()
db                 = firestore.Client()

bucket_name        = os.environ.get('bucket_name')
key                = os.environ.get('consumer_key')
secret             = os.environ.get('consumer_secret')
project_id         = os.environ.get('GOOGLE_CLOUD_PROJECT')
scheduler_job_name = os.environ.get('scheduler_job_name')  # e.g., 'projects/homelife-analytics/locations/us-central1/jobs/my-job'
location_id        = os.environ.get('scheduler_location_id')  # e.g., 'us-central1'
site_url           = os.environ.get('site_url')
software_name      = os.environ.get('software_name')

bucket             = storage_client.get_bucket(bucket_name)

# Function to pause the scheduler job
def pause_scheduler_job():
    job_path = scheduler_client.job_path(project_id, location_id, scheduler_job_name)
    scheduler_client.pause_job(name=job_path)
    print(f"Paused Cloud Scheduler job: {scheduler_job_name}")


def process_woocommerce_orders_by_date(event, context):
    print(f"Processing orders for {bucket_name} - {software_name}")

    wcapi = API(
        url=site_url,
        consumer_key=key,
        consumer_secret=secret,
        wp_api=True,
        version="wc/v3",
        timeout=120
    )

    # Calculate 'after' and 'before' dates for the previous day
    utc          = pytz.UTC
    today        = datetime.now(utc)
    yesterday    = today - timedelta(days=1)
    after        = yesterday.strftime('%Y-%m-%dT00:00:00Z')
    before       = yesterday.strftime('%Y-%m-%dT23:59:59Z')

    page_doc_ref = db.collection(f'{bucket_name}-processing_state').document('woocommerce_orders_page')
    page_doc     = page_doc_ref.get()

    # Check if the document exists
    if page_doc.exists:
        last_processed_page = page_doc.to_dict().get('last_processed_page', 0)
    else:
        print(f"No existing document for {software_name}, starting from the beginning.")
        last_processed_page = 0

    # Fetch the next batch of orders in ascending order
    current_page = last_processed_page + 1

    # Get yesterday's start/end date

    # Fetch orders from the API
    orders = wcapi.get("orders", params={
        "after": after,
        "before": before,
        "page": current_page, 
        "order": "asc",
        "per_page": 50
    }).json()

    

    if len(orders) > 0:
        for order in orders:
            order_id = order['id'] # Assuming each order has a unique 'id'
            # Parse the date_created string to get a datetime object
            date_created = parser.isoparse(order['date_created_gmt'])
            current_year, current_month = date_created.year, date_created.month
            blob = bucket.blob(f'{software_name}/orders/{current_year}/{current_month}/{order_id}.json')
            # Update the page in Firestore
            page_doc_ref.set({'last_processed_page': current_page})
            # Store the order in the bucket
            try:
                blob.upload_from_string(json.dumps(order))
            except Exception as e:
                print(f"Error uploading order {order_id}: {e}")
    else:
        print("No orders found - canceling pulls")
        pause_scheduler_job()
        page_doc_ref.set({'last_processed_page': 0}, merge=True)  # Using merge=True to update specific field
        print("Successfully reset 'last_processed_page' to 0.")
