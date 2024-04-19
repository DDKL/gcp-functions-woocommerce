import functions_framework
import os
import json
from google.cloud import storage
from woocommerce import API
from dateutil import parser # Import parser from dateutil
from datetime import datetime, timedelta
import pytz
from google.cloud import scheduler_v1
from google.cloud import firestore

# Initialize
storage_client = storage.Client()
scheduler_client = scheduler_v1.CloudSchedulerClient()
db = firestore.Client()
bucket_name = os.environ.get('bucket_name')
site_name = os.environ.get('site_name')
site_url = os.environ.get('site_url')
project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
location_id = os.environ.get('scheduler_location_id')  # e.g., 'us-central1'
scheduler_job_name = os.environ.get('scheduler_job_name')  # e.g., 'projects/my-project/locations/us-central1/jobs/my-job'
bucket = storage_client.get_bucket(bucket_name)



key = os.environ.get('consumer_key')
secret = os.environ.get('consumer_secret')

wcapi = API(
 url=site_url,
 consumer_key=key,
 consumer_secret=secret,
 wp_api=True,
 version="wc/v3",
 timeout=60
)



# Function to pause the scheduler job
def pause_scheduler_job():
    job_path = scheduler_client.job_path(project_id, location_id, scheduler_job_name)
    scheduler_client.pause_job(name=job_path)
    print(f"Paused Cloud Scheduler job: {scheduler_job_name}")


def list_orders(event, context):
    print(f"Processing orders for site: {site_name}")

    # Calculate 'after' and 'before' dates for the previous day
    utc = pytz.UTC
    today = datetime.now(utc)
    yesterday = today - timedelta(days=1)
    after = yesterday.strftime('%Y-%m-%dT00:00:00Z')
    before = yesterday.strftime('%Y-%m-%dT23:59:59Z')

    page_doc_ref = db.collection(f'{site_name}-processing_state').document('woocommerce_orders_page')
    page_doc = page_doc_ref.get()

    # Check if the document exists
    if page_doc.exists:
        last_processed_page = page_doc.to_dict().get('last_processed_page', 0)
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
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

    print(f"Fetched {len(orders)} orders from WooCommerce for site: {site_name}")

    if len(orders) > 0:
        for order in orders:
            order_id = order['id'] # Assuming each order has a unique 'id'
            # Parse the date_created string to get a datetime object
            date_created = parser.isoparse(order['date_created'])
            current_year, current_month = date_created.year, date_created.month
            blob = bucket.blob(f'{site_name}/Orders/Unprocessed/{current_year}/{current_month}/{order_id}.json')
            # Store the order in the bucket
            try:
                blob.upload_from_string(json.dumps(order))
            except Exception as e:
                print(f"Error uploading order {order_id}: {e}")
    else:
        print("No orders found - canceling pulls")
        pause_scheduler_job()
