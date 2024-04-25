import functions_framework
import os
import json
import pytz
import requests
from google.cloud import storage
from dateutil     import parser
from datetime     import datetime, timedelta

# Initialize
storage_client     = storage.Client()

api_version = "2024-04"

bucket_name        = os.environ.get('bucket_name')
access_token       = os.environ.get('access_token')
shop_name          = os.environ.get('shop_name')
software_name      = os.environ.get('software_name')

site_url           = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/orders.json"
bucket             = storage_client.get_bucket(bucket_name)
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}


def process_yesterday_shopify_orders(a, b):

    # Set the timezone to PST/PDT
    pst_tz = pytz.timezone('America/Los_Angeles')
    
    # Get the current time in PST
    today = datetime.now(pst_tz)
    yesterday = today - timedelta(days=1)

    # Format the dates in ISO 8601 format with timezone information
    after = yesterday.strftime('%Y-%m-%dT00:00:00%z')
    before = yesterday.strftime('%Y-%m-%dT23:59:59%z')

    # utc          = pytz.UTC
    # today        = datetime.now(utc)
    # yesterday    = today - timedelta(days=1)
    # after        = yesterday.strftime('%Y-%m-%dT00:00:00Z')
    # before       = yesterday.strftime('%Y-%m-%dT23:59:59Z')

    params = {
        "status": "any",
        "limit": 25,
        "created_at_min": after,
        "created_at_max": before
    }

    url = site_url

    processed_count = 0  # Initialize a counter for successfully processed orders

    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            orders = response.json().get('orders', [])
            for order in orders:
                order_number = order.get('order_number')
                if order_number is None:
                    order_id = order.get('id', 'Unknown ID')
                    print(f"Order number missing for order ID: {order_id}, skipping order")
                    continue  # Skip this order if no number is available
                
                date_created = parser.isoparse(order['created_at'])
                current_year, current_month = date_created.year, date_created.month
                blob = bucket.blob(f'{software_name}/orders/{current_year}/{current_month}/{order_number}.json')
                blob.upload_from_string(json.dumps(order), content_type='application/json')
                processed_count += 1  # Increment the processed order count
                
            link_header = response.headers.get('Link')
            next_url = None
            if link_header:
                links = link_header.split(',')
                for link in links:
                    if 'rel="next"' in link:
                        next_url = link.split(';')[0].strip('<> ')
            url = next_url
            params = None  # Clear params since they're set in the next URL
        else:
            print(f"Failed to fetch orders: {response.text}")
            break

    print(f"Finished processing {processed_count} orders")
