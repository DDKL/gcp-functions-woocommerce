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
after_date         = os.environ.get('after_date')  # Example usage - '2024-04-22'
before_date        = os.environ.get('before_date') # Example usage - '2024-04-22'

site_url           = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/orders.json"
bucket             = storage_client.get_bucket(bucket_name)
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}


def process_yesterday_shopify_orders(a, b):

    # Set the timezone to PST/PDT
    pst_tz = pytz.timezone('America/Los_Angeles')
    
    # Convert after_date and before_date from string to datetime object
    after_datetime = datetime.strptime(after_date, '%Y-%m-%d').replace(tzinfo=pst_tz)
    before_datetime = datetime.strptime(before_date, '%Y-%m-%d').replace(tzinfo=pst_tz)

    # Format the dates in ISO 8601 format with timezone information
    # Since the dates are without time, set the start of 'after' and the end of 'before'
    after = after_datetime.strftime('%Y-%m-%dT00:00:00%z')
    before = before_datetime.strftime('%Y-%m-%dT23:59:59%z')

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
