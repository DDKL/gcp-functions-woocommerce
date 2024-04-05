import base64
import functions_framework
import pandas as pd 
from datetime import datetime
from google.cloud import storage
import os
import json

# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.bucket(bucket_name)
site_name = 'iHeartDogs'

current_year, current_month = datetime.now().year, datetime.now().month
current_year, current_month = datetime.now().year, 3

def extract_wc_cog_order_total_cost(row):
    for item in row:
        if item['key'] == '_wc_cog_order_total_cost':
            return item['value']
    return None


def extract_total_refunds(row):
    # If there's exactly one refund entry
    if len(row) == 1:
        print('refund found')
        # Convert to absolute value to remove negative sign
        return abs(float(row[0].get('total', 0)))
    # If there are two or more refund entries, sum their totals
    elif len(row) >= 2:
        print('refunds found')
        # Convert each total to absolute value and sum them
        return sum(abs(float(refund.get('total', 0))) for refund in row)
    # Default value if no refunds
    return 0


def calculate_transaction_costs(total, payment_method):
    payment_methods = {
        "afterpay": 7,
        "amazon_payments_advanced": 3.8,
        "authorize_net_cim_credit_card": 2.75,
        "cheque": 0,
        "first_data_payeezy_credit_card": 0,
        "other": 0,
        "paypal": 2.9,
        "ppec_paypal": 2.9,
        "simplify_commerce": 0,
        "square_credit_card": 2.6,
        "wc_autoship_authorize_net": 2.75,
        "wc_autoship_authorize_net_cbd": 2.75,
        "ppcp-gateway": 0,
        "wpfi_test": 0
    }

    rate = payment_methods.get(payment_method, 0) / 100  # Convert percentage to a decimal
    transaction_cost = float(total) * rate
    return round(transaction_cost, 2)


def process_blob_to_csv(blob):
    # Read JSON data from blob
    json_bytes = blob.download_as_bytes()

    data = json.loads(json_bytes.decode('utf-8'))
    
    # Convert JSON to Pandas DataFrame
    df = pd.json_normalize(data)

    # Process DataFrame (e.g., drop columns, transform, etc.)
    df['total_cogs'] = df['meta_data'].apply(extract_wc_cog_order_total_cost)
    df['total_refunds'] = df['refunds'].apply(extract_total_refunds)
    df['transaction_cost'] = df.apply(lambda row: calculate_transaction_costs(pd.to_numeric(row['total']), row['payment_method']), axis=1)

    selected_columns = [
        "id", "status", "currency", "discount_total", "shipping_total",
        "total", "total_tax", "customer_id", "payment_method",
        "date_created_gmt", "date_modified_gmt", "date_completed_gmt",
        "date_paid_gmt", "date_created", "date_modified",
        "date_completed", "date_paid", "total_cogs", "total_refunds",
        "transaction_cost"
    ]

    df = df[selected_columns]

    # Convert DataFrame to CSV
    csv_string = df.to_csv(index=False)
    
    # Define new path for the processed CSV
    new_path = blob.name.replace('Unprocessed', 'Processed/Finance')
    
    # Upload to Cloud Storage as CSV
    csv_blob = bucket.blob(new_path.replace('.json', '.csv'))
    csv_blob.upload_from_string(csv_string, content_type='text/csv')


@functions_framework.cloud_event
def process_json_to_csv(cloud_event):
    # print(base64.b64decode(cloud_event.data["message"]["data"]))

    path_name = f'{site_name}/Orders/Unprocessed/{current_year}/{current_month}'

    # blobs = storage_client.list_blobs(bucket_name, prefix=path_name, max_results=50)
    blobs = storage_client.list_blobs(bucket_name, prefix=path_name)

    if(blobs):
        for blob in blobs:
            process_blob_to_csv(blob)