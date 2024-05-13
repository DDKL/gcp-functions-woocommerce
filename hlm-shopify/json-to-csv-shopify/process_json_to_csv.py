import functions_framework
import pandas as pd 
from datetime import datetime
from google.cloud import storage
from google.cloud import firestore
import os
import json
import ast

# Initialize Google Cloud Storage client
storage_client = storage.Client()
db = firestore.Client()

# bucket_name = os.environ.get('bucket_name')
site_name = os.environ.get('site_name')
payment_methods_str = os.environ.get('payment_methods')
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)


if payment_methods_str:
    try:
        # Convert the string representation of the dictionary back to a dictionary
        payment_methods = ast.literal_eval(payment_methods_str)
    except (ValueError, SyntaxError):
        print("Error: The environment variable 'payment_methods' does not contain a valid dictionary.")
        # Handle the error appropriately
else:
    print("Error: Environment variable 'payment_methods' is not set.")
    # Handle the missing environment variable appropriately

current_year, current_month = datetime.now().year, datetime.now().month

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
    rate = payment_methods.get(payment_method, 0) / 100  # Convert percentage to a decimal
    transaction_cost = float(total) * rate
    transaction_cost = 0.3
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
    state_doc_ref = db.collection(f'{site_name}-processing_state').document('woocommerce_orders-gcspage')
    state_doc = state_doc_ref.get()

    # Check if the Firestore document exists
    if state_doc.exists:
        last_processed_blob = state_doc.to_dict().get('last_processed_blob', '')
    else:
        print(f"No existing document for {site_name}, starting from the beginning.")
        last_processed_blob = ''

    path_name = f'{site_name}/Orders/Unprocessed/{current_year}/{current_month}/'
    blobs = storage_client.list_blobs(bucket_name, prefix=path_name)
    file_count = 0

    for blob in blobs:
        # Process only if the blob is after the last processed one
        if blob.name > last_processed_blob:
            process_blob_to_csv(blob)
            last_processed_blob = blob.name
            file_count += 1
            if file_count >= 1000:
                break

    # Update the last processed blob in Firestore
    state_doc_ref.set({'last_processed_blob': last_processed_blob})
