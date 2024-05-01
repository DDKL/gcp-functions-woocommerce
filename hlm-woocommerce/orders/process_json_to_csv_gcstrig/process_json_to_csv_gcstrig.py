import functions_framework
import os
import pandas as pd 
import os
import json
import ast
from google.cloud import storage

# Initialize the storage client
storage_client = storage.Client()

site_initials = os.environ.get('site_initials') # must be either ihd, ihc, or can
destination_bucket_name = f'data-{site_initials}-processed'  # Destination bucket name

payment_methods_str = os.environ.get('payment_methods')

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
    return round(transaction_cost, 2)


@functions_framework.cloud_event
def process_json_to_csv_gcstrig(cloud_event):
    data = cloud_event.data

    # Extract file details from the event data
    source_bucket_name = data['bucket']
    file_name = data['name']

    # Ensure the file is a JSON file
    if not file_name.endswith('.json'):
        print(f"Skipped non-JSON file: {file_name}")
        return

    # Get the source bucket and blob
    source_bucket = storage_client.bucket(source_bucket_name)
    blob = source_bucket.blob(file_name)

    # Download JSON data from the blob
    json_bytes = blob.download_as_bytes()
    data = json.loads(json_bytes.decode('utf-8'))

    # Convert JSON to PD dataframe
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

    # Define new path for the processed CSV, replacing the file extension
    new_path = file_name.replace('.json', '.csv')

    # Get the destination bucket and create a new blob
    destination_bucket = storage_client.bucket(destination_bucket_name)
    new_blob = destination_bucket.blob(new_path)

    # Upload the CSV data
    new_blob.upload_from_string(csv_string, content_type='text/csv')

    print(f"File {file_name} converted to CSV and uploaded to {destination_bucket_name} as {new_path}.")

    # Get the destination bucket
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Copy the blob to the new bucket
    new_blob = source_bucket.copy_blob(blob, destination_bucket, file_name)

    print(f"File {file_name} copied to {destination_bucket_name}.")
