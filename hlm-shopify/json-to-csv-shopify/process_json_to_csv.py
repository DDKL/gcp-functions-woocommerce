# import functions_framework
import pandas as pd
from datetime import datetime
from google.cloud import storage
from google.cloud import firestore
import json
import requests
import time
import ast
import os

# Initialize Google Cloud Storage client
storage_client = storage.Client()
db = firestore.Client()

# Set environment variables

site_name = os.environ.get('site_name')
payment_methods_str = os.environ.get('payment_methods')
shopify_store = os.environ.get('shopify_store')
shopify_token = os.environ.get('shopify_token')
bucket_name = os.environ.get('bucket_name')
bucket = storage_client.get_bucket(bucket_name)

api_version = "2024-04"

current_year, current_month = datetime.now().year, datetime.now().month

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

def get_inventory_item_id(product_id):
    url = f'https://{shopify_store}.myshopify.com/admin/api/{api_version}/products/{product_id}/variants.json'
    headers = {'X-Shopify-Access-Token': shopify_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        variants = response.json().get('variants', [])
        if variants:
            return variants[0].get('inventory_item_id')
    except requests.RequestException as e:
        print(f"Error fetching inventory item ID for product {product_id}: {e}")
    return None

def get_cost(inventory_item_id):
    url = f'https://{shopify_store}.myshopify.com/admin/api/{api_version}/inventory_items/{inventory_item_id}.json'
    headers = {'X-Shopify-Access-Token': shopify_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        inventory_item = response.json().get('inventory_item', {})
        cost = inventory_item.get('cost', 0)
        return float(cost) if cost is not None else 0
    except requests.RequestException as e:
        print(f"Error fetching cost for inventory item {inventory_item_id}: {e}")
    return 0

def calculate_cost_of_goods(order):
    total_cost = 0
    for item in order.get('line_items', []):
        product_id = item.get('product_id')
        quantity = item.get('quantity', 1)
        if product_id:
            inventory_item_id = get_inventory_item_id(product_id)
            if inventory_item_id:
                cost = get_cost(inventory_item_id)
                total_cost += cost * quantity
    return total_cost

def calculate_transaction_costs(total, payment_method):
    rate = payment_methods.get(payment_method, 0) / 100  # Convert percentage to a decimal
    transaction_cost = float(total) * rate
    if payment_method in ['shopify_installments', 'shopify_payments']:
        transaction_cost += 0.3
    return round(transaction_cost, 2)


def process_blob_to_csv(blob):
    json_bytes = blob.download_as_bytes()
    data_str = json_bytes.decode('utf-8')

    try:
        data = json.loads(data_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from blob {blob.name}: {e}")
        return

    # If the data is a dictionary, convert it to a list
    if isinstance(data, dict):
        data = [data]

    for order in data:
        order['cost_of_goods'] = calculate_cost_of_goods(order)
        time.sleep(1)  # Add 1 second delay between processing each order

    df = pd.json_normalize(data)

    def get_transaction_cost(row):
        total_price = pd.to_numeric(row['total_price'])
        payment_methods_list = row.get('payment_gateway_names', [])
        if payment_methods_list:
            payment_method = payment_methods_list[0]
        else:
            payment_method = None
        return calculate_transaction_costs(total_price, payment_method)

    df['transaction_cost'] = df.apply(get_transaction_cost, axis=1)
    # df['transaction_cost'] = df.apply(lambda row: calculate_transaction_costs(pd.to_numeric(row['total_price']), row['payment_gateway_names'][0]), axis=1)
    
    selected_columns = [
        "id", "created_at", "currency", "total_price", "subtotal_price",
        "total_tax", "cancelled_at", "closed_at", "confirmed", "order_number",
        "payment_gateway_names", "processed_at", "total_discounts", "cost_of_goods",
        "transaction_cost"
    ]

    df = df[selected_columns]

    csv_string = df.to_csv(index=False)
    new_path = blob.name.replace('Unprocessed', 'Processed/Finance')
    csv_blob = bucket.blob(new_path.replace('.json', '.csv'))
    csv_blob.upload_from_string(csv_string, content_type='text/csv')

def process_json_to_csv():
    state_doc_ref = db.collection(f'{site_name}-processing_state').document('shopify_orders-gcspage')
    state_doc = state_doc_ref.get()

    last_processed_blob = state_doc.to_dict().get('last_processed_blob', '') if state_doc.exists else ''
    path_name = f'{site_name}/Orders/Unprocessed/{current_year}/{current_month}/'
    blobs = storage_client.list_blobs(bucket_name, prefix=path_name)
    file_count = 0

    for blob in blobs:
        if blob.name > last_processed_blob:
            process_blob_to_csv(blob)
            last_processed_blob = blob.name
            file_count += 1
            if file_count >= 1000:
                break

    state_doc_ref.set({'last_processed_blob': last_processed_blob})