import functions_framework
from google.cloud import bigquery, storage
import pandas as pd
import io

PROJECT_ID = "airflow-project-1-494605"
DATASET = "raw_ecommerce"

bq_client = bigquery.Client()
storage_client = storage.Client()


# ✅ Create dataset if not exists
def create_dataset():
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-south1"

    try:
        bq_client.get_dataset(dataset_id)
        print("Dataset already exists")
    except:
        bq_client.create_dataset(dataset)
        print("Dataset created")


# ✅ Decide table based on file name
def get_table_name(file_name):
    name = file_name.lower()

    if "orders" in name:
        return "raw_orders"
    elif "customers" in name:
        return "raw_customers"
    elif "products" in name:
        return "raw_products"
    elif "payments" in name:
        return "raw_payments"
    else:
        return None


# ✅ Main function
@functions_framework.cloud_event
def load_to_bq(cloud_event):

    bucket = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    print(f"Processing file: {file_name}")

    # 🔥 Step 1: Ensure dataset exists
    create_dataset()

    # 🔥 Step 2: Decide table
    table = get_table_name(file_name)

    if not table:
        print("❌ File not recognized")
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{table}"

    # 🔹 CASE 1: CSV / JSON
    if file_name.endswith(".csv") or file_name.endswith(".json"):

        uri = f"gs://{bucket}/{file_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=(
                bigquery.SourceFormat.CSV
                if file_name.endswith(".csv")
                else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            ),
            skip_leading_rows=1 if file_name.endswith(".csv") else 0,
            write_disposition="WRITE_APPEND",
        )

        job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        job.result()

        print(f"✅ Loaded {file_name} → {table}")

    # 🔹 CASE 2: Excel
    elif file_name.endswith(".xlsx"):

        blob = storage_client.bucket(bucket).blob(file_name)
        file_bytes = blob.download_as_bytes()

        df = pd.read_excel(io.BytesIO(file_bytes))

        job = bq_client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
        )
        job.result()

        print(f"✅ Excel loaded → {table}")

    else:
        print("❌ Unsupported file type")
