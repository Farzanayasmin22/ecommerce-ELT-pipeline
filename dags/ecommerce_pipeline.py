from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # CLEAN ORDERS
    clean_orders = BigQueryInsertJobOperator(
        task_id="clean_orders",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `airflow-project-1-494605.cleaned_ecommerce.clean_orders` AS
                SELECT
                  SAFE_CAST(order_id AS STRING) AS order_id,
                  SAFE_CAST(customer_id AS STRING) AS customer_id,
                  SAFE_CAST(product_id AS STRING) AS product_id,
                  SAFE_CAST(quantity AS INT64) AS quantity,
                  LOWER(TRIM(status)) AS status,
                  DATE(order_date) AS order_date
                FROM `airflow-project-1-494605.raw_ecommerce.raw_orders`
                WHERE order_id IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
        location="asia-south1",
        project_id="airflow-project-1-494605"
    )

    # CLEAN CUSTOMERS
    clean_customers = BigQueryInsertJobOperator(
        task_id="clean_customers",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `airflow-project-1-494605.cleaned_ecommerce.clean_customers` AS
                SELECT
                  SAFE_CAST(customer_id AS STRING) AS customer_id,
                  INITCAP(TRIM(CONCAT(first_name, ' ', last_name))) AS full_name,
                  LOWER(TRIM(email)) AS email,
                  INITCAP(TRIM(city)) AS city,
                  signup_date
                FROM `airflow-project-1-494605.raw_ecommerce.raw_customers`
                WHERE customer_id IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
        location="asia-south1",
        project_id="airflow-project-1-494605"
    )

    # CLEAN PRODUCTS
    clean_products = BigQueryInsertJobOperator(
        task_id="clean_products",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `airflow-project-1-494605.cleaned_ecommerce.clean_products` AS
                SELECT
                  SAFE_CAST(product_id AS STRING) AS product_id,
                  INITCAP(TRIM(product_name)) AS product_name,
                  INITCAP(TRIM(category)) AS category,
                  SAFE_CAST(unit_price AS FLOAT64) AS unit_price
                FROM `airflow-project-1-494605.raw_ecommerce.raw_products`
                WHERE product_id IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
        location="asia-south1",
        project_id="airflow-project-1-494605"
    )

    # FINAL TABLE
    final_table = BigQueryInsertJobOperator(
        task_id="final_table",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `airflow-project-1-494605.final_ecommerce.analytics_ecommerce` AS
                SELECT 
                  o.order_id,
                  o.order_date,
                  EXTRACT(YEAR FROM o.order_date) AS order_year,
                  EXTRACT(MONTH FROM o.order_date) AS order_month,
                  o.status,
                  c.customer_id,
                  c.full_name,
                  c.email,
                  c.city,
                  p.product_id,
                  p.product_name,
                  p.category,
                  p.unit_price,
                  o.quantity,
                  (o.quantity * p.unit_price) AS total_amount,
                  CASE
                    WHEN (o.quantity * p.unit_price) > 1000 THEN 'High purchase'
                    WHEN (o.quantity * p.unit_price) > 500 THEN 'Medium purchase'
                    ELSE 'Low purchase'
                  END AS order_value_category
                FROM `airflow-project-1-494605.cleaned_ecommerce.clean_orders` o
                LEFT JOIN `airflow-project-1-494605.cleaned_ecommerce.clean_customers` c
                  ON o.customer_id = c.customer_id
                LEFT JOIN `airflow-project-1-494605.cleaned_ecommerce.clean_products` p
                  ON o.product_id = p.product_id
                """,
                "useLegacySql": False,
            }
        },
        location="asia-south1",
        project_id="airflow-project-1-494605"
    )

    [clean_orders, clean_customers, clean_products] >> final_table
