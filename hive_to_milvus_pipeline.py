
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="hive_data_fetch_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["Hive", "Trino", "AI"],
) as dag:

    fetch_hive_data = KubernetesPodOperator(
        task_id="fetch_hive_data",
        name="fetch-hive-data",
        namespace="airflow",

        image="python:3.9-slim",

        cmds=["bash", "-c"],

        arguments=[
            """
            pip install -q trino pandas && python -c '

import trino
import pandas as pd

print("=== Connecting to Trino ===")

conn = trino.dbapi.connect(
    host="my-trino.trino.svc.cluster.local",
    port=8080,
    user="airflow",
    catalog="hive",
    schema="default"
)

cur = conn.cursor()

print("=== Running Query ===")

query = """
SELECT *
FROM gamal.orders
LIMIT 10
"""

cur.execute(query)

rows = cur.fetchall()

print("=== Retrieved Data ===")

for row in rows:
    print(row)

print(f"Total Rows Fetched: {len(rows)}")

print("✅ Hive fetch successful!")

'
            """
        ],

        get_logs=True,
        is_delete_operator_pod=True,
    )

    fetch_hive_data
