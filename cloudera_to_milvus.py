from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='cloudera_to_milvus_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['AI', 'Production'],
) as dag:

    run_pipeline = KubernetesPodOperator(
        namespace='airflow',
        image="python:3.9-slim",
        cmds=["bash", "-c"],
        arguments=[
            "pip install -q trino pymilvus && python -c '"
            "import trino; "
            "from pymilvus import connections, utility; "
            "print(\"--- Connecting to Cloudera via Trino ---\"); "
            "conn = trino.dbapi.connect(host=\"my-trino.trino.svc.cluster.local\", port=8080, user=\"airflow\"); "
            "cur = conn.cursor(); cur.execute(\"SHOW SCHEMAS FROM cloudera\"); print(cur.fetchall()); "
            "print(\"--- Connecting to Milvus ---\"); "
            "connections.connect(host=\"my-milvus.milvus.svc.cluster.local\", port=19530); "
            "print(utility.list_collections()); "
            "print(\"✅ SUCCESS!\")'"
        ],
        name="ai-pipeline-task",
        task_id="fetch_and_embed",
        get_logs=True,
        is_delete_operator_pod=True,
    )
