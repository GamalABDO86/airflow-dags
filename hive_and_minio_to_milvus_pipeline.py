
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='hive_to_milvus_pipeline',
    default_args=default_args,
    schedule=None,     
    catchup=False,
    tags=['AI', 'Local_Hive_Source'],
) as dag:

    run_pipeline = KubernetesPodOperator(
        namespace='airflow',
        image="python:3.9-slim",
        cmds=["bash", "-c"],
        arguments=[
            "pip install -q trino pymilvus sentence-transformers && python -c '"
            "import trino\n"
            "import numpy as np\n"
            "from pymilvus import connections, utility, FieldSchema, CollectionSchema, DataType, Collection\n"
            "from sentence_transformers import SentenceTransformer\n"
            "\n"
            "print(\"=== 1. Connecting to Local Hive via Trino Engine ===\");\n"
            "conn = trino.dbapi.connect(\n"
            "    host=\"my-trino.trino.svc.cluster.local\",\n"
            "    port=8080,\n"
            "    user=\"airflow\"\n"
            ");\n"
            "cur = conn.cursor();\n"
            "\n"
            "cur.execute(\"CREATE SCHEMA IF NOT EXISTS hive.gamal\");\n"
            "cur.execute(\"\"\"\n"
            "    CREATE TABLE IF NOT EXISTS hive.gamal.production_data (\n"
            "        id BIGINT,\n"
            "        text_content VARCHAR\n"
            "    )\n"
            "\"\"\");\n"
            "\n"
            "query = \"SELECT id, text_content FROM hive.gamal.production_data WHERE text_content IS NOT NULL LIMIT 100\";\n"
            "cur.execute(query);\n"
            "rows = cur.fetchall();\n"
            "print(f\"Fetched {len(rows)} records from Local Hive.\");\n"
            "\n"
            "if not rows:\n"
            "    print(\"⚠️ Hive Table is currently empty. Please insert data into hive.gamal.production_data to run embeddings.\");\n"
            "    exit(0);\n"
            "\n"
            "ids = [r[0] for r in rows]; texts = [r[1] for r in rows];\n"
            "\n"
            "print(\"=== 2. Transforming Text into AI Vectors (Embeddings) ===\");\n"
            "model = SentenceTransformer(\"all-MiniLM-L6-v2\");\n"
            "embeddings = model.encode(texts, show_progress_bar=False).tolist();\n"
            "dimension = len(embeddings[0]);\n"
            "\n"
            "print(\"=== 3. Connecting to Milvus Vector Database ===\");\n"
            "connections.connect(host=\"my-milvus.milvus.svc.cluster.local\", port=19530);\n"
            "\n"
            "collection_name = \"hive_gamal_embeddings\";\n"
            "if utility.has_collection(collection_name): utility.drop_collection(collection_name);\n"
            "\n"
            "fields = [\n"
            "    FieldSchema(name=\"id\", dtype=DataType.INT64, is_primary=True, auto_id=False),\n"
            "    FieldSchema(name=\"vector\", dtype=DataType.FLOAT_VECTOR, dim=dimension),\n"
            "    FieldSchema(name=\"raw_text\", dtype=DataType.VARCHAR, max_length=500)\n"
            "]\n"
            "schema = CollectionSchema(fields, \"AI Pipeline data powered by Local Hive and MinIO\");\n"
            "collection = Collection(name=collection_name, schema=schema);\n"
            "\n"
            "print(\"=== 4. Shipping Vectors and Meta-data to Milvus ===\");\n"
            "mr = collection.insert([ids, embeddings, texts]);\n"
            "print(f\"Successfully loaded {mr.insert_count} vectors into Milvus.\");\n"
            "\n"
            "print(\"=== 5. Building Vector IVF_FLAT Index for Fast AI Search ===\");\n"
            "index_params = {\"metric_type\": \"L2\", \"index_type\": \"IVF_FLAT\", \"params\": {\"nlist\": 128}};\n"
            "collection.create_index(field_name=\"vector\", index_params=index_params);\n"
            "print(\"✅ PIPELINE EXECUTED SUCCESSFULLY!\")'"
        ],
        name="hive-ai-processor",
        task_id="fetch_embed_and_load",
        get_logs=True,
        is_delete_operator_pod=True, 
    )
