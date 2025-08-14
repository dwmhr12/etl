from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')))


# Import fungsi dari file Python lain
from ekstrak import run_ekstrak
from cleansing import run_cleansing
from chunking import run_chunk

def generate_embeddings_wrapper():
    from generate_embedding import generate_embeddings  
    generate_embeddings()
from insert_to_milvus import insert_to_milvus

with DAG(
    dag_id="ekstrak_pdf_dag",
    start_date=datetime(2025, 7, 29),
    schedule_interval=None,
    catchup=False,
    tags=["pdf", "ekstraksi"]
) as dag:

    ekstrak_task = PythonOperator(
        task_id="ekstrak_pdf",
        python_callable=run_ekstrak
    )

    cleansing_task = PythonOperator(
        task_id="cleansing",
        python_callable=run_cleansing
    )

    chunk_task = PythonOperator(
        task_id="chunk",
        python_callable=run_chunk
    )

    generate_embeddings_task = PythonOperator(
    task_id="generate_embeddings",
    python_callable=generate_embeddings_wrapper,
    )

    insert_to_milvus_task = PythonOperator(
        task_id="insert_to_milvus",
        python_callable=insert_to_milvus
    )

    ekstrak_task >> cleansing_task >> chunk_task >> generate_embeddings_task >> insert_to_milvus_task


globals()["dag"] = dag
