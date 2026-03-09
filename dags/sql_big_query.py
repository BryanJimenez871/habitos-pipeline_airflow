from airflow import DAG
from airflow.sdk import task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import tempfile
import os

default_args = {"owner": "bryan"}

PROJECT_ID = "youtubelikes-488314"
DATASET = "habitos_personales"
TABLE = "registro_habitos"

with DAG(
    dag_id="sql_big_query",
    start_date=datetime(2024, 1, 1),
    schedule="0 15 * * *",  # airflow trabaja con utc y en chile es utc - 3 , entonces 15-3 = 12, para que se ejecute todos los días a las 12pm
    catchup=False,
) as dag:

    @task()
    def extract_postgres():
        postgres = PostgresHook(postgres_conn_id="postgres_local")
        df = postgres.get_pandas_df("SELECT * FROM registro_habitos")
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df.to_parquet(tmp_file.name, index=False)
        return tmp_file.name # con .name devuelve la ruta del archivo en el sistema, sino fuera por el .name se retorna la dirección del objeto  <tempfile._TemporaryFileWrapper object at 0x...>

    @task()
    def load_bigquery(file_path):
        bq = BigQueryHook(gcp_conn_id="big_query_conn")
        client = bq.get_client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET, # qué tipo de archivo se está subiendo.
            autodetect=True, # detecta automáticamente el schema de la tabla.
            write_disposition="WRITE_APPEND",
        )

        with open(file_path, "rb") as f: # rb -> read binary, el parquet es un archivo binario
            job = client.load_table_from_file(
                f, #el archivo abierto
                f"{PROJECT_ID}.{DATASET}.{TABLE}", # donde ira la tabla
                job_config=job_config, # le paso las configuraciones que defini antes
            )

        job.result() # bloquea la ejecución hasta que BigQuery termine de cargar los datos.
        os.unlink(file_path)  # limpia el temp, es decir el parquet creado existe solo para cargarlo en bigquery, el archivo temporal se elimina
        print(f"{job.output_rows} filas cargadas en BigQuery")

    file_path = extract_postgres()
    load_bigquery(file_path)