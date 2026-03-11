from airflow import DAG
from airflow.sdk import task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from pathlib import Path

PROJECT_ID = "youtubelikes-488314"
DATASET = "habitos_personales"

with DAG(
    dag_id="sql_big_query",
    start_date=datetime(2024, 1, 1),
    schedule="0 15 * * *",  # airflow trabaja con utc y en chile es utc - 3 , entonces 15-3 = 12, para que se ejecute todos los días a las 12pm
    catchup=False,
) as dag:
    @task()
    def get_queries():
        queries = [
            {
                "table": "registro_habitos",
                "query": "SELECT * FROM registro_habitos"
            },
            {
                "table": "fecha",
                "query": "SELECT * FROM fecha"
            }
        ]
        return queries

    @task
    def extract_postgres(query_dict):
        postgres = PostgresHook(postgres_conn_id="postgres_local")

        df = postgres.get_pandas_df(query_dict["query"])

        output_dir = Path("/opt/airflow/data/parquet")
        output_dir.mkdir(parents=True, exist_ok=True)

        filepath = output_dir/f"{query_dict['table']}.parquet"

        df.to_parquet(filepath, index=False)

        return {
            "file_path": str(filepath),
            "table": query_dict["table"]
        }


    @task()
    def load_bigquery(data):
        file_path = data["file_path"]
        table = data["table"]

        bq = BigQueryHook(gcp_conn_id="big_query_conn")
        client = bq.get_client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET, # qué tipo de archivo se está subiendo.
            autodetect=True, # detecta automáticamente el schema de la tabla.
            write_disposition="WRITE_TRUNCATE", # Sobreescribe la tabla
        )

        with open(file_path, "rb") as f:
            job = client.load_table_from_file(
                f,
                f"{PROJECT_ID}.{DATASET}.{table}",
                job_config=job_config,  # le paso las configuraciones que defini antes
            )

        job.result()
        print(f"{job.output_rows} filas cargadas en BigQuery")

    queries = get_queries()
    data = extract_postgres.expand(query_dict=queries)
    load_bigquery.expand(data=data)