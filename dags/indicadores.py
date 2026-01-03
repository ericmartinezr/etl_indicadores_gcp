import pendulum
import logging
import httpx
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

# TODO: Move this to Variables?
PROJECT_ID = "etl-indicadores"
DATASET_ID = "ds_indicadores"
TABLE_ID = "tbl_indicadores"

with DAG(
    dag_id="indicadores",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    max_active_runs=5,
    default_args={
        "retries": 1
    }
) as dag:

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check-table-exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        gcp_conn_id="google_cloud_default",
    )

    @task(pool="etl_api_mindicador_pool")
    def extract(ds=None) -> dict:
        """
        Extracts information from mindicador.cl
        """

        # Get the year from the data
        # ds format = yyyy-mm-dd
        yyyy = ds.split("-")[0]
        api_url = f"https://mindicador.cl/api/uf/{yyyy}"

        response = httpx.get(api_url)
        if response.status_code != 200:
            # TODO: Buscar mejor excepcion, una que permite saltar al siguiente año
            # sin cancelar la ejecucion del pipeline
            raise AirflowFailException(f"No existen datos para el año {yyyy}")

        logging.info(f"Logical date: {ds}")
        logging.info(f"api url {api_url}")
        return {}

    @task()
    def transform(extract_data: dict) -> dict:
        """
            Transforms de data extracted from mindicador.cl
            """
        return {}

    @task()
    def load(transformed_data: dict):
        """
            Loads the data to BigQuery
            """
        return {}

    extract_data = extract()
    transform_data = transform(extract_data)
    check_table_exists >> load(transform_data)
