import pendulum
from datetime import datetime
from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator


@dag()
def indicadores(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False
):

    create_new_dataset = BigQueryCreateEmptyDatasetOperator(
        dataset_id='ds_indicadores',
        project_id='etl-indicadores',
        dataset_reference={"friendlyName": "Dataset para los indicadores"},
        gcp_conn_id='google_cloud_default',
        task_id='create-dataset',
        if_exists='ignore',
        dag=dag
    )

    @task()
    def extract() -> dict:
        """
        Extracts information from mindicador.cl
        """
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
    create_new_dataset >> load


indicadores()
