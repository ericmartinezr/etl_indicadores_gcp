import pendulum
import logging
import httpx
import json
from typing import Optional
from pydantic import ValidationError
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# from schemas.indicador_response import IndicadorResponse


# TODO: Move this to Variables?
PROJECT_ID = "etl-indicadores"
DATASET_ID = "ds_indicadores"
TABLE_ID = "tbl_indicadores"
BUCKET_ID = "indicadores-bucket"

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

    insert_to_table = BigQueryInsertJobOperator(
        task_id="insert-to-table",
        configuration={
                "load": {
                    "sourceUris": ["{{ ti.xcom_pull(task_ids='transform') }}"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET_ID,
                        "tableId": TABLE_ID
                    },
                    "sourceFormat": "NEWLINE_DELIMITED_JSON",
                    "writeDisposition": "WRITE_APPEND",
                    "autodetect": True
                }
        }
    )

    @task(pool="etl_api_mindicador_pool")
    def extract(ds: Optional[str] = None) -> dict:  # IndicadorResponse:
        """
        Extracts information from mindicador.cl
        """

        # Get the year from the data
        # ds format = yyyy-mm-dd
        yyyy = ds.split("-")[0]
        api_url = f"https://mindicador.cl/api/uf/{yyyy}"

        response = httpx.get(api_url)
        if response.status_code != 200:
            raise AirflowFailException(f"No existen datos para el año {yyyy}")

        try:
            # Aparentemente si se pasa algun año sin datos (e.g., 1600) en vez de retornar un 404 o 500
            # simplemente retorna un json con el arreglo "serie" vacio
            res_json = response.json()  # IndicadorResponse.model_validate_json(response.json())
            if not res_json["serie"]:
                # El mismo error anterior pero para un caso distinto
                raise AirflowFailException(
                    f"No existen datos para el año {yyyy}")

            return res_json.model_dump()
        except ValidationError as ve:
            logging.error("El formato de respuesta es incorrecto")
            logging.error(ve, exc_info=True)
            raise AirflowFailException(
                f"El formato de respuesta es incorrecto")

    @task()
    def transform(extract_data: dict, ds: Optional[str] = None) -> str:
        """
        Transforms de data extracted from mindicador.cl
        """
        rows = []
        extract_serie = extract_data["serie"]
        for serie in extract_serie:
            rows.append({
                "codigo": extract_data["codigo"],
                "nombre": extract_data["nombre"],
                "unidad_medida": extract_data["unidad_medida"],
                "valor": serie["valor"],
                "fecha_valor": serie["fecha"]
            })

        # Indicador
        # TODO: Esto no deberia estar en duro
        indicador = "UF"
        fecha = "".join([x for x in ds.split("-")])
        object_name = f"{indicador}_{fecha}.csv"

        hook = GCSHook()
        hook.upload(
            bucket_name=BUCKET_ID,
            object_name=f"{indicador}_{fecha}.csv",
            data=json.dumps(rows),
            encoding="utf-8"
        )

        return f"gs://{BUCKET_ID}/{object_name}"

    extract_data = extract()
    transform_data = transform(extract_data)
    check_table_exists >> transform_data >> insert_to_table
