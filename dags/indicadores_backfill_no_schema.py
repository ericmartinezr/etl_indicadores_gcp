import pendulum
import logging
import httpx
import json
from datetime import timedelta
from typing import Optional
from pydantic import ValidationError
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.db import LazySelectSequence
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


with DAG(
    dag_id="indicadores_backfill_no_schema",
    schedule="@yearly",
    # La API tiene datos desde 1928 (IPC) en adelante
    start_date=pendulum.datetime(1928, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    default_args={
        "retries": 1,
        "email": [Variable.get("email")],
        "email_on_failure": True,
        "execution_timeout": timedelta(minutes=1)
    }
) as dag:

    # Inserta el resultado a BigQuery
    insert_to_table = BigQueryInsertJobOperator(
        task_id="insert-to-table",
        configuration={
                "load": {
                    "sourceUris": ["{{ task_instance.xcom_pull(task_ids='save-to-gcs') }}"],
                    "destinationTable": {
                        "projectId": Variable.get("project"),
                        "datasetId": Variable.get("dataset"),
                        "tableId": Variable.get("table")
                    },
                    "sourceFormat": "CSV",
                    "writeDisposition": "WRITE_APPEND",
                    "autodetect": False
                }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Valida que la insercion se haya realizado correctamente
    check_table = BigQueryCheckOperator(
        task_id='check-table',
        sql="""
            SELECT 
                COUNT(*) 
            FROM 
                `{{var.value.get('project')}}.{{var.value.get('dataset')}}.{{var.value.get('table')}}`
            WHERE
                fecha_valor 
            BETWEEN
                "{{macros.ds_format(ds, '%Y-%m-%d', '%Y')}}-01-01" and "{{macros.ds_format(ds, '%Y-%m-%d', '%Y')}}-12-31"
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # Elimina de BigQuery el periodo (año) que se está procesando en caso de reproceso
    delete_from_table = BigQueryInsertJobOperator(
        task_id="delete-from-table",
        configuration={
                "query": {
                    "query": """
                    DELETE FROM 
                        `{{var.value.get('project')}}.{{var.value.get('dataset')}}.{{var.value.get('table')}}` 
                    WHERE 
                        fecha_valor 
                    BETWEEN 
                        "{{macros.ds_format(ds, '%Y-%m-%d', '%Y')}}-01-01" and "{{macros.ds_format(ds, '%Y-%m-%d', '%Y')}}-12-31"
                    """,
                    "useLegacySql": False
                }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Elimina el archivo de Cloud Storage luego de cargarlo en BigQuery
    delete_file = GCSDeleteObjectsOperator(
        task_id="delete-file",
        bucket_name=Variable.get("bucket"),
        objects=[
            "{{ task_instance.xcom_pull(task_ids='save-to-gcs').split('/')[-1] }}"],
        gcp_conn_id="google_cloud_default",
    )

    @task(pool="etl_api_mindicador_pool",
          max_active_tis_per_dag=1)
    def extract(indicator_type: str, ds: Optional[str] = None) -> dict:
        """
        Extrae datos de indicadores desde mindicador.cl
        """

        # Obtiene el año del logical date
        yyyy = ds.split("-")[0]
        api_url = f"https://mindicador.cl/api/{indicator_type}/{yyyy}"

        response = httpx.get(api_url, timeout=60)
        if response.status_code != 200:
            raise AirflowFailException(f"No existen datos para el año {yyyy}")

        try:
            # Aparentemente si se pasa alguna fecha sin datos (e.g., 1600-01-01)
            # en vez de retornar un 404 o 500
            # simplemente retorna un json con el arreglo "serie" vacio
            res_json = response.json()

            if not res_json["serie"]:
                # El mismo error anterior pero para un caso distinto
                raise AirflowSkipException(
                    f"No existen datos para el año {yyyy}")

            return res_json
        except ValidationError as ve:
            logging.error("El formato de respuesta es incorrecto")
            logging.error(ve, exc_info=True)
            raise AirflowFailException(
                f"El formato de respuesta es incorrecto")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def transform(extract_data: dict) -> str:
        """
        Transforma los datos obtrenidos desde from mindicador.cl
        """
        rows = []
        extract_serie = extract_data["serie"]
        for serie in extract_serie:
            rows.append("{codigo},{nombre},{um},{val},{fecval}"
                        .format(codigo=extract_data["codigo"],
                                nombre=extract_data["nombre"],
                                um=extract_data["unidad_medida"],
                                val=serie["valor"],
                                fecval=serie["fecha"][:10]))

        # La API en algunos casos retorna valores duplicados
        # Por ejemplo UF 2015-07-11
        rows = list(set(rows))
        return "\n".join(rows)

    @task(task_id='save-to-gcs')
    def save_to_gcs(sequence: LazySelectSequence, ds: Optional[str] = None) -> str:
        """"
        Guarda el resultado en un archivo en Cloud Storage
        """
        bucket = Variable.get("bucket")
        rows = "\n".join([row for row in sequence])
        fecha = "".join([x for x in ds.split("-")])
        object_name = f"indicadores_{fecha}.csv"

        hook = GCSHook(
            gcp_conn_id="google_cloud_default",
        )
        hook.upload(
            bucket_name=bucket,
            object_name=object_name,
            data=rows,
            encoding="utf-8",
        )

        return f"gs://{bucket}/{object_name}"

    indicators = Variable.get("indicators", deserialize_json=True)

    extract_data = extract.expand(indicator_type=indicators)
    transform_data = transform.expand(extract_data=extract_data)
    gcs_data = save_to_gcs(transform_data)
    gcs_data >> delete_from_table >> insert_to_table >> check_table >> delete_file
