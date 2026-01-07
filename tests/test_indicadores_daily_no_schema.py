import pytest
import re
from airflow.models import DagBag
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)

PROJECT_ID = "etl-indicadores"
DATASET_ID = "ds_indicadores"
TABLE_ID = "tbl_indicadores"


@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag()


@pytest.fixture()
def dag(dagbag: DagBag) -> (DAG | None):
    return dagbag.get_dag(dag_id="indicadores_no_schema")


def test_dag(dag, dagbag):
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 7

    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries >= 1, f"Task {task.task_id} in {dag_id} must have retries >= 1"


def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag_structure(dag):
    assert_dag_dict_equal(
        {
            "extract": ["transform"],
            "transform": ["save-to-gcs"],
            "save-to-gcs": ["delete-from-table"],
            "delete-from-table": ["insert-to-table"],
            "insert-to-table": ["check-table"],
            "check-table": ["delete-file"],
            "delete-file": [],
        },
        dag,
    )


def clean_sql(sql: str) -> str:
    return re.sub(r'\s+', ' ', sql.replace("\n", " ")).strip()


def test_check_table_query(dag: DAG):
    mock_context = {
        "var": {
            "value": {
                "project": PROJECT_ID,
                "dataset": DATASET_ID,
                "table": TABLE_ID
            }
        },
        "ds": "2026-01-01"
    }
    task = dag.get_task('check-table')
    task.render_template_fields(context=mock_context)
    sql = task.sql
    expected_sql = """
    SELECT 
        COUNT(*) 
    FROM 
        `etl-indicadores.ds_indicadores.tbl_indicadores`
    WHERE
        fecha_valor = "2026-01-01"
    """

    assert clean_sql(sql) == clean_sql(expected_sql)
