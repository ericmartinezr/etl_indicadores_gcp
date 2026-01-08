import pytest
from unittest.mock import patch
from airflow.models import DagBag
from airflow import DAG
from airflow.models import Variable


@pytest.fixture()
def dagbag() -> DagBag:
    with patch.dict('os.environ',
                    AIRFLOW_VAR_EMAIL="myemail@example.com",
                    AIRFLOW_VAR_PROJECT="etl-indicadores",
                    AIRFLOW_VAR_DATASET="ds_indicadores",
                    AIRFLOW_VAR_TABLE="tbl_indicadores",
                    AIRFLOW_VAR_BUCKET="indicadores-bucket",
                    AIRFLOW_VAR_INDICATORS='["uf", "ipc"]'):

        assert "myemail@example.com" == Variable.get("email")
        assert "etl-indicadores" == Variable.get("project")
        assert "ds_indicadores" == Variable.get("dataset")
        assert "tbl_indicadores" == Variable.get("table")
        assert "indicadores-bucket" == Variable.get("bucket")
        assert ["uf", "ipc"] == Variable.get(
            "indicators", deserialize_json=True)

        return DagBag(dag_folder="dags/", read_dags_from_db=False, include_examples=False)


@pytest.fixture()
def dag(dagbag: DagBag) -> (DAG | None):
    return dagbag.get_dag(dag_id="indicadores_backfill")


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
            "insert-to-table": ["check-table"],
            "check-table": ["delete-file"],
            "delete-from-table": ["insert-to-table"],
            "delete-file": [],
            "extract": ["transform"],
            "transform": ["save-to-gcs"],
            "save-to-gcs": ["delete-from-table"],
        },
        dag,
    )
