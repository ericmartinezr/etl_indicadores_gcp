import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_indicadores(dagbag):
    dag = dagbag.get_dag(dag_id="indicadores")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4
