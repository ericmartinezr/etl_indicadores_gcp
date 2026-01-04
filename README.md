# Proyecto de ETL de indicadores de Chile en GCP

Estos comandos son para uso local. Para el uso en GCP se debe utilizar el archivo cloudbuild.yaml.

## Preparación de ambiente

Definición de variables comunes al proyecto

```bash
export PROJECT_ID="etl-indicadores"
export REGION="us-central1"
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

gcloud config set project $PROJECT_ID
```

## Generar una cuenta de servicio en Google Cloud

```sh
gcloud iam service-accounts create airflow-app-sa \
  --description="Cuenta de servicio para Airflow"

SA_EMAIL="airflow-app-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

### Dar permisos para operar sobre BigQuery

```sh
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.user"
```

## Generar una llave para uso local

```sh
gcloud iam service-accounts keys create /home/eric/.config/gcloud/airflow.json --iam-account=airflow-app-sa@etl-indicadores.iam.gserviceaccount.com
```

## Actualizar la librería cryptography

Si se ve el siguiente mensaje de error

```
{processor.py:401} WARNING - Error when trying to pre-import module 'airflow.providers.google.cloud.operators.bigquery' found in <dag.py>: cffi library '_openssl' has no function, constant or global variable named 'Cryptography_HAS_SSL_VERIFY_CLIENT_POST_HANDSHAKE
```

Se debe ejecutar este comando

```sh
pip install --upgrade --force-reinstall --no-cache-dir cryptography pyOpenSSL cffi
```

### Instalar uv

Necesario para instalar la version espeficia que usa Composer 3

```sh
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION=3.11
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.11.8
uv venv .venv --python 3.11.8
source .venv/bin/activate
uv pip install "apache-airflow[google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
uv pip install psycopg2
```

### Crear usuario admin

```sh
airflow db migrate

airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin@admin.com
```

### Resetear usuario admin (opcional si hay problemas)

```sh
airflow db reset

airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin@admin.com
```

---

## Iniciar el webserver en modo daemon

```sh
airflow webserver --port 8080 -D
airflow scheduler -D
```

## Para detener el webserver

```sh
# Detener el webserver
kill $(cat ~/airflow/airflow-webserver.pid)

# Detener el scheduler
kill $(cat ~/airflow/airflow-scheduler.pid)

# Detener lo que quede vivo del webserver
ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs kill -9

# Detener lo que quede vivo del scheduler
lsof -i :8793 | sed 1d |  awk '{print $2}' | xargs kill -9
```

## Pool

Para evitar colapsar la API se asignó un pool de 3 slots. Esto se realiza desde la interfaz gráfica `Admin -> Pools`. Luego se referencia en el `@task` que realiza la consulta a la API.

## Testing

Ejecutar los tests

```sh
python -m pytest ./tests
```
