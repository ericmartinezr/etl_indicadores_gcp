# Proyecto de ETL de indicadores de Chile en GCP

Estos comandos son para uso local. Para el uso en GCP se debe utilizar el archivo cloudbuild.yaml.

# Configuración común (GCP y local)

Definición de variables comunes al proyecto

```bash
export PROJECT_ID="etl-indicadores"
export REGION="us-central1"
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
export BQ_DATASET="ds_indicadores"
export BQ_TABLE="tbl_indicadores"

gcloud config set project $PROJECT_ID
```

## Crear una cuenta de servicio para Composer y asignarle persmisos sobre Cloud Storage, BigQuery y Logging

```sh
gcloud iam service-accounts create airflow-app-sa \
  --description="Cuenta de servicio para Airflow"

SA_EMAIL_AF="airflow-app-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/storage.objectUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/bigquery.user"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/logging.logWriter"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/composer.worker"
```

## Crear una cuenta de servicio para Cloud Build y asignarle permisos sobre Cloud Build y Logging

```sh
gcloud iam service-accounts create cloudbuild-app-sa \
  --description="Cuenta de servicio para Cloud Build"

SA_EMAIL_CB="cloudbuild-app-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/storage.objectUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/cloudbuild.builds.editor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/logging.viewer"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/logging.logWriter"
```

- Referencia: para ver configuración de Cloud Build que no se abarca en este documento ver https://docs.cloud.google.com/composer/docs/composer-3/dag-cicd-github

## Eliminar tabla y dataset (opcional)

En caso de querer recrear el dataset y la tabla

```sh
bq rm --table=true $PROJECT_ID:$BQ_DATASET.$BQ_TABLE
bq rm --dataset=true $PROJECT_ID:$BQ_DATASET
```

## Crear dataset y tabla en BigQuery

```sh
bq mk --dataset --location=$REGION $PROJECT_ID:$BQ_DATASET

bq mk --table --clustering_fields=codigo,valor,fecha_valor \
--description="Tabla con indicadores historicos" \
--schema=./table_schema.json \
$PROJECT_ID:$BQ_DATASET.$BQ_TABLE
```

## Dar permisos al usuario sobre el dataset

Esto es necesario ya que si elimino el dataset y la tabla con el comando `bq`
se pierden todos los permisos.

```sh
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/bigquery.dataEditor"
```

# Configuración GCP

Específicamente Composer

```sh
EMAIL="my.email@gmail.com"

# Generar el ambiente de Composer
# Ref: https://docs.cloud.google.com/composer/docs/composer-3/known-issues
gcloud composer environments create etl-indicadores \
    --location $REGION \
    --image-version composer-3-airflow-2.10.5-build.23 \
    --service-account "${SA_EMAIL_AF}" \
    --environment-size large \
    --airflow-configs "^|^smtp-smtp_host=smtp.gmail.com|smtp-smtp_starttls=True|smtp-smtp_ssl=False|smtp-smtp_user=${EMAIL}|smtp-smtp_port=587|smtp-smtp_password_secret=smtp-password|smtp-smtp_mail_from=${EMAIL}"

# Importar las variables de ambiente al almacenamiento interno de Airflow
gcloud composer environments storage data import \
    --environment etl-indicadores \
    --location $REGION \
    --source=variables.json

# Cargar las variables en Airflow
gcloud composer environments run etl-indicadores \
    --location $REGION \
    variables import -- /home/airflow/gcs/data/variables.json

# Agregar pool para Airflow
gcloud composer environments run etl-indicadores \
    --location $REGION \
    pools set -- etl_api_mindicador_pool 2 "Pool para consumir API de mindicador.cl"
```

## SMTP

Las variables se deben asignar en la sección `Airflow configuration overrides`. En este caso Composer no permite setear la variable `smtp_password` debido a que quedan como texto plano en airflow.cfg lo que es inseguro. Para setear la password se usa otro método usando las variables `smtp_password_cmd` o `smtp_password_secret`.

Por conveniencia usé la segunda opción: `smtp_password_secret`

```sh
# Crear secreto con el password del smtp
echo -n "SMTP_PASSWORD" | gcloud secrets create \
  airflow-config-smtp-password \
  --data-file=- \
  --replication-policy=user-managed
  --locations=$REGION
```

- Referencia: https://docs.cloud.google.com/composer/docs/composer-3/configure-email#smtp_password

# Configuración local

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

## Instalar uv

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

## Crear usuario admin

```sh
airflow db migrate

airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin@admin.com
```

## Resetear usuario admin (opcional si hay problemas)

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

## Conexión GCP

En teoría esto debería ser solo en local.

> Admin > Connections

Buscar y editar `google_cloud_default`.
En el campo `Keyfile Path` ingresar `/home/eric/.config/gcloud/airflow.json` (la ruta donde se generó el key del paso más arriba)

## SMTP

Bajo la configuración `[smtp]` se configuró con el SMTP de Gmail usando una App Password

## Pool

Para evitar colapsar la API se asignó un pool de 3 slots. Esto se realiza desde la interfaz gráfica `Admin -> Pools`. Luego se referencia en el `@task` que realiza la consulta a la API.

## Serialización

Ya que estoy usando schemas de Pydantic es necesario registrarlos en la configuración para que Airflow pueda serializarlos/deserializarlos

```sh
nano ~/airflow/airflow.cfg

# Buscar la linea allowed_deserialization_classes
# Separar por coma
allowed_deserialization_classes = airflow.*, schemas.indicador_response.*
```

# Testing

Ejecutar los tests

```sh
python -m pytest ./tests
```
