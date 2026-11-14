FROM apache/airflow:slim-3.4.0-python3.13

# Install the FAB provider with the constraints shipped in the base image.
RUN pip install --no-cache-dir --constraint "${HOME}/constraints.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-fab==3.10.2 \
    apache-airflow-providers-postgres==7.1.1 asyncpg psycopg2-binary

COPY requirements_airflow.txt /requirements_airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements_airflow.txt \
    && pip check

COPY --chmod=0755 airflow/start.sh /opt/airflow/start.sh
COPY airflow/metadata_url.py /opt/airflow/metadata_url.py
