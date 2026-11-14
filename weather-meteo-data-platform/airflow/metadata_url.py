"""Build Airflow's metadata connection URL from environment variables."""

import os
import sys

from sqlalchemy.engine import URL

sys.stdout.write(URL.create(
    "postgresql+psycopg2",
    username=os.environ["AIRFLOW_DB_USER"],
    password=os.environ["AIRFLOW_DB_PASSWORD"],
    host="postgres_db",
    port=5432,
    database=os.environ["AIRFLOW_DB_NAME"],
).render_as_string(hide_password=False))
