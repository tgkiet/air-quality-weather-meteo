FROM apache/airflow:3.2.0

# Copy the requirements file into the container
COPY requirements_airflow.txt /requirements_airflow.txt

# Install dependencies using the airflow user
RUN pip install --no-cache-dir -r /requirements_airflow.txt
