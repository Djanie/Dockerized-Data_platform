FROM apache/airflow:2.8.0  # or your Airflow base image version
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
