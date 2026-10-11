
FROM apache/airflow:2.10.5-python3.11

USER airflow

RUN pip install --no-cache-dir \
    "pandas==2.2.3" \
    "pyspark==3.5.3" \
    "requests==2.32.3" \
    "psycopg2-binary==2.9.10" \
    "apache-airflow-providers-postgres==5.14.0"
