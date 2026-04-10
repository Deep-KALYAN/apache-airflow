FROM apache/airflow:2.8.0-python3.10

USER root
RUN apt-get update && apt-get install -y gcc

USER airflow

COPY scripts/ /opt/scripts/

RUN pip install --no-cache-dir pandas