FROM apache/airflow:2.8.0-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev \
        libsasl2-modules \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-hive==7.0.0 \
    pyhive \
    thrift \
    kafka-python \
    requests