FROM apache/airflow:2.8.0-python3.10

# Switch to root only for system dependencies
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsasl2-dev \
        libsasl2-modules \
        gcc \
        g++ \
        && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user before any pip install
USER airflow

# Now install Python packages as airflow (required)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-hive \
    pyhive \
    thrift