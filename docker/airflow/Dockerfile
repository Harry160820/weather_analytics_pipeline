FROM apache/airflow:2.8.0-python3.11

USER root

# Install wget and Java (OpenJDK 17) – required for beeline
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        openjdk-17-jre-headless \
        && rm -rf /var/lib/apt/lists/*

# Download and extract Apache Hive standalone binaries (includes beeline)
ENV HIVE_VERSION=4.0.0
RUN wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    -O /tmp/hive.tar.gz && \
    tar -xzf /tmp/hive.tar.gz -C /opt/ && \
    ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive && \
    rm /tmp/hive.tar.gz

# Add Hive's bin to PATH
ENV PATH="/opt/hive/bin:${PATH}"

# Switch to airflow user for pip installs
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir \
    pyhive \
    thrift \
    azure-eventhub \
    psycopg2-binary