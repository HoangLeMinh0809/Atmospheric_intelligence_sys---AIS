FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gfortran \
    gdal-bin \
    libgdal-dev \
    libeccodes-data \
    libhdf4-alt-dev \
    libhdf5-dev \
    libnetcdff-dev \
    python3-dev \
    python3-pip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Optional HYSPLIT install:
# Place the official NOAA Linux HYSPLIT archive under spark/hysplit/ before
# building. The public NOAA download requires accepting their use agreement.
COPY spark/hysplit/ /tmp/hysplit/
RUN set -eux; \
    mkdir -p /opt/hysplit; \
    archive="$(find /tmp/hysplit -maxdepth 1 -type f \( -name '*.tar.gz' -o -name '*.tgz' -o -name '*.tar' -o -name '*.zip' \) | head -n 1)"; \
    if [ -n "$archive" ]; then \
      case "$archive" in \
        *.zip) unzip -q "$archive" -d /opt/hysplit ;; \
        *.tar|*.tar.gz|*.tgz) tar -xf "$archive" -C /opt/hysplit ;; \
      esac; \
      nested="$(find /opt/hysplit -mindepth 1 -maxdepth 2 -type d -iname 'hysplit*' | head -n 1)"; \
      if [ -n "$nested" ] && [ "$nested" != "/opt/hysplit" ]; then \
        cp -a "$nested"/. /opt/hysplit/; \
      fi; \
      chmod -R a+rx /opt/hysplit; \
      if [ ! -x /opt/hysplit/exec/era5_2arl ] && [ -x /opt/hysplit/exec/era52arl ]; then \
        ln -sf /opt/hysplit/exec/era52arl /opt/hysplit/exec/era5_2arl; \
      fi; \
      test -x /opt/hysplit/exec/era5_2arl; \
    else \
      echo "No HYSPLIT archive found in spark/hysplit; skipping HYSPLIT binary install."; \
    fi; \
    rm -rf /tmp/hysplit

ENV ECCODES_DEFINITION_PATH=/usr/share/eccodes/definitions

ARG INSTALL_XGBOOST=false
ARG PIP_DEFAULT_TIMEOUT=300
ARG PIP_RETRIES=10
ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
ARG SPARK_HADOOP_PACKAGE=org.apache.hadoop:hadoop-client:3.3.4
ARG SPARK_ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
ARG SPARK_CASSANDRA_PACKAGE=com.datastax.spark:spark-cassandra-connector_2.12:3.5.1

RUN rm -rf /root/.cache/pip && \
    python3 -m pip install --no-cache-dir --default-timeout="${PIP_DEFAULT_TIMEOUT}" --retries="${PIP_RETRIES}" --only-binary=:all: \
    pyyaml==6.0.1 \
    netCDF4==1.6.5 \
    pyhdf==0.11.4 \
    numpy==1.24.4 \
    pandas==1.5.3 \
    scipy==1.10.1 \
    scikit-learn==1.3.2 \
    lightgbm==3.3.5 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    python-dotenv==1.0.1 \
    cdsapi==0.7.6 && \
    if [ "${INSTALL_XGBOOST}" = "true" ]; then \
      python3 -m pip install --no-cache-dir --default-timeout="${PIP_DEFAULT_TIMEOUT}" --retries="${PIP_RETRIES}" --only-binary=:all: \
      xgboost==2.0.3; \
    else \
      echo "Skipping optional xgboost install."; \
    fi

# Pre-resolve Spark package dependencies at build time and bake them into the
# runtime image so spark-submit does not hit Maven at job runtime.
RUN set -eux; \
    SPARK_JARS_IVY=/tmp/.ivy2; \
    printf '%s\n' 'import pyspark  # noqa: F401' 'print("preload spark deps")' > /tmp/preload_deps.py; \
    /opt/spark/bin/spark-submit \
      --master local[1] \
      --packages "${SPARK_KAFKA_PACKAGE},${SPARK_HADOOP_PACKAGE},${SPARK_ICEBERG_PACKAGE},${SPARK_CASSANDRA_PACKAGE}" \
      --conf "spark.jars.ivy=${SPARK_JARS_IVY}" \
      /tmp/preload_deps.py; \
    test -f "${SPARK_JARS_IVY}/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar"; \
    test -f "${SPARK_JARS_IVY}/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar"; \
    cp -n "${SPARK_JARS_IVY}/jars/"*.jar /opt/spark/jars/; \
    rm -f /tmp/preload_deps.py; \
    rm -rf "${SPARK_JARS_IVY}"

COPY spark_jobs/ /opt/spark-jobs/
COPY ml/ /opt/ml/
COPY ingest/ /opt/ais/ingest/
COPY config/ /opt/config/
COPY scripts/ /opt/ais/scripts/

ENV PYTHONPATH=/opt/spark-jobs:/opt/ml:/opt/ais/ingest:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV HANOI_PIPELINE_CONFIG=/opt/config/hanoi_pipeline.yaml

USER root
