FROM apache/spark:3.5.3

USER root

ARG INSTALL_XGBOOST=false
ARG PIP_DEFAULT_TIMEOUT=300
ARG PIP_RETRIES=10

RUN apt-get update && apt-get install -y --no-install-recommends \
    python-is-python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN rm -rf /root/.cache/pip && \
    python3 -m pip install --no-cache-dir --default-timeout="${PIP_DEFAULT_TIMEOUT}" --retries="${PIP_RETRIES}" --only-binary=:all: \
    pyyaml==6.0.1 \
    numpy==1.24.4 \
    pandas==1.5.3 \
    pyarrow==15.0.2 \
    scikit-learn==1.3.2 \
    lightgbm==3.3.5 \
    requests==2.31.0 && \
    if [ "${INSTALL_XGBOOST}" = "true" ]; then \
      python3 -m pip install --no-cache-dir --default-timeout="${PIP_DEFAULT_TIMEOUT}" --retries="${PIP_RETRIES}" --only-binary=:all: \
      xgboost==2.0.3; \
    else \
      echo "Skipping optional xgboost install."; \
    fi

WORKDIR /opt/ais

COPY ml/ ./ml/
COPY spark_jobs/ ./spark_jobs/
COPY config/ ./config/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/opt/ais/spark_jobs:/opt/ais/ml:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV HANOI_PIPELINE_CONFIG=/opt/ais/config/hanoi_pipeline.yaml

CMD ["python", "ml/train_hanoi_pm25.py", "--help"]
