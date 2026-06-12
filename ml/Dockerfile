ARG AIS_SPARK_RUNTIME_IMAGE=ais-spark-runtime:local
FROM ${AIS_SPARK_RUNTIME_IMAGE}

USER root
WORKDIR /opt/ais

COPY ml/ ./ml/
COPY spark_jobs/ ./spark_jobs/
COPY config/ ./config/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/opt/ais/spark_jobs:/opt/ais/ml:/opt/spark-jobs:/opt/ml:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV HANOI_PIPELINE_CONFIG=/opt/ais/config/hanoi_pipeline.yaml

CMD ["python", "ml/train_hanoi_pm25.py", "--help"]
