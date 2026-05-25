# Spark on Kubernetes

Apply the base namespace, config, and Spark RBAC:

```bash
kubectl apply -k deploy/k8s
kubectl -n ais get sa spark
kubectl -n ais auth can-i create pods --as=system:serviceaccount:ais:spark
```

Build the runtime image from the repository root:

```bash
docker build -t ais-spark-runtime:local -f spark/Dockerfile .
```

Run a minimal Spark submit against Kubernetes:

```bash
bash scripts/submit_spark_k8s.sh spark-smoke
kubectl -n ais get pods
kubectl -n ais logs -l spark-role=driver --tail=200
```

Run the Iceberg table bootstrap on Kubernetes:

```bash
bash scripts/submit_spark_k8s.sh ensure-iceberg
```

`spark-submit-template.yaml` is a reference manifest for one-off manual submit jobs. It is intentionally not included in the base kustomization because applying a Kubernetes `Job` starts it immediately.

For local Docker Desktop, `deploy/k8s/configmap.yaml` uses `host.docker.internal` to reach Compose-published Kafka and HDFS ports. On Linux, minikube, or kind, replace those values with the host IP, NodePort, or a shared-network endpoint before running Spark jobs.

Spark jobs must use env/ConfigMap values for storage endpoints. `scripts/submit_spark_k8s.sh` forwards those values into Spark driver and executor pods with `spark.kubernetes.driverEnv.*` and `spark.executorEnv.*`. It also sets `spark.jars.ivy=/tmp/.ivy2` so non-root driver pods can resolve package dependencies. The Compose Spark master remains a dev fallback only; TODO3 jobs should submit with `--master k8s://...`.

Connectivity debug pod:

```bash
kubectl -n ais run debug-net --rm -it --image=busybox:1.36 -- sh
nc -vz "$KAFKA_HOST" "$KAFKA_PORT"
wget -qO- "$HDFS_WEBHDFS_BASE/?op=LISTSTATUS"
```

If the cluster cannot resolve `host.docker.internal`, set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_HOST`, `HDFS_NAMENODE`, `HDFS_WEBHDFS_BASE`, `ICEBERG_WAREHOUSE`, and `CASSANDRA_HOST` in `deploy/k8s/configmap.yaml` to a host IP, NodePort, or other endpoint reachable from pods. If HDFS is backed by Docker Compose, make sure the datanode data-transfer port `9866` is reachable from pods and keep `HDFS_CLIENT_USE_DATANODE_HOSTNAME=true`.
