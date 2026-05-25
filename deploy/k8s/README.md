# AIS Kubernetes Runtime

This directory contains the TODO3 Kubernetes compute layer manifests. Storage remains outside Kubernetes in this phase; pods reach Kafka, HDFS/Iceberg, Cassandra, and model artifact storage through values in `ais-runtime-config`.

Apply the base resources:

```bash
kubectl apply -k deploy/k8s
kubectl get ns ais
kubectl -n ais get configmap,secret,serviceaccount
```

Local Docker Desktop defaults use `host.docker.internal` for Compose-published services. On Linux, minikube, or kind, update these ConfigMap keys before running workloads:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_HOST
KAFKA_PORT
HDFS_NAMENODE
HDFS_WEBHDFS_BASE
HDFS_CLIENT_USE_DATANODE_HOSTNAME
ICEBERG_WAREHOUSE
CASSANDRA_HOST
MODEL_ARTIFACT_BASE_URI
```

Debug connectivity from inside the cluster:

```bash
kubectl -n ais run debug-net --rm -it --image=busybox:1.36 -- sh
nc -vz "$KAFKA_HOST" "$KAFKA_PORT"
wget -qO- "$HDFS_WEBHDFS_BASE/?op=LISTSTATUS"
```

For Docker Desktop with Compose HDFS, the datanode must publish port `9866` and advertise `host.docker.internal`; otherwise K8s Spark pods can reach the Namenode but fail reading blocks from the Datanode.

Use image tags that identify the source revision or release, for example:

```text
ais-spark-runtime:<git-sha|date|semver>
ais-ml-runtime:<git-sha|date|semver>
ais-pm25-api:<git-sha|date|semver>
```

Do not put real credentials in `secret.example.yaml`; create or patch the `ais-runtime-secrets` Secret in the target cluster.
