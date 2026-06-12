#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
HDFS_WEBHDFS_BASE="${HDFS_WEBHDFS_BASE:-http://localhost:9870/webhdfs/v1}"

run_hdfs() {
  docker exec "$NAMENODE_CONTAINER" hdfs dfs "$@"
}

run_hdfs_admin() {
  docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin "$@"
}

echo "=== HDFS container state ==="
docker inspect --format 'namenode status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' namenode
docker inspect --format 'datanode  status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' datanode

echo
echo "=== HDFS safemode ==="
run_hdfs_admin -fs hdfs://namenode:9000 -safemode get

echo
echo "=== HDFS core layout ==="
for path in /warehouse/iceberg /checkpoints /raw /models /visualization_cache /tmp/spark /logs; do
  if run_hdfs -test -d "$path"; then
    echo "ok      $path"
  else
    echo "missing $path"
    exit 1
  fi
done

echo
echo "=== HDFS write smoke ==="
smoke_path="/tmp/spark/ais_hdfs_smoke_$(date +%s).txt"
printf 'ais-hdfs-smoke\n' | docker exec -i "$NAMENODE_CONTAINER" hdfs dfs -put -f - "$smoke_path"
run_hdfs -test -s "$smoke_path"
run_hdfs -rm -f "$smoke_path" >/dev/null
echo "ok      write/read/delete via hdfs://namenode:9000"

echo
echo "=== WebHDFS host endpoint ==="
curl -fsS "${HDFS_WEBHDFS_BASE}/?op=LISTSTATUS" >/dev/null
echo "ok      ${HDFS_WEBHDFS_BASE}"

echo
echo "HDFS external layer OK"
