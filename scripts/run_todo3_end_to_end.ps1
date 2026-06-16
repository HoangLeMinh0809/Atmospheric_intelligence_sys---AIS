# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
param(
    [int]$LookbackDays = 30,
    [string]$StartDate = "",
    [string]$EndDate = "",
    [switch]$SkipBuildImages,
    [switch]$SkipBackfill
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir

# Khai bao class Step de gom state, cau hinh hoac hanh vi lien quan.
function Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Write-Host ""
    Write-Host "=== $Name ===" -ForegroundColor Cyan
    & $Action
    Write-Host "[OK] $Name" -ForegroundColor Green
}

# Khai bao class Require de gom state, cau hinh hoac hanh vi lien quan.
function Require-Command {
    param([Parameter(Mandatory = $true)][string]$CommandName)
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

# Khai bao class Resolve de gom state, cau hinh hoac hanh vi lien quan.
function Resolve-DateRange {
    if ([string]::IsNullOrWhiteSpace($StartDate) -or [string]::IsNullOrWhiteSpace($EndDate)) {
        $utcNow = (Get-Date).ToUniversalTime()
        $resolvedEnd = $utcNow.Date
        $resolvedStart = $resolvedEnd.AddDays(-$LookbackDays)
        return @{
            Start = $resolvedStart.ToString("yyyy-MM-dd")
            End = $resolvedEnd.ToString("yyyy-MM-dd")
        }
    }

    return @{
        Start = $StartDate
        End = $EndDate
    }
}

Require-Command -CommandName "docker"
Require-Command -CommandName "kubectl"
Require-Command -CommandName "bash"

$range = Resolve-DateRange
$resolvedStartDate = $range.Start
$realtimeSimulationDate = $range.End
$resolvedEndDate = ([datetime]::ParseExact($realtimeSimulationDate, "yyyy-MM-dd", $null)).Date.AddDays(-1).ToString("yyyy-MM-dd")
if ([datetime]::ParseExact($resolvedStartDate, "yyyy-MM-dd", $null) -gt [datetime]::ParseExact($resolvedEndDate, "yyyy-MM-dd", $null)) {
    throw "Invalid window: StartDate=$resolvedStartDate must be <= historical EndDate=$resolvedEndDate (requested EndDate - 1 day)."
}

Write-Host "TODO-3 historical window: $resolvedStartDate -> $resolvedEndDate (requested current day: $realtimeSimulationDate)"

Step "1) Start core infra (Docker Compose)" {
    docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra | Out-Host
}

Step "2) Ensure K8s runtime config + bridge services" {
    kubectl apply -f deploy/k8s/configmap.yaml | Out-Host
    kubectl apply -f deploy/k8s/compose-bridge-services.yaml | Out-Host
}

if (-not $SkipBuildImages) {
    Step "3) Build runtime images for TODO-3" {
        docker build -t ais-spark-runtime:local -f spark\Dockerfile . | Out-Host
        docker build -t ais-ml-runtime:local -f ml\Dockerfile . | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip image build due to -SkipBuildImages"
}

if (-not $SkipBackfill) {
    Step "4) Backfill OpenAQ -> Kafka" {
        $env:LOOKBACK_DAYS = "$LookbackDays"
        docker compose run --rm `
            -e WINDOW_MODE=batch `
            -e BATCH_LOOKBACK_DAYS="$LookbackDays" `
            -e WINDOW_START_UTC="${resolvedStartDate}T00:00:00Z" `
            -e WINDOW_END_UTC="${resolvedEndDate}T23:59:59Z" `
            openaq-ingest | Out-Host
    }

    Step "5) Catch Kafka OpenAQ -> Iceberg bronze (earliest, batch stop)" {
        bash -lc "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh openaq" | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip backfill due to -SkipBackfill"
}

Step "6) Build Hanoi feature layers (silver/gold)" {
    bash -lc "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' FULL_REFRESH=1 bash scripts/submit_spark.sh hanoi-openaq-silver" | Out-Host
    bash -lc "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' FULL_REFRESH=1 bash scripts/submit_spark.sh openaq-gradient" | Out-Host
    bash -lc "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' FULL_REFRESH=1 bash scripts/submit_spark.sh hanoi-master-features-gold" | Out-Host
    bash -lc "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' FULL_REFRESH=1 bash scripts/submit_spark.sh hanoi-training-dataset-gold" | Out-Host
}

Step "7) Train model (K8s Job)" {
    kubectl -n ais delete job pm25-train --ignore-not-found | Out-Host
    kubectl apply -f deploy\k8s\ml\pm25-train-job.yaml | Out-Host
    kubectl -n ais wait --for=condition=complete --timeout=900s job/pm25-train | Out-Host
}

Step "8) Promote latest 6h/12h/24h model versions to production" {
    $promoteScript = @'
set -euo pipefail
for horizon in 6 12 24; do
  run_id="$(docker exec spark-master /opt/spark/bin/spark-sql -S \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ais.type=hadoop \
    --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg \
    -e "SELECT run_id FROM ais.models.hanoi_pm25_model_runs_gold WHERE horizon=${horizon} ORDER BY trained_at DESC LIMIT 1;" \
    | tr -d '\r' | awk 'NF{print $1; exit}')"
  if [ -z "${run_id}" ]; then
    echo "No run_id found for horizon=${horizon}" >&2
    exit 1
  fi
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ais.type=hadoop \
    --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg \
    /opt/ml/promote_hanoi_pm25_model.py --run-id "${run_id}"
done
'@
    bash -lc $promoteScript | Out-Host
}

Step "9) Build serving features (Spark-on-K8s)" {
    bash -lc "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' FULL_REFRESH=1 SPARK_SUBMIT_IMAGE_PULL_POLICY=IfNotPresent bash scripts/submit_spark_k8s.sh hanoi-serving-features-gold" | Out-Host
}

Step "10) Run predict job (K8s)" {
    kubectl -n ais delete job pm25-predict --ignore-not-found | Out-Host
    kubectl apply -f deploy\k8s\ml\pm25-predict-job.yaml | Out-Host
    kubectl -n ais wait --for=condition=complete --timeout=600s job/pm25-predict | Out-Host
}

Step "11) Final checks" {
    kubectl -n ais logs job/pm25-train --tail=80 | Out-Host
    kubectl -n ais logs job/pm25-predict --tail=80 | Out-Host

    $verifySql = @'
SELECT location_id, base_hour, created_at, pm25_6h, pm25_12h, pm25_24h, model_version_6h, model_version_12h, model_version_24h
FROM ais.predictions.hanoi_pm25_forecast_gold
ORDER BY created_at DESC
LIMIT 5;
'@
    docker exec spark-master /opt/spark/bin/spark-sql -S `
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
        --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.ais.type=hadoop `
        --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg `
        -e $verifySql | Out-Host
}

Write-Host ""
Write-Host "TODO-3 E2E test completed successfully." -ForegroundColor Green
Write-Host "Window: $resolvedStartDate -> $resolvedEndDate"
