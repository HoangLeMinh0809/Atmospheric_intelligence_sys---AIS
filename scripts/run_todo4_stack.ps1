param(
    [int]$LookbackDays = 2,
    [string]$StartDate = "",
    [string]$EndDate = "",
    [switch]$SkipBuildImages,
    [switch]$SkipBackfill,
    [switch]$SkipTodo2,
    [switch]$SkipTraining,
    [switch]$SkipVisualization,
    [switch]$UseCombinedPipelines,
    [switch]$UseLegacyPerJobSpark,
    [switch]$UseComposeSparkForTodo1,
    [string]$K8sKafkaBootstrapServers = "host.docker.internal:29092",
    [int]$HealthWaitTimeoutSeconds = 300,
    [int]$HysplitMaxRuns = 50,
    [int]$HysplitParallelism = 2,
    [int]$HysplitTimeoutSec = 300,
    [ValidateRange(1, 64)][int]$HysplitShardCount = 1,
    [int]$TrajectoryMaxPaths = 150,
    [int]$TrajectoryMaxPoints = 100,
    [int]$VisMaxGeoJsonFeatures = 5000,
    [switch]$AllowTrajectoryDegraded = $true,
    [ValidateRange(1, 15)][int]$ResumeFromStep = 1
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir

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

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$CommandName)
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

function Resolve-DateRange {
    if ([string]::IsNullOrWhiteSpace($StartDate) -or [string]::IsNullOrWhiteSpace($EndDate)) {
        # Use yesterday as the default end date to avoid ingesting partially available "today" data.
        $resolvedEnd = (Get-Date).Date.AddDays(-1)
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

function Invoke-Bash {
    param([Parameter(Mandatory = $true)][string]$Command)
    & bash -lc $Command | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw ("Bash command failed with exit code {0}: {1}" -f $LASTEXITCODE, $Command)
    }
}

function Wait-ComposeHealthy {
    param([int]$TimeoutSeconds = 300)

    $services = @("zookeeper", "kafka", "namenode", "datanode", "cassandra", "spark-master", "spark-worker")
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $allReady = $true
        foreach ($svc in $services) {
            $status = docker inspect --format "{{.State.Status}}" $svc 2>$null
            if (-not $status -or $status.Trim() -ne "running") {
                $allReady = $false
                break
            }

            $health = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $svc 2>$null
            if ($health -and $health.Trim() -ne "none" -and $health.Trim() -ne "healthy") {
                $allReady = $false
                break
            }
        }

        if ($allReady) {
            return
        }
        Start-Sleep -Seconds 5
    }

    throw "Timeout waiting for Docker Compose services to become healthy/running within ${TimeoutSeconds}s"
}

function Should-RunStep {
    param([int]$StepNumber)
    return $StepNumber -ge $ResumeFromStep
}
function Assert-KafkaTopicHasMessages {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Topic,

        [string]$Label = "",

        [int64]$MinMessages = 1
    )

    $display = if ($Label) { $Label } else { $Topic }

    Write-Host "=== Check Kafka topic has messages: $Topic ==="

    # Use the Docker Compose internal broker address. This is the same address
    # used by ingest containers, and avoids localhost/advertised-listener mismatch.
    $bootstrap = "kafka:9092"

    $output = @()
    $exitCode = 0

    # Prefer kafka-run-class if available in the Confluent image.
    $output = & docker exec kafka kafka-run-class kafka.tools.GetOffsetShell `
        --broker-list $bootstrap `
        --topic $Topic `
        --time -1 2>&1
    $exitCode = $LASTEXITCODE

    # Some images expose kafka-run-class.sh instead.
    if ($exitCode -ne 0) {
        $output = & docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell `
            --broker-list $bootstrap `
            --topic $Topic `
            --time -1 2>&1
        $exitCode = $LASTEXITCODE
    }

    if ($exitCode -ne 0) {
        $raw = ($output | ForEach-Object { $_.ToString() }) -join "`n"
        throw "Kafka offset query failed for topic '$Topic'. Raw output:`n$raw"
    }

    $count = [int64]0
    $matched = $false

    foreach ($line in $output) {
        $s = $line.ToString().Trim()

        # Expected format:
        # topic-name:partition:latestOffset
        # Example:
        # openaq-hourly:0:3371
        if ($s -match '^[^:]+:\d+:(\d+)$') {
            $count += [int64]$Matches[1]
            $matched = $true
        }
    }

    if (-not $matched) {
        $raw = ($output | ForEach-Object { $_.ToString() }) -join "`n"
        throw "Could not parse Kafka offsets for topic '$Topic'. Raw output:`n$raw"
    }

    Write-Host ("[CHECK] {0}: {1} messages" -f $display, $count) -ForegroundColor Yellow

    if ($count -lt $MinMessages) {
        throw "Kafka topic '$Topic' has only $count messages, expected at least $MinMessages"
    }
}

function Submit-SparkK8s {
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [string]$ExtraEnv = "",
        [switch]$NoDateRange
    )

    $dateEnv = ""
    if (-not $NoDateRange) {
        $dateEnv = "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' "
    }

    # Spark pods run inside K8s network namespace; never pass localhost broker.
    $kafkaEnv = "KAFKA_BOOTSTRAP_SERVERS='$K8sKafkaBootstrapServers' "

    # Large jobs can take time while Spark driver/executors are scheduled.
    Invoke-Bash "${kafkaEnv}${dateEnv}${ExtraEnv}FULL_REFRESH=1 KUBECTL_TIMEOUT=3600s SPARK_SUBMIT_IMAGE_PULL_POLICY=IfNotPresent bash scripts/submit_spark_k8s.sh $Job"
}

function Submit-SparkK8sBestEffort {
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [string]$ExtraEnv = "",
        [switch]$NoDateRange
    )
    try {
        Submit-SparkK8s -Job $Job -ExtraEnv $ExtraEnv -NoDateRange:$NoDateRange
        return $true
    }
    catch {
        Write-Host "[WARN] Best-effort job failed: $Job -- $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
}

function Start-SparkK8sAsync {
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [Parameter(Mandatory = $true)][string]$SubmitJobName,
        [string]$ExtraEnv = "",
        [switch]$NoDateRange
    )

    $dateEnv = ""
    if (-not $NoDateRange) {
        $dateEnv = "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' "
    }

    $kafkaEnv = "KAFKA_BOOTSTRAP_SERVERS='$K8sKafkaBootstrapServers' "
    Invoke-Bash "${kafkaEnv}${dateEnv}${ExtraEnv}FULL_REFRESH=1 WAIT_FOR_COMPLETION=false FOLLOW_LOGS=false SPARK_SUBMIT_JOB_NAME='$SubmitJobName' KUBECTL_TIMEOUT=3600s SPARK_SUBMIT_IMAGE_PULL_POLICY=IfNotPresent bash scripts/submit_spark_k8s.sh $Job"
    return $SubmitJobName
}

function Wait-K8sSubmitJobs {
    param(
        [Parameter(Mandatory = $true)][string[]]$JobNames,
        [int]$TimeoutSeconds = 3600
    )

    foreach ($jobName in $JobNames) {
        Write-Host "[INFO] Waiting for K8s submit job: $jobName" -ForegroundColor Yellow
        kubectl -n ais wait --for=condition=complete --timeout="${TimeoutSeconds}s" "job/$jobName" | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] K8s submit job failed: $jobName" -ForegroundColor Red
            kubectl -n ais describe job $jobName | Out-Host
            kubectl -n ais logs "job/$jobName" --all-containers=true --tail=300 | Out-Host
            throw "K8s submit job failed: $jobName"
        }
        kubectl -n ais logs "job/$jobName" --all-containers=true --tail=300 | Out-Host
    }
}

function Submit-SparkCompose {
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [string]$ExtraEnv = "",
        [switch]$NoDateRange
    )

    $dateEnv = ""
    if (-not $NoDateRange) {
        $dateEnv = "START_DATE='$resolvedStartDate' END_DATE='$resolvedEndDate' "
    }
    Invoke-Bash "${dateEnv}${ExtraEnv}FULL_REFRESH=1 bash scripts/submit_spark.sh $Job"
}

function Submit-Todo1Job {
    param([Parameter(Mandatory = $true)][string]$Job)

    if ($UseComposeSparkForTodo1) {
        Submit-SparkCompose $Job
    }
    else {
        Submit-SparkK8s $Job
    }
}

function Promote-LatestModels {
    $horizons = @(6, 12, 24)
    foreach ($horizon in $horizons) {
        $sql = "SELECT model_run_id FROM ais.models.hanoi_pm25_model_runs_gold WHERE horizon_hour=$horizon ORDER BY created_at DESC LIMIT 1;"
        $result = docker exec spark-master /opt/spark/bin/spark-sql -S `
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
            --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog `
            --conf spark.sql.catalog.ais.type=hadoop `
            --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg `
            -e $sql
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to query latest model_run_id for horizon=$horizon"
        }

        $runId = ($result `
            | ForEach-Object { $_.ToString().Trim() } `
            | Where-Object { $_ -and $_ -ne "model_run_id" } `
            | Select-Object -First 1)

        if ([string]::IsNullOrWhiteSpace($runId)) {
            throw "No run_id found for horizon=$horizon"
        }

        Write-Host "[INFO] Promote run_id=$runId horizon=$horizon" -ForegroundColor Yellow
        docker exec spark-master /opt/spark/bin/spark-submit `
            --master spark://spark-master:7077 `
            --deploy-mode client `
            --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
            --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog `
            --conf spark.sql.catalog.ais.type=hadoop `
            --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg `
            /opt/ml/promote_hanoi_pm25_model.py --model-run-id $runId --horizon-hour $horizon | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to promote model run_id=$runId horizon=$horizon"
        }
    }
}

Require-Command -CommandName "docker"
Require-Command -CommandName "kubectl"
Require-Command -CommandName "bash"

$range = Resolve-DateRange
$resolvedStartDate = $range.Start
$resolvedEndDate = $range.End
$combinedMode = $true
if ($UseLegacyPerJobSpark) {
    $combinedMode = $false
}
elseif ($UseCombinedPipelines) {
    $combinedMode = $true
}

Write-Host "TODO4 stack run window: $resolvedStartDate -> $resolvedEndDate"
Write-Host "LookbackDays default: 2 days (end at yesterday) when StartDate/EndDate are omitted"
Write-Host "Resume from step: $ResumeFromStep"
Write-Host ("Spark execution mode: {0}" -f $(if ($combinedMode) { "combined pipeline mode" } else { "legacy per-job mode" })) -ForegroundColor Yellow

if (Should-RunStep 1) {
    Step "1) Start core infra" {
        docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra | Out-Host
        Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds
    }
}
else { Write-Host "[SKIP] Step 1 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (Should-RunStep 2) {
    Step "2) Ensure namespace and apply K8s runtime config" {
        kubectl create namespace ais --dry-run=client -o yaml | kubectl apply -f - | Out-Host
        kubectl apply -f deploy/k8s/serviceaccount.yaml | Out-Host
        kubectl apply -f deploy/k8s/rbac.yaml | Out-Host
        kubectl apply -f deploy/k8s/configmap.yaml | Out-Host
        kubectl apply -f deploy/k8s/compose-bridge-services.yaml | Out-Host
        kubectl apply -f deploy/k8s/spark/spark-serviceaccount.yaml | Out-Host
        kubectl apply -f deploy/k8s/spark/spark-rbac.yaml | Out-Host

        $configPatch = @{ data = @{ KAFKA_BOOTSTRAP_SERVERS = $K8sKafkaBootstrapServers } } | ConvertTo-Json -Compress
        kubectl patch configmap ais-runtime-config -n ais --type merge -p $configPatch | Out-Host
        Write-Host "[INFO] K8s Spark Kafka bootstrap: $K8sKafkaBootstrapServers" -ForegroundColor Yellow
    }
}
else { Write-Host "[SKIP] Step 2 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipBuildImages) {
    if (Should-RunStep 3) {
        Step "3) Build runtime images" {
            docker build -t ais-spark-runtime:local -f spark/Dockerfile . | Out-Host
            docker build -t ais-ml-runtime:local -f ml/Dockerfile . | Out-Host
        }
    }
    else { Write-Host "[SKIP] Step 3 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip image build due to -SkipBuildImages"
}

if (Should-RunStep 4) {
    Step "4) Ensure Iceberg tables" {
        Submit-SparkK8s "ensure-iceberg" -NoDateRange
    }
}
else { Write-Host "[SKIP] Step 4 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipBackfill) {
    if (Should-RunStep 5) {
        Step "5) Backfill source data to Kafka" {
            Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh openaq-ingest"
            Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh weather-ingest"
            Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' S5P_DOWNLOAD_RAW=true S5P_RAW_HDFS_BASE_PATH='/raw/sentinel5p' bash scripts/submit_spark.sh sentinel5p-ingest"
            Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh maiac-ingest"
            Invoke-Bash "LOOKBACK_DAYS=$LookbackDays ERA5_START_DATE='$resolvedStartDate' ERA5_END_DATE='$resolvedEndDate' bash scripts/submit_spark.sh era5-ingest"
            Assert-KafkaTopicHasMessages -Topic "openaq-hourly"
            Assert-KafkaTopicHasMessages -Topic "weather_history"
            Assert-KafkaTopicHasMessages -Topic "sentinel5p-summary"
            Assert-KafkaTopicHasMessages -Topic "maiac-summary"
            Assert-KafkaTopicHasMessages -Topic "era5-files"
        }
    }
    else { Write-Host "[SKIP] Step 5 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

    if (Should-RunStep 6) {
        Step "6) Catch Kafka bronze topics into Iceberg" {
            if ($combinedMode) {
                Submit-SparkK8s "bronze-pipeline" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true PIPELINE_SOURCES='openaq,weather,sentinel5p,maiac,era5-files' PIPELINE_CONTINUE_ON_ERROR=false " -NoDateRange
            }
            else {
                Submit-SparkK8s "openaq" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true " -NoDateRange
                Submit-SparkK8s "weather" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true " -NoDateRange
                Submit-SparkK8s "sentinel5p" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true " -NoDateRange
                Submit-SparkK8s "maiac" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true " -NoDateRange
                Submit-SparkK8s "era5-files" "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true " -NoDateRange
            }
        }
    }
    else { Write-Host "[SKIP] Step 6 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip backfill due to -SkipBackfill"
}

if (Should-RunStep 7) {
    Step "7) TODO1 silver/gold tables" {
        if ($combinedMode) {
            Submit-SparkK8s "pm25-feature-pipeline" "PIPELINE_STEPS='openaq-station,weather-proxy,era5-surface,sentinel5p-silver,maiac-silver' "
        }
        else {
            Submit-Todo1Job "hanoi-openaq-silver"
            Submit-Todo1Job "hanoi-weather-silver"
            Submit-Todo1Job "era5-surface-hanoi-silver"
            Submit-Todo1Job "sentinel5p-hanoi-silver"
            Submit-Todo1Job "maiac-hanoi-silver"
            Submit-Todo1Job "hanoi-master-features-gold"
            Submit-Todo1Job "hanoi-training-dataset-gold"
        }
    }
}
else { Write-Host "[SKIP] Step 7 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipTodo2) {
    if (Should-RunStep 8) {
        Step "8) TODO2 trajectory and source feature tables" {
            $hysplitEnv = "HYSPLIT_MAX_RUNS=$HysplitMaxRuns HYSPLIT_PARALLELISM=$HysplitParallelism HYSPLIT_TIMEOUT_SEC=$HysplitTimeoutSec "
            Write-Host ("[INFO] HYSPLIT window={0}->{1} max_runs={2} parallelism={3} timeout_sec={4} shard_count={5}" -f $resolvedStartDate, $resolvedEndDate, $HysplitMaxRuns, $HysplitParallelism, $HysplitTimeoutSec, $HysplitShardCount) -ForegroundColor Yellow
            if ($combinedMode) {
                if ($AllowTrajectoryDegraded) {
                    Submit-SparkK8sBestEffort "era5-pressure-arl" | Out-Null
                }
                else {
                    Submit-SparkK8s "era5-pressure-arl"
                }

                $submittedJobs = @()
                $stamp = Get-Date -Format "yyyyMMddHHmmss"
                foreach ($direction in @("backward", "forward")) {
                    foreach ($shardId in 0..($HysplitShardCount - 1)) {
                        $jobName = "todo4-hysplit-$direction-s$shardId-$stamp"
                        $extraEnv = $hysplitEnv + "DIRECTION=$direction HYSPLIT_SHARD_ID=$shardId HYSPLIT_SHARD_COUNT=$HysplitShardCount "
                        $submittedJobs += Start-SparkK8sAsync "hysplit-run" -SubmitJobName $jobName -ExtraEnv $extraEnv
                    }
                }
                Wait-K8sSubmitJobs -JobNames $submittedJobs -TimeoutSeconds 3600
                Submit-SparkK8s "trajectory-post-pipeline" "DIRECTION=both TRAJ_SPATIAL_BUCKET_DEG=0.25 MAX_DISTANCE_DEG=0.25 "
            }
            else {
                if ($AllowTrajectoryDegraded) {
                    Submit-SparkK8sBestEffort "era5-pressure-arl" | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-run" ($hysplitEnv + "DIRECTION=backward ") | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-parse" "DIRECTION=backward " | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-run" ($hysplitEnv + "DIRECTION=forward ") | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-parse" "DIRECTION=forward " | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-cluster" | Out-Null
                }
                else {
                    Submit-SparkK8s "era5-pressure-arl"
                    Submit-SparkK8s "hysplit-run" ($hysplitEnv + "DIRECTION=backward ")
                    Submit-SparkK8s "hysplit-parse" "DIRECTION=backward "
                    Submit-SparkK8s "hysplit-run" ($hysplitEnv + "DIRECTION=forward ")
                    Submit-SparkK8s "hysplit-parse" "DIRECTION=forward "
                    Submit-SparkK8s "hysplit-cluster"
                }
                Submit-SparkK8s "openaq-gradient"
                Submit-SparkK8s "s5p-grid-silver"
                Submit-SparkK8s "traj-path-sampling" "TRAJ_SPATIAL_BUCKET_DEG=0.25 MAX_DISTANCE_DEG=0.25 "
                Submit-SparkK8s "traj-hourly-features"
            }
        }
    }
    else { Write-Host "[SKIP] Step 8 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (Should-RunStep 9) {
    Step "9) Rebuild PM2.5 gold after TODO2 features" {
            if ($combinedMode) {
                Submit-SparkK8s "pm25-feature-pipeline" "PIPELINE_STEPS='openaq-gradient,s5p-grid,master-features,training-dataset' "
            }
            else {
                Submit-SparkK8s "hanoi-master-features-gold"
                Submit-SparkK8s "hanoi-training-dataset-gold"
            }
    }
}
    else { Write-Host "[SKIP] Step 9 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip TODO2 due to -SkipTodo2"
}

if (-not $SkipTraining) {
    if (Should-RunStep 10) {
        Step "10) Train PM2.5 models" {
            kubectl -n ais delete job pm25-train --ignore-not-found | Out-Host
            kubectl apply -f deploy/k8s/ml/pm25-train-job.yaml | Out-Host
            kubectl -n ais wait --for=condition=complete --timeout=900s job/pm25-train | Out-Host
        }
    }
    else { Write-Host "[SKIP] Step 10 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

    if (Should-RunStep 11) {
        Step "11) Promote latest models to production registry" {
            Promote-LatestModels
        }
    }
    else { Write-Host "[SKIP] Step 11 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip training/promotion due to -SkipTraining"
}

if (Should-RunStep 12) {
    Step "12) TODO3 serving features and prediction" {
        Submit-SparkK8s "hanoi-serving-features-gold"
        kubectl -n ais delete job pm25-predict --ignore-not-found | Out-Host
        kubectl apply -f deploy/k8s/ml/pm25-predict-job.yaml | Out-Host
        kubectl -n ais wait --for=condition=complete --timeout=600s job/pm25-predict | Out-Host
    }
}
else { Write-Host "[SKIP] Step 12 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipVisualization) {
    if (Should-RunStep 13) {
        Step "13) TODO4 visualization gold tables" {
            $visEnv = "VIS_MAX_TRAJECTORIES=$TrajectoryMaxPaths VIS_MAX_POINTS_PER_TRAJECTORY=$TrajectoryMaxPoints VIS_MAX_GEOJSON_FEATURES=$VisMaxGeoJsonFeatures "
            Write-Host ("[INFO] Visualization limits max_paths={0} max_points={1} max_geojson_features={2}" -f $TrajectoryMaxPaths, $TrajectoryMaxPoints, $VisMaxGeoJsonFeatures) -ForegroundColor Yellow
            if ($combinedMode) {
                $baseTime = "${resolvedEndDate}T23:00:00Z"
                Submit-SparkK8s "visualization-pipeline" ($visEnv + "BASE_TIME='$baseTime' PIPELINE_LAYERS='heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries' EXPORT_CACHE=true ")
            }
            else {
                Submit-SparkK8s "visualization-forecast-dashboard" $visEnv
                Submit-SparkK8s "visualization-pm25-timeseries" $visEnv
                Submit-SparkK8s "visualization-station-observations" $visEnv
                Submit-SparkK8s "visualization-backward-trajectories" $visEnv
                Submit-SparkK8s "visualization-source-attribution" $visEnv
                Submit-SparkK8s "visualization-forward-plume" $visEnv
                Submit-SparkK8s "visualization-heatmap-grid" $visEnv
            }
        }
    }
    else { Write-Host "[SKIP] Step 13 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

    if (Should-RunStep 14) {
        Step "14) Export visualization cache and run quality checks" {
            $visEnv = "VIS_MAX_TRAJECTORIES=$TrajectoryMaxPaths VIS_MAX_POINTS_PER_TRAJECTORY=$TrajectoryMaxPoints VIS_MAX_GEOJSON_FEATURES=$VisMaxGeoJsonFeatures "
            if (-not $combinedMode) {
                Submit-SparkK8s "visualization-export-cache" $visEnv -NoDateRange
            }
            Submit-SparkK8s "visualization-quality-checks" $visEnv -NoDateRange
        }
    }
    else { Write-Host "[SKIP] Step 14 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip visualization due to -SkipVisualization"
}

if (Should-RunStep 15) {
    Step "15) Show latest forecast rows" {
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
}
else { Write-Host "[SKIP] Step 15 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

Write-Host ""
Write-Host "TODO4 stack run completed." -ForegroundColor Green
Write-Host "Window: $resolvedStartDate -> $resolvedEndDate"
