param(
    [int]$LookbackDays = 2,
    [string]$StartDate = "2026-05-29",
    [string]$EndDate = "2026-05-31",
    [switch]$SkipBuildImages,
    [switch]$SkipBackfill,
    [switch]$SkipTodo2,
    [switch]$SkipTraining,
    [switch]$SkipVisualization,
    [switch]$IncludeForwardTrajectory,
    [switch]$UseCombinedPipelines,
    [switch]$UseLegacyPerJobSpark,
    [switch]$UseComposeSparkForTodo1,
    [switch]$UseCassandraOnlineServing,
    [switch]$UseIcebergPredictionInput,
    [string]$K8sHostAddress = "",
    [string]$K8sKafkaBootstrapServers = "192.168.65.254:29092",
    [int]$HdfsHostPort = 0,
    [int]$SparkMasterUiHostPort = 0,
    [int]$HealthWaitTimeoutSeconds = 300,
    [int]$HysplitMaxRuns = 0,
    [int]$HysplitParallelism = 2,
    [int]$HysplitTimeoutSec = 300,
    [ValidateRange(1, 64)][int]$HysplitShardCount = 1,
    [int]$TrajectoryMaxPaths = 150,
    [int]$TrajectoryMaxPoints = 100,
    [int]$VisMaxGeoJsonFeatures = 5000,
    [switch]$SkipRealtimeNewData = $true,
    [int]$RealtimeLookbackMinutes = 180,
    [int]$RealtimePollSeconds = 60,
    [string]$RealtimeProcessingTime = "30 seconds",
    [switch]$EnableWeatherRealtimeStream,
    [switch]$UseComposeRealtimeBronze,
    [switch]$SkipDemoRealtimeFeed,
    [int]$DemoFeedStepMinutes = 1,
    [int]$DemoFeedMaxBatches = 60,
    [int]$DemoFeedBatchIntervalSeconds = 15,
    [int]$DemoFeedBatchSize = 24,
    [double]$DemoFeedNoiseRatio = 0.08,
    [string]$DemoFeedSources = "weather,openaq",
    [int]$OnlineFeatureIntervalSeconds = 600,
    [int]$RealtimePredictionIntervalSeconds = 600,
    [int]$OnlineFeatureLookbackHours = 30,
    [int]$RealtimeBronzeWarmupSeconds = 45,
    [switch]$EnableHourlyContextUpdater,
    [ValidateSet("Static", "Refresh")][string]$HourlyContextMode = "Static",
    [switch]$EnableOnlineCron,
    [switch]$RunOnlineCycleNow,
    [switch]$KeepFinishedBatchJobs = $true,
    [switch]$UseComposeCassandra = $true,
    [switch]$AllowTrajectoryDegraded = $true,
    [ValidateSet("Auto", "Require", "Refresh", "SnapshotOnly")][string]$BackfillCacheMode = "Auto",
    [switch]$ResetKafkaBackfillTopics,
    [ValidateRange(1, 20)][int]$ResumeFromStep = 1
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
    try {
        & $Action
        Write-Host "[OK] $Name" -ForegroundColor Green
    }
    finally {
        if (-not $KeepFinishedBatchJobs) {
            Cleanup-FinishedK8sCompute -DeleteAllFinished -BestEffort
        }
    }
}

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$CommandName)
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

function Resolve-DateRange {
    $today = (Get-Date).Date

    if ([string]::IsNullOrWhiteSpace($StartDate) -and [string]::IsNullOrWhiteSpace($EndDate)) {
        $resolvedEnd = $today
        $resolvedStart = $resolvedEnd.AddDays(-$LookbackDays)
        return @{
            Start = $resolvedStart.ToString("yyyy-MM-dd")
            End = $resolvedEnd.ToString("yyyy-MM-dd")
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($StartDate) -and [string]::IsNullOrWhiteSpace($EndDate)) {
        return @{
            Start = $StartDate
            End = $today.ToString("yyyy-MM-dd")
        }
    }

    if ([string]::IsNullOrWhiteSpace($StartDate) -and -not [string]::IsNullOrWhiteSpace($EndDate)) {
        $resolvedEnd = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
        $resolvedStart = $resolvedEnd.Date.AddDays(-$LookbackDays)
        return @{
            Start = $resolvedStart.ToString("yyyy-MM-dd")
            End = $EndDate
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

function Invoke-Kubectl {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & kubectl @Arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw ("kubectl failed with exit code {0}: kubectl {1}" -f $LASTEXITCODE, ($Arguments -join " "))
    }
}

function Set-K8sCronJobSuspended {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][bool]$Suspended
    )

    $patch = @{ spec = @{ suspend = $Suspended } } | ConvertTo-Json -Compress
    $patchFile = New-TemporaryFile
    try {
        [System.IO.File]::WriteAllText($patchFile.FullName, $patch, [System.Text.UTF8Encoding]::new($false))
        Invoke-Kubectl @("patch", "cronjob", $Name, "-n", "ais", "--type", "merge", "--patch-file", $patchFile.FullName)
    }
    finally {
        Remove-Item -LiteralPath $patchFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-DockerCompose {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & docker compose @Arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw ("docker compose failed with exit code {0}: docker compose {1}" -f $LASTEXITCODE, ($Arguments -join " "))
    }
}

function Test-HostPortInUse {
    param([Parameter(Mandatory = $true)][int]$Port)

    $connections = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    return $connections.Count -gt 0
}

function Resolve-HdfsHostPort {
    param([int]$RequestedPort = 0)

    if ($RequestedPort -gt 0) {
        return $RequestedPort
    }

    $publishedPorts = @()
    $portExitCode = 1
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $publishedPorts = @(& docker port namenode 9000/tcp 2>$null)
        $portExitCode = $LASTEXITCODE
    }
    catch {
        $publishedPorts = @()
        $portExitCode = 1
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($portExitCode -eq 0 -and $publishedPorts.Count -gt 0) {
        $publishedPort = ($publishedPorts `
            | ForEach-Object { $_.ToString().Trim() } `
            | Where-Object { $_ -match ':(\d+)$' } `
            | ForEach-Object { [regex]::Match($_, ':(\d+)$').Groups[1].Value } `
            | Select-Object -First 1)
        if (-not [string]::IsNullOrWhiteSpace($publishedPort)) {
            return [int]$publishedPort
        }
    }

    $candidatePorts = @(9000, 19000, 19001, 19002, 19003)
    foreach ($port in $candidatePorts) {
        if (-not (Test-HostPortInUse -Port $port)) {
            return $port
        }
    }

    throw "No free HDFS host port found. Tried: $($candidatePorts -join ', ')"
}

function Resolve-SparkMasterUiHostPort {
    param([int]$RequestedPort = 0)

    if ($RequestedPort -gt 0) {
        return $RequestedPort
    }

    $candidatePorts = @(8080, 18080, 18081, 18082, 18083)
    foreach ($port in $candidatePorts) {
        if (-not (Test-HostPortInUse -Port $port)) {
            return $port
        }
    }

    throw "No free Spark Master UI host port found. Tried: $($candidatePorts -join ', ')"
}

function Patch-RuntimeConfig {
    param([Parameter(Mandatory = $true)][hashtable]$Data)

    $patch = @{ data = $Data } | ConvertTo-Json -Compress -Depth 4
    $patchFile = Join-Path ([System.IO.Path]::GetTempPath()) ("ais-runtime-config-{0}.json" -f ([guid]::NewGuid().ToString("N")))
    try {
        Set-Content -Path $patchFile -Value $patch -Encoding UTF8
        Invoke-Kubectl @("patch", "configmap", "ais-runtime-config", "-n", "ais", "--type", "merge", "--patch-file", $patchFile)
    }
    finally {
        Remove-Item -LiteralPath $patchFile -ErrorAction SilentlyContinue
    }
}

function Patch-ComposeBridgeEndpoints {
    param([Parameter(Mandatory = $true)][string]$HostAddress)

    $bridgeYaml = @"
apiVersion: v1
kind: Endpoints
metadata:
  name: namenode
  namespace: ais
  labels:
    app.kubernetes.io/name: namenode-bridge
    app.kubernetes.io/part-of: atmospheric-intelligence-system
subsets:
  - addresses:
      - ip: $HostAddress
    ports:
      - name: hdfs
        port: 9000
      - name: webhdfs
        port: 9870
---
apiVersion: v1
kind: Endpoints
metadata:
  name: datanode
  namespace: ais
  labels:
    app.kubernetes.io/name: datanode-bridge
    app.kubernetes.io/part-of: atmospheric-intelligence-system
subsets:
  - addresses:
      - ip: $HostAddress
    ports:
      - name: data-transfer
        port: 9866
      - name: http
        port: 9864
"@
    $bridgeFile = Join-Path ([System.IO.Path]::GetTempPath()) ("ais-compose-bridge-endpoints-{0}.yaml" -f ([guid]::NewGuid().ToString("N")))
    try {
        Set-Content -Path $bridgeFile -Value $bridgeYaml -Encoding UTF8
        Invoke-Kubectl @("apply", "-f", $bridgeFile)
    }
    finally {
        Remove-Item -LiteralPath $bridgeFile -ErrorAction SilentlyContinue
    }
}

function Wait-ComposeHealthy {
    param(
        [int]$TimeoutSeconds = 300,
        [string[]]$Services = @("zookeeper", "kafka", "namenode", "datanode", "cassandra", "spark-master", "spark-worker")
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastReportAt = Get-Date

    while ((Get-Date) -lt $deadline) {
        $allReady = $true
        $notReady = @()
        foreach ($svc in $Services) {
            $status = docker inspect --format "{{.State.Status}}" $svc 2>$null
            if (-not $status -or $status.Trim() -ne "running") {
                $allReady = $false
                $notReady += "${svc}:status=$($status.Trim())"
                continue
            }

            $health = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $svc 2>$null
            if ($health -and $health.Trim() -ne "none" -and $health.Trim() -ne "healthy") {
                $allReady = $false
                $notReady += "${svc}:health=$($health.Trim())"
            }
        }

        if ($allReady) {
            return
        }

        if (((Get-Date) - $lastReportAt).TotalSeconds -ge 30 -and $notReady.Count -gt 0) {
            Write-Host ("[WAIT] Compose not ready: {0}" -f ($notReady -join ", ")) -ForegroundColor Yellow
            $lastReportAt = Get-Date
        }
        Start-Sleep -Seconds 5
    }

    Show-ComposeDiagnostics -Services $Services
    throw "Timeout waiting for Docker Compose services to become healthy/running within ${TimeoutSeconds}s"
}

function Show-ComposeDiagnostics {
    param([Parameter(Mandatory = $true)][string[]]$Services)

    Write-Host ""
    Write-Host "=== Docker Compose diagnostics ===" -ForegroundColor Yellow
    & docker compose ps | Out-Host
    foreach ($svc in $Services) {
        Write-Host ""
        Write-Host "--- docker inspect: $svc ---" -ForegroundColor Yellow
        docker inspect --format "status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} exit={{.State.ExitCode}}" $svc 2>$null | Out-Host
        Write-Host "--- docker logs --tail 80 $svc ---" -ForegroundColor Yellow
        docker logs --tail 80 $svc 2>&1 | Out-Host
    }
}

function Resolve-K8sHostAddress {
    param([string]$RequestedHost = "")

    if (-not [string]::IsNullOrWhiteSpace($RequestedHost)) {
        return $RequestedHost.Trim()
    }

    $resolved = ""
    try {
        $pods = @(kubectl -n ais get pods -o jsonpath="{.items[0].metadata.name}" 2>$null)
        $podName = ($pods | Select-Object -First 1)
        if (-not [string]::IsNullOrWhiteSpace($podName)) {
            $lookup = kubectl -n ais exec $podName -- getent hosts host.docker.internal 2>$null
            foreach ($line in @($lookup)) {
                $text = $line.ToString().Trim()
                if ($text -match '^([0-9]+(?:\.[0-9]+){3})\s+') {
                    $resolved = $Matches[1]
                    break
                }
            }
        }
    }
    catch {
        $resolved = ""
    }

    if ([string]::IsNullOrWhiteSpace($resolved)) {
        $candidates = @("192.168.65.254", "172.18.0.1", "192.168.1.63")
        foreach ($candidate in $candidates) {
            try {
                $client = [System.Net.Sockets.TcpClient]::new()
                $async = $client.BeginConnect($candidate, 9000, $null, $null)
                if ($async.AsyncWaitHandle.WaitOne(1000, $false)) {
                    $client.EndConnect($async)
                    $client.Close()
                    $resolved = $candidate
                    break
                }
                $client.Close()
            }
            catch {
                try { $client.Close() } catch {}
            }
        }
    }

    if ([string]::IsNullOrWhiteSpace($resolved)) {
        $resolved = "192.168.65.254"
    }
    return $resolved
}

function Replace-EndpointHost {
    param(
        [Parameter(Mandatory = $true)][string]$Endpoint,
        [Parameter(Mandatory = $true)][string]$TargetHost
    )
    return $Endpoint.Replace("host.docker.internal", $TargetHost)
}

function Remove-HdfsHaLeftovers {
    $haContainers = @(
        "zkfc1",
        "zkfc2",
        "journalnode1",
        "journalnode2",
        "journalnode3",
        "namenode1",
        "namenode2",
        "datanode1",
        "datanode2"
    )

    Write-Host "[INFO] Removing leftover HDFS HA containers before starting single-node HDFS." -ForegroundColor Yellow
    $existingContainers = @(docker ps -a --format "{{.Names}}" 2>$null)
    $containersToRemove = @($haContainers | Where-Object { $existingContainers -contains $_ })
    if ($containersToRemove.Count -gt 0) {
        docker rm -f @containersToRemove | Out-Host
    }
    else {
        Write-Host "[INFO] No leftover HDFS HA containers found." -ForegroundColor DarkGray
    }
}

function Clear-KafkaBrokerRegistration {
    param([int]$BrokerId = 1)

    $path = "/brokers/ids/$BrokerId"
    Write-Host "[INFO] Clearing stale Kafka broker registration in Zookeeper if present: $path" -ForegroundColor Yellow
    $deleteCommand = "printf 'delete $path`n' | zookeeper-shell localhost:2181 >/tmp/zk-delete-broker.log 2>&1 || true; cat /tmp/zk-delete-broker.log"
    $output = docker exec zookeeper sh -lc $deleteCommand 2>&1
    $exitCode = $LASTEXITCODE
    if ($output) {
        $interesting = @($output | Where-Object {
            $_ -match "Node does not exist|Deleted|SyncConnected|KeeperErrorCode|Welcome to ZooKeeper"
        })
        if ($interesting.Count -gt 0) {
            Write-Host ($interesting -join "`n") -ForegroundColor DarkGray
        }
    }
    if ($exitCode -ne 0) {
        Write-Host "[WARN] Could not clear Kafka broker registration; Kafka startup will retry normally." -ForegroundColor Yellow
    }
}

function Ensure-K8sCassandra {
    if ($UseComposeCassandra) {
        Write-Host "[INFO] UseComposeCassandra enabled; keeping Cassandra on Docker Compose." -ForegroundColor Yellow
        Invoke-DockerCompose @("up", "-d", "--build", "cassandra")
        Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds -Services @("cassandra")
        return
    }

    Write-Host "[INFO] Deploying Cassandra serving layer on Kubernetes." -ForegroundColor Yellow
    kubectl create namespace ais --dry-run=client -o yaml | kubectl apply -f - | Out-Host
    Invoke-Kubectl @("apply", "-f", "deploy/k8s/cassandra/cassandra-statefulset.yaml")
    Invoke-Kubectl @("rollout", "status", "statefulset/cassandra", "-n", "ais", "--timeout=${HealthWaitTimeoutSeconds}s")
    Invoke-Kubectl @("wait", "--for=condition=ready", "pod/cassandra-0", "-n", "ais", "--timeout=${HealthWaitTimeoutSeconds}s")
}

function Start-CoreInfra {
    $volatileServices = @("kafka", "zookeeper", "namenode", "datanode", "spark-master", "spark-worker")
    $recreateServices = @("namenode", "datanode", "spark-master", "spark-worker")

    Write-Host "[INFO] Stopping volatile Compose services to clear stale DNS/Zookeeper sessions." -ForegroundColor Yellow
    Invoke-DockerCompose (@("stop") + $volatileServices)
    Remove-HdfsHaLeftovers

    # Recreate HDFS/Spark containers so stale Docker DNS/network attachments do
    # not survive between runs. HDFS data remains in named volumes.
    Invoke-DockerCompose (@("rm", "-f") + $recreateServices)

    $env:HDFS_NAMENODE_HOST_PORT = "$resolvedHdfsHostPort"
    $env:SPARK_MASTER_UI_HOST_PORT = "$resolvedSparkMasterUiHostPort"
    if (-not $UseComposeCassandra) {
        Invoke-DockerCompose @("stop", "cassandra")
    }
    Ensure-K8sCassandra
    Invoke-DockerCompose @("up", "-d", "--build", "zookeeper")
    Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds -Services @("zookeeper")
    Clear-KafkaBrokerRegistration -BrokerId 1
    Invoke-DockerCompose @("up", "-d", "--build", "kafka")
    Invoke-DockerCompose @("up", "-d", "--build", "namenode", "datanode")
    Invoke-DockerCompose @("up", "-d", "--build", "spark-master", "spark-worker")
    $coreServices = @("zookeeper", "kafka", "namenode", "datanode", "spark-master", "spark-worker")
    if ($UseComposeCassandra) {
        $coreServices += "cassandra"
    }
    Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds -Services $coreServices
    Invoke-Bash "bash scripts/init_hdfs_layout.sh"
}

function Start-RealtimeNewData {
    $envNames = @(
        "WEATHER_WINDOW_MODE",
        "WEATHER_REALTIME_CONTINUOUS",
        "WEATHER_REALTIME_LOOKBACK_MINUTES",
        "WEATHER_REALTIME_POLL_SECONDS",
        "WEATHER_WINDOW_START_UTC",
        "WEATHER_WINDOW_END_UTC",
        "OPENAQ_WINDOW_MODE",
        "OPENAQ_REALTIME_CONTINUOUS",
        "OPENAQ_REALTIME_LOOKBACK_MINUTES",
        "OPENAQ_REALTIME_POLL_SECONDS",
        "OPENAQ_WINDOW_START_UTC",
        "OPENAQ_WINDOW_END_UTC"
    )
    $previous = @{}
    foreach ($name in $envNames) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    }

    try {
        $env:WEATHER_WINDOW_MODE = "realtime"
        $env:WEATHER_REALTIME_CONTINUOUS = "true"
        $env:WEATHER_REALTIME_LOOKBACK_MINUTES = "$RealtimeLookbackMinutes"
        $env:WEATHER_REALTIME_POLL_SECONDS = "$RealtimePollSeconds"
        $env:WEATHER_WINDOW_START_UTC = ""
        $env:WEATHER_WINDOW_END_UTC = ""

        $env:OPENAQ_WINDOW_MODE = "realtime"
        $env:OPENAQ_REALTIME_CONTINUOUS = "true"
        $env:OPENAQ_REALTIME_LOOKBACK_MINUTES = "$RealtimeLookbackMinutes"
        $env:OPENAQ_REALTIME_POLL_SECONDS = "$RealtimePollSeconds"
        $env:OPENAQ_WINDOW_START_UTC = ""
        $env:OPENAQ_WINDOW_END_UTC = ""

        Invoke-DockerCompose @("up", "-d", "--build", "ingest", "openaq-ingest")
    }
    finally {
        foreach ($name in $envNames) {
            if ($null -eq $previous[$name]) {
                [Environment]::SetEnvironmentVariable($name, $null, "Process")
            }
            else {
                [Environment]::SetEnvironmentVariable($name, $previous[$name], "Process")
            }
        }
    }

}

function Stop-ComposeKafkaProducers {
    $producerServices = @("ingest", "openaq-ingest", "sentinel5p-ingest", "maiac-ingest", "demo-realtime-feed")
    Write-Host "[INFO] Stopping Compose Kafka producer services before historical catch-up." -ForegroundColor Yellow
    Invoke-DockerCompose (@("stop") + $producerServices)
}

function Ensure-ComposeKafkaProducersDoNotAutoRestart {
    $sourceProducerServices = @("ingest", "openaq-ingest", "sentinel5p-ingest", "maiac-ingest")
    Write-Host "[INFO] Recreating source producer containers with restart=no and leaving them stopped." -ForegroundColor Yellow
    Invoke-DockerCompose (@("up", "--no-start", "--force-recreate") + $sourceProducerServices)
    Invoke-DockerCompose (@("stop") + $sourceProducerServices)
}

function Cleanup-FinishedK8sCompute {
    param(
        [int]$KeepNewestJobs = 8,
        [switch]$DeleteAllFinished,
        [switch]$BestEffort
    )

    $cleanupArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "scripts/cleanup_finished_k8s_compute.ps1",
        "-Namespace", "ais",
        "-KeepNewestJobs", "$KeepNewestJobs"
    )
    if ($DeleteAllFinished) {
        $cleanupArgs += "-DeleteAllFinished"
    }

    & powershell @cleanupArgs
    if ($LASTEXITCODE -ne 0) {
        $message = "Finished Kubernetes compute cleanup failed"
        if ($BestEffort) {
            Write-Host "[WARN] $message" -ForegroundColor Yellow
            return
        }
        throw $message
    }
}

function Reset-BackfillKafkaTopics {
    $topics = Get-BackfillKafkaTopics
    Write-Host ("[INFO] Resetting Kafka backfill topics: {0}" -f ($topics -join ", ")) -ForegroundColor Yellow
    foreach ($topic in $topics) {
        docker exec kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic $topic 2>$null | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[WARN] Topic delete failed or topic was absent: $topic" -ForegroundColor Yellow
        }
    }
    Start-Sleep -Seconds 5
}

function Test-ComposeSparkAppRunning {
    param([Parameter(Mandatory = $true)][string]$AppName)

    try {
        $sparkApps = Invoke-RestMethod -Uri "http://localhost:$resolvedSparkMasterUiHostPort/json" -TimeoutSec 5
        return @($sparkApps.activeapps | Where-Object { $_.name -eq $AppName -and $_.state -eq "RUNNING" }).Count -gt 0
    }
    catch {
        Write-Host "[WARN] Could not query Spark master app list: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
}

function ConvertTo-K8sNameToken {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)
    return ($Value.ToLowerInvariant() -replace '[^a-z0-9]+', '-').Trim('-')
}

function Stop-ComposeSparkAppsByName {
    param([Parameter(Mandatory = $true)][string[]]$AppNames)

    try {
        $sparkApps = Invoke-RestMethod -Uri "http://localhost:$resolvedSparkMasterUiHostPort/json" -TimeoutSec 5
    }
    catch {
        Write-Host "[INFO] Compose Spark master UI not reachable; no Compose realtime app to stop." -ForegroundColor Yellow
        return
    }

    $activeApps = @($sparkApps.activeapps)
    foreach ($appName in $AppNames) {
        $matches = @($activeApps | Where-Object { $_.name -eq $appName -and $_.state -eq "RUNNING" })
        foreach ($app in $matches) {
            Write-Host "[INFO] Stopping old Compose Spark app before K8s realtime: $($app.name) $($app.id)" -ForegroundColor Yellow
            docker exec spark-master /opt/spark/bin/spark-class org.apache.spark.deploy.Client kill spark://spark-master:7077 $app.id | Out-Host
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[WARN] Failed to kill Compose Spark app $($app.id); continuing." -ForegroundColor Yellow
            }
        }
    }
}

function Test-K8sRealtimeStreamRunning {
    param([Parameter(Mandatory = $true)][string]$AppName)

    $token = ConvertTo-K8sNameToken -Value $AppName
    try {
        $pods = kubectl -n ais get pods -l spark-role=driver -o json | ConvertFrom-Json
    }
    catch {
        return $false
    }

    foreach ($pod in @($pods.items)) {
        if ($pod.status.phase -ne "Running") {
            continue
        }
        $labels = $pod.metadata.labels
        $sparkAppName = [string]$labels."spark-app-name"
        $podName = [string]$pod.metadata.name
        $haystack = "$(ConvertTo-K8sNameToken -Value $sparkAppName) $(ConvertTo-K8sNameToken -Value $podName)"
        if ($haystack -like "*$token*") {
            return $true
        }
    }
    return $false
}

function Wait-K8sRealtimeDriverRunning {
    param(
        [Parameter(Mandatory = $true)][string]$AppName,
        [int]$TimeoutSeconds = 180
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-K8sRealtimeStreamRunning -AppName $AppName) {
            Write-Host "[INFO] K8s realtime Spark driver is running: $AppName" -ForegroundColor Yellow
            return
        }
        Start-Sleep -Seconds 5
    }

    kubectl -n ais get pods -l spark-role=driver -o wide | Out-Host
    throw "Timed out waiting for K8s realtime Spark driver: $AppName"
}

function Submit-K8sRealtimeBronzeStream {
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [Parameter(Mandatory = $true)][string]$AppName,
        [Parameter(Mandatory = $true)][string]$CheckpointPath
    )

    if (Test-K8sRealtimeStreamRunning -AppName $AppName) {
        Write-Host "[INFO] K8s realtime Spark stream already running; skip duplicate submit: $AppName" -ForegroundColor Yellow
        return
    }

    $stamp = Get-Date -Format "yyyyMMddHHmmss"
    $submitJobName = "ais-realtime-$Job-$stamp"
    $extraEnv = @(
        "KAFKA_STARTING_OFFSETS=latest",
        "STOP_AFTER_BATCH=false",
        "PROCESSING_TIME='$RealtimeProcessingTime'",
        "CHECKPOINT_PATH='$CheckpointPath'",
        "SPARK_RUNTIME_APP_NAME='$AppName'",
        "SPARK_K8S_WAIT_APP_COMPLETION=false",
        "SPARK_EXECUTOR_INSTANCES=1",
        "SPARK_EXECUTOR_MEMORY=768m",
        "SPARK_DRIVER_MEMORY=768m",
        "SPARK_EXECUTOR_REQUEST_CORES=250m",
        "SPARK_DRIVER_REQUEST_CORES=250m",
        "SPARK_EXECUTOR_LIMIT_CORES=1",
        "SPARK_DRIVER_LIMIT_CORES=1",
        "SPARK_SQL_SHUFFLE_PARTITIONS=4",
        "SPARK_DEFAULT_PARALLELISM=4",
        "SPARK_SUBMIT_REQUEST_CPU=200m",
        "SPARK_SUBMIT_REQUEST_MEMORY=512Mi",
        "SPARK_SUBMIT_LIMIT_CPU=1",
        "SPARK_SUBMIT_LIMIT_MEMORY=1Gi",
        "JOB_ACTIVE_DEADLINE_SECONDS=300",
        "JOB_TTL_SECONDS_AFTER_FINISHED=3600"
    ) -join " "

    Start-SparkK8sAsync -Job $Job -SubmitJobName $submitJobName -ExtraEnv "$extraEnv " -NoDateRange | Out-Null
    Wait-K8sRealtimeDriverRunning -AppName $AppName
}

function Submit-ComposeRealtimeBronzeStream {
    param(
        [Parameter(Mandatory = $true)][string]$AppName,
        [Parameter(Mandatory = $true)][string]$JobFile,
        [Parameter(Mandatory = $true)][string]$KafkaTopic,
        [Parameter(Mandatory = $true)][string]$IcebergTable,
        [Parameter(Mandatory = $true)][string]$CheckpointPath,
        [Parameter(Mandatory = $true)][string]$HdfsDataDir,
        [Parameter(Mandatory = $true)][string]$HdfsCheckpointDir
    )

    $packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    $warehouse = "hdfs://namenode:9000/warehouse/iceberg"
    $hdfs = "hdfs://namenode:9000"

    if (Test-ComposeSparkAppRunning -AppName $AppName) {
        Write-Host "[INFO] Realtime Spark stream already running; skip duplicate submit: $AppName" -ForegroundColor Yellow
        return
    }

    foreach ($dir in @($HdfsDataDir, $HdfsCheckpointDir, "/warehouse/iceberg")) {
        docker exec namenode hdfs dfs -fs $hdfs -mkdir -p $dir | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "Failed to create HDFS path: $dir" }
        docker exec namenode hdfs dfs -fs $hdfs -chmod 777 $dir | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "Failed to chmod HDFS path: $dir" }
    }

    $dockerArgs = @(
        "exec", "-d",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-e", "KAFKA_STARTING_OFFSETS=latest",
        "-e", "KAFKA_TOPIC=$KafkaTopic",
        "-e", "ICEBERG_TABLE=$IcebergTable",
        "-e", "CHECKPOINT_PATH=$CheckpointPath",
        "-e", "HDFS_NAMENODE=$hdfs",
        "-e", "HDFS_DEFAULT_FS=$hdfs",
        "-e", "HADOOP_DEFAULT_FS=$hdfs",
        "-e", "ICEBERG_WAREHOUSE=$warehouse",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--name", $AppName,
        "--conf", "spark.jars.ivy=/root/.ivy2",
        "--repositories", "https://repo.maven.apache.org/maven2,https://repo1.maven.org/maven2,https://repos.spark-packages.org",
        "--packages", $packages,
        "--conf", "spark.sql.streaming.checkpointLocation=$CheckpointPath",
        "--conf", "spark.hadoop.fs.defaultFS=$hdfs",
        "--conf", "spark.cores.max=1",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.sql.shuffle.partitions=4",
        "--conf", "spark.default.parallelism=4",
        "--conf", "spark.sql.session.timeZone=UTC",
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.ais.type=hadoop",
        "--conf", "spark.sql.catalog.ais.warehouse=$warehouse",
        $JobFile,
        "--processing-time", $RealtimeProcessingTime
    )

    & docker @dockerArgs | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to submit realtime Spark stream: $AppName"
    }
    Write-Host "[INFO] Submitted realtime Spark stream without bash: $AppName" -ForegroundColor Yellow
}

function Start-RealtimeBronzeStreaming {
    $runId = Get-Date -Format "yyyyMMddHHmmss"
    if (-not $UseComposeRealtimeBronze) {
        Write-Host "[INFO] Starting realtime Bronze streams on Kubernetes Spark pods." -ForegroundColor Yellow
        Stop-ComposeSparkAppsByName -AppNames @("OpenAQHourly_Streaming", "WeatherHistory_Streaming")
        Submit-K8sRealtimeBronzeStream `
            -Job "openaq" `
            -AppName "OpenAQHourly_Streaming" `
            -CheckpointPath "$k8sHdfsNamenode/checkpoints/realtime/$runId/openaq_hourly/"
        if ($EnableWeatherRealtimeStream) {
            Submit-K8sRealtimeBronzeStream `
                -Job "weather" `
                -AppName "WeatherHistory_Streaming" `
                -CheckpointPath "$k8sHdfsNamenode/checkpoints/realtime/$runId/weather_history/"
        }
        else {
            Write-Host "[INFO] Weather realtime Bronze stream skipped by default for local demo stability. Pass -EnableWeatherRealtimeStream to run it too." -ForegroundColor Yellow
        }
        kubectl -n ais get pods -l spark-role=driver -o wide | Out-Host
        return
    }

    Write-Host "[INFO] Ensuring Compose Spark master/worker are running before realtime Bronze streaming." -ForegroundColor Yellow
    Invoke-DockerCompose @("up", "-d", "spark-master", "spark-worker")
    Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds -Services @("spark-master", "spark-worker")
    Submit-ComposeRealtimeBronzeStream `
        -AppName "OpenAQHourly_Streaming" `
        -JobFile "/opt/spark-jobs/openaq_hourly_streaming.py" `
        -KafkaTopic "openaq-hourly" `
        -IcebergTable "ais.air_quality.openaq_hourly_bronze" `
        -CheckpointPath "hdfs://namenode:9000/checkpoints/realtime/$runId/openaq_hourly/" `
        -HdfsDataDir "/warehouse/iceberg/air_quality/openaq_hourly_bronze" `
        -HdfsCheckpointDir "/checkpoints/realtime/$runId/openaq_hourly"
    if ($EnableWeatherRealtimeStream) {
        Submit-ComposeRealtimeBronzeStream `
            -AppName "WeatherHistory_Streaming" `
            -JobFile "/opt/spark-jobs/weather_streaming.py" `
            -KafkaTopic "weather_history" `
            -IcebergTable "ais.weather.weather_history_bronze" `
            -CheckpointPath "hdfs://namenode:9000/checkpoints/realtime/$runId/weather_history/" `
            -HdfsDataDir "/warehouse/iceberg/weather/weather_history_bronze" `
            -HdfsCheckpointDir "/checkpoints/realtime/$runId/weather_history"
    }
    else {
        Write-Host "[INFO] Weather realtime Bronze stream skipped by default for local demo stability. Pass -EnableWeatherRealtimeStream to run it too." -ForegroundColor Yellow
    }
}

function Ensure-OnlineServingInfra {
    Write-Host "[INFO] Ensuring Cassandra and HDFS are running for online serving." -ForegroundColor Yellow
    Ensure-K8sCassandra
    $env:HDFS_NAMENODE_HOST_PORT = "$resolvedHdfsHostPort"
    Invoke-DockerCompose @("up", "-d", "namenode", "datanode")
    Wait-ComposeHealthy -TimeoutSeconds $HealthWaitTimeoutSeconds -Services @("namenode", "datanode")
    Invoke-Bash "bash scripts/init_hdfs_layout.sh"
}

function Start-DemoRealtimeFeed {
    $envNames = @(
        "DEMO_FEED_MODE",
        "DEMO_FEED_SOURCES",
        "DEMO_FEED_BASE_TIME",
        "DEMO_FEED_STEP_MINUTES",
        "DEMO_FEED_MAX_BATCHES",
        "DEMO_FEED_BATCH_INTERVAL_SECONDS",
        "DEMO_FEED_BATCH_SIZE",
        "DEMO_FEED_NOISE_RATIO",
        "DEMO_FEED_REPLAY_ID"
    )
    $previous = @{}
    foreach ($name in $envNames) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    }

    try {
        $env:DEMO_FEED_MODE = "prepare-and-replay"
        $env:DEMO_FEED_SOURCES = $DemoFeedSources
        $env:DEMO_FEED_BASE_TIME = $simulatedBaseTime
        $env:DEMO_FEED_STEP_MINUTES = "$DemoFeedStepMinutes"
        $env:DEMO_FEED_MAX_BATCHES = "$DemoFeedMaxBatches"
        $env:DEMO_FEED_BATCH_INTERVAL_SECONDS = "$DemoFeedBatchIntervalSeconds"
        $env:DEMO_FEED_BATCH_SIZE = "$DemoFeedBatchSize"
        $env:DEMO_FEED_NOISE_RATIO = $DemoFeedNoiseRatio.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        $env:DEMO_FEED_REPLAY_ID = (Get-Date -Format "yyyyMMddHHmmss")

        Write-Host ("[INFO] Demo near-realtime feed base_time={0} sources={1} step={2}m ticks={3} interval={4}s noise={5}" -f $simulatedBaseTime, $DemoFeedSources, $DemoFeedStepMinutes, $DemoFeedMaxBatches, $DemoFeedBatchIntervalSeconds, $DemoFeedNoiseRatio) -ForegroundColor Yellow
        Invoke-DockerCompose @("rm", "-f", "-s", "demo-realtime-feed")
        Invoke-DockerCompose @("up", "-d", "--build", "demo-realtime-feed")
    }
    finally {
        foreach ($name in $envNames) {
            if ($null -eq $previous[$name]) {
                [Environment]::SetEnvironmentVariable($name, $null, "Process")
            }
            else {
                [Environment]::SetEnvironmentVariable($name, $previous[$name], "Process")
            }
        }
    }
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

function Get-BackfillCacheDir {
    return Join-Path $rootDir ("cache/backfill/{0}_{1}" -f $resolvedStartDate, $resolvedEndDate)
}

function Get-BackfillKafkaTopics {
    return @(
        "openaq-hourly",
        "weather_history",
        "sentinel5p-summary",
        "maiac-summary",
        "era5-files"
    )
}

function Test-BackfillKafkaCache {
    param([Parameter(Mandatory = $true)][string]$CacheDir)

    if (-not (Test-Path -LiteralPath $CacheDir -PathType Container)) {
        return $false
    }

    foreach ($topic in Get-BackfillKafkaTopics) {
        $topicFile = Join-Path $CacheDir "$topic.jsonl"
        if (-not (Test-Path -LiteralPath $topicFile -PathType Leaf)) {
            Write-Host "[WARN] Backfill cache exists but is missing $topicFile; will refresh from source." -ForegroundColor Yellow
            return $false
        }
        if ((Get-Item -LiteralPath $topicFile).Length -le 0) {
            Write-Host "[WARN] Backfill cache file is empty: $topicFile; will refresh from source." -ForegroundColor Yellow
            return $false
        }
    }

    return $true
}

function Restore-BackfillKafkaCache {
    param([Parameter(Mandatory = $true)][string]$CacheDir)

    Write-Host "[INFO] Loading historical backfill from cache: $CacheDir" -ForegroundColor Yellow
    foreach ($topic in Get-BackfillKafkaTopics) {
        $topicFile = Join-Path $CacheDir "$topic.jsonl"
        $lineCount = (Get-Content -LiteralPath $topicFile | Measure-Object -Line).Lines
        Write-Host ("[INFO] Replaying {0} cached message(s) to Kafka topic {1}" -f $lineCount, $topic) -ForegroundColor Yellow
        Get-Content -LiteralPath $topicFile -Raw | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:9092 --topic $topic | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to replay cached backfill topic '$topic' from $topicFile"
        }
    }
}

function Save-BackfillKafkaCache {
    param([Parameter(Mandatory = $true)][string]$CacheDir)

    Write-Host "[INFO] Saving historical backfill Kafka snapshot to cache: $CacheDir" -ForegroundColor Yellow
    New-Item -ItemType Directory -Force -Path $CacheDir | Out-Null
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    $manifestTopics = @{}

    foreach ($topic in Get-BackfillKafkaTopics) {
        $topicFile = Join-Path $CacheDir "$topic.jsonl"
        $previousErrorActionPreference = $ErrorActionPreference
        $rawOutput = @()
        $consumerExitCode = 0
        try {
            $ErrorActionPreference = "Continue"
            $rawOutput = @(& docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic $topic --from-beginning --timeout-ms 10000 2>&1)
            $consumerExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }

        $messages = @(
            $rawOutput `
                | ForEach-Object { $_.ToString() } `
                | Where-Object {
                    $line = $_.Trim()
                    $line.StartsWith("{")
                }
        )
        if ($consumerExitCode -ne 0 -and $messages.Count -eq 0) {
            $raw = ($rawOutput | ForEach-Object { $_.ToString() } | Select-Object -Last 20) -join "`n"
            throw "Failed to export Kafka topic '$topic' to backfill cache. consumer_exit=$consumerExitCode raw_tail:`n$raw"
        }
        if ($messages.Count -eq 0) {
            throw "Kafka topic '$topic' has no messages to cache"
        }

        [System.IO.File]::WriteAllLines($topicFile, [string[]]$messages, $utf8NoBom)
        $manifestTopics[$topic] = @{
            file = "$topic.jsonl"
            messages = $messages.Count
        }
        Write-Host ("[INFO] Cached {0} message(s) from Kafka topic {1}" -f $messages.Count, $topic) -ForegroundColor Yellow
    }

    $manifest = @{
        start_date = $resolvedStartDate
        end_date = $resolvedEndDate
        requested_end_date = $realtimeSimulationDate
        created_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        topics = $manifestTopics
    } | ConvertTo-Json -Depth 5
    [System.IO.File]::WriteAllText((Join-Path $CacheDir "manifest.json"), $manifest, $utf8NoBom)
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
    $hdfsEnv = "HDFS_NAMENODE='$k8sHdfsNamenode' HDFS_DEFAULT_FS='$k8sHdfsNamenode' HADOOP_DEFAULT_FS='$k8sHdfsNamenode' ICEBERG_WAREHOUSE='$k8sIcebergWarehouse' HDFS_WEBHDFS_BASE='$k8sWebHdfsBase' "

    # Large jobs can take time while Spark driver/executors are scheduled.
    Invoke-Bash "${kafkaEnv}${hdfsEnv}${dateEnv}${ExtraEnv}FULL_REFRESH=1 KUBECTL_TIMEOUT=3600s SPARK_SUBMIT_IMAGE_PULL_POLICY=IfNotPresent bash scripts/submit_spark_k8s.sh $Job"
    if (-not $KeepFinishedBatchJobs) {
        Cleanup-FinishedK8sCompute -DeleteAllFinished -BestEffort
    }
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
    $hdfsEnv = "HDFS_NAMENODE='$k8sHdfsNamenode' HDFS_DEFAULT_FS='$k8sHdfsNamenode' HADOOP_DEFAULT_FS='$k8sHdfsNamenode' ICEBERG_WAREHOUSE='$k8sIcebergWarehouse' HDFS_WEBHDFS_BASE='$k8sWebHdfsBase' "
    Invoke-Bash "${kafkaEnv}${hdfsEnv}${dateEnv}${ExtraEnv}FULL_REFRESH=1 WAIT_FOR_COMPLETION=false FOLLOW_LOGS=false SPARK_SUBMIT_JOB_NAME='$SubmitJobName' KUBECTL_TIMEOUT=3600s SPARK_SUBMIT_IMAGE_PULL_POLICY=IfNotPresent bash scripts/submit_spark_k8s.sh $Job"
    return $SubmitJobName
}

function Wait-K8sSubmitJobs {
    param(
        [Parameter(Mandatory = $true)][string[]]$JobNames,
        [int]$TimeoutSeconds = 3600,
        [switch]$AllowFailures
    )

    foreach ($jobName in $JobNames) {
        Write-Host "[INFO] Waiting for K8s submit job: $jobName" -ForegroundColor Yellow
        kubectl -n ais wait --for=condition=complete --timeout="${TimeoutSeconds}s" "job/$jobName" | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] K8s submit job failed: $jobName" -ForegroundColor Red
            kubectl -n ais describe job $jobName | Out-Host
            kubectl -n ais logs "job/$jobName" --all-containers=true --tail=300 | Out-Host
            $driverPods = kubectl -n ais get pods -l spark-role=driver -o jsonpath="{range .items[*]}{.metadata.name}{' '}{.status.phase}{'\n'}{end}" 2>$null
            $matchingDrivers = @($driverPods | Where-Object { $_ -like "*$jobName*" })
            foreach ($line in $matchingDrivers) {
                $podName = ($line -split '\s+')[0]
                if ($podName) {
                    kubectl -n ais logs $podName --tail=300 | Out-Host
                }
            }
            if ($AllowFailures) {
                Write-Host "[WARN] Continuing despite failed K8s submit job: $jobName" -ForegroundColor Yellow
                continue
            }
            throw "K8s submit job failed: $jobName"
        }
        kubectl -n ais logs "job/$jobName" --all-containers=true --tail=300 | Out-Host

        $driverPods = kubectl -n ais get pods -l spark-role=driver -o jsonpath="{range .items[*]}{.metadata.name}{' '}{.status.phase}{'\n'}{end}" 2>$null
        $matchingDrivers = @($driverPods | Where-Object { $_ -like "*$jobName*" })
        if ($matchingDrivers.Count -eq 0) {
            Write-Host "[WARN] No Spark driver pod found for submit job: $jobName" -ForegroundColor Yellow
            continue
        }

        $failedDrivers = @($matchingDrivers | Where-Object { $_ -match '\sFailed$' })
        if ($failedDrivers.Count -gt 0) {
            Write-Host "[ERROR] Spark driver pod failed for submit job: $jobName" -ForegroundColor Red
            $failedDrivers | ForEach-Object { Write-Host $_ -ForegroundColor Red }
            foreach ($line in $failedDrivers) {
                $podName = ($line -split '\s+')[0]
                if ($podName) {
                    kubectl -n ais logs $podName --tail=300 | Out-Host
                }
            }
            if ($AllowFailures) {
                Write-Host "[WARN] Continuing despite failed Spark driver pod: $jobName" -ForegroundColor Yellow
                continue
            }
            throw "Spark driver pod failed for submit job: $jobName"
        }
        if (-not $KeepFinishedBatchJobs) {
            Cleanup-FinishedK8sCompute -DeleteAllFinished -BestEffort
        }
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
    param(
        [Parameter(Mandatory = $true)][string]$Job,
        [string]$ExtraEnv = ""
    )

    if ($UseComposeSparkForTodo1) {
        Submit-SparkCompose $Job $ExtraEnv
    }
    else {
        Submit-SparkK8s $Job $ExtraEnv
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
            --conf "spark.hadoop.fs.defaultFS=$composeHdfsNamenode" `
            --conf "spark.sql.catalog.ais.warehouse=$composeIcebergWarehouse" `
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
        docker exec `
            -e "HDFS_NAMENODE=$composeHdfsNamenode" `
            -e "HDFS_DEFAULT_FS=$composeHdfsNamenode" `
            -e "HADOOP_DEFAULT_FS=$composeHdfsNamenode" `
            -e "ICEBERG_WAREHOUSE=$composeIcebergWarehouse" `
            -e "MODEL_ARTIFACT_BASE_URI=$composeHdfsNamenode/models" `
            spark-master /opt/spark/bin/spark-submit `
            --master spark://spark-master:7077 `
            --deploy-mode client `
            --conf "spark.hadoop.fs.defaultFS=$composeHdfsNamenode" `
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
            --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog `
            --conf spark.sql.catalog.ais.type=hadoop `
            --conf "spark.sql.catalog.ais.warehouse=$composeIcebergWarehouse" `
            /opt/ml/promote_hanoi_pm25_model.py --model-run-id $runId --horizon-hour $horizon | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to promote model run_id=$runId horizon=$horizon"
        }
    }
}

function Resolve-VisualizationBaseTime {
    param(
        [Parameter(Mandatory = $true)][string]$RequestedEndDate,
        [Parameter(Mandatory = $true)][string]$RequestedBaseTime
    )

    $sql = @"
SELECT substr(cast(max(init_time) AS string), 1, 10) AS latest_date
FROM ais.trajectory.hysplit_runs_bronze
WHERE direction = 'backward'
  AND status = 'success'
  AND init_time <= TIMESTAMP '${RequestedEndDate} 23:59:59';
"@

    try {
        $result = docker exec spark-master /opt/spark/bin/spark-sql -S `
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
            --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog `
            --conf spark.sql.catalog.ais.type=hadoop `
            --conf "spark.hadoop.fs.defaultFS=$composeHdfsNamenode" `
            --conf "spark.sql.catalog.ais.warehouse=$composeIcebergWarehouse" `
            -e $sql
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[WARN] Could not query latest real backward trajectory date; using requested end date." -ForegroundColor Yellow
            return $RequestedBaseTime
        }

        $latestDate = ($result `
            | ForEach-Object { $_.ToString().Trim() } `
            | Where-Object { $_ -match '^\d{4}-\d{2}-\d{2}$' } `
            | Select-Object -First 1)

        if (-not [string]::IsNullOrWhiteSpace($latestDate)) {
            if ($latestDate -ne $RequestedEndDate) {
                Write-Host "[WARN] Requested visualization end date $RequestedEndDate has no real backward trajectory; using latest available backward date $latestDate." -ForegroundColor Yellow
            }
            $requestedClock = ([datetime]::Parse($RequestedBaseTime.Replace("Z", "+00:00"))).ToString("HH:mm:ss")
            return "${latestDate}T${requestedClock}Z"
        }
    }
    catch {
        Write-Host "[WARN] Could not resolve visualization base time: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    return $RequestedBaseTime
}

Require-Command -CommandName "docker"
Require-Command -CommandName "kubectl"
Require-Command -CommandName "bash"

$range = Resolve-DateRange
$resolvedStartDate = $range.Start
$realtimeSimulationDate = $range.End
$historicalBackfillEndDate = ([datetime]::ParseExact($realtimeSimulationDate, "yyyy-MM-dd", $null)).Date.AddDays(-1).ToString("yyyy-MM-dd")
$resolvedEndDate = $historicalBackfillEndDate
if ([datetime]::ParseExact($resolvedStartDate, "yyyy-MM-dd", $null) -gt [datetime]::ParseExact($historicalBackfillEndDate, "yyyy-MM-dd", $null)) {
    throw "Invalid window: StartDate=$resolvedStartDate must be <= HistoricalBackfillEndDate=$historicalBackfillEndDate (EndDate - 1 day)."
}
$resolvedHdfsHostPort = Resolve-HdfsHostPort -RequestedPort $HdfsHostPort
$resolvedSparkMasterUiHostPort = Resolve-SparkMasterUiHostPort -RequestedPort $SparkMasterUiHostPort
$k8sHostBridge = Resolve-K8sHostAddress -RequestedHost $K8sHostAddress
$K8sKafkaBootstrapServers = Replace-EndpointHost -Endpoint $K8sKafkaBootstrapServers -TargetHost $k8sHostBridge
$k8sHdfsNamenode = "hdfs://namenode:9000"
$k8sWebHdfsBase = "http://namenode:9870/webhdfs/v1"
$k8sIcebergWarehouse = "$k8sHdfsNamenode/warehouse/iceberg"
$k8sCassandraHost = if ($UseComposeCassandra) { $k8sHostBridge } else { "cassandra.ais.svc.cluster.local" }
$composeHdfsNamenode = "hdfs://namenode:9000"
$composeWebHdfsBase = "http://namenode:9870/webhdfs/v1"
$composeIcebergWarehouse = "$composeHdfsNamenode/warehouse/iceberg"
$composeHdfsEnv = "HDFS_NAMENODE='$composeHdfsNamenode' HDFS_DEFAULT_FS='$composeHdfsNamenode' HADOOP_DEFAULT_FS='$composeHdfsNamenode' HDFS_WEBHDFS_BASE='$composeWebHdfsBase' WEBHDFS_BASE='$composeWebHdfsBase' "
$runnerNow = Get-Date
$simulatedCurrentClock = $runnerNow.ToString("HH:mm:ss")
$simulatedCurrentHour = $runnerNow.ToString("HH")
$historicalBackfillEndTime = "${historicalBackfillEndDate}T23:59:59Z"
$staticHourlyContextHour = "${historicalBackfillEndDate}T23:00:00Z"
$simulatedBaseTime = "${realtimeSimulationDate}T${simulatedCurrentClock}Z"
$simulatedBaseHour = "${realtimeSimulationDate}T${simulatedCurrentHour}:00:00Z"
$asofEnv = "BASE_TIME='$historicalBackfillEndTime' BASE_HOUR='${historicalBackfillEndDate}T23:00:00Z' "
$realtimeEnv = "BASE_TIME='$simulatedBaseTime' BASE_HOUR='$simulatedBaseHour' REALTIME_SIMULATION_DATE='$realtimeSimulationDate' HISTORICAL_BACKFILL_END_DATE='$historicalBackfillEndDate' ONLINE_FEATURE_LOOKBACK_HOURS=$OnlineFeatureLookbackHours "
$onlineServingEnabled = -not $UseIcebergPredictionInput
if ($UseCassandraOnlineServing) {
    $onlineServingEnabled = $true
}
$combinedMode = $true
if ($UseLegacyPerJobSpark) {
    $combinedMode = $false
}
elseif ($UseCombinedPipelines) {
    $combinedMode = $true
}

Write-Host "TODO4 requested window: $resolvedStartDate -> $realtimeSimulationDate"
Write-Host "Historical batch range: $resolvedStartDate -> $historicalBackfillEndDate"
Write-Host "Realtime simulation date: $realtimeSimulationDate"
Write-Host "Simulated current time: $simulatedBaseTime (date=realtime simulation date, clock=current runner clock)"
Write-Host "Simulated feature base hour: $simulatedBaseHour (hourly feature/prediction key)"
Write-Host "Default demo window: StartDate=$StartDate EndDate=$EndDate. LookbackDays is used only when you pass an empty StartDate/EndDate."
Write-Host "Resume from step: $ResumeFromStep"
Write-Host "Backfill cache mode: $BackfillCacheMode path=$(Get-BackfillCacheDir)"
Write-Host ("Finished batch cleanup: {0}" -f $(if ($KeepFinishedBatchJobs) { "disabled by default; keeping completed/failed K8s jobs visible for demo" } else { "enabled; deleting completed/failed K8s jobs after each step" })) -ForegroundColor Yellow
Write-Host "Hourly context mode: $HourlyContextMode static_hour=$staticHourlyContextHour"
Write-Host ("Realtime Bronze streams: mode={0} openaq=enabled weather={1}" -f $(if ($UseComposeRealtimeBronze) { "compose-spark" } else { "k8s-spark-pods" }), $(if ($EnableWeatherRealtimeStream) { "enabled" } else { "disabled by default for local demo stability" })) -ForegroundColor Yellow
Write-Host ("Spark execution mode: {0}" -f $(if ($combinedMode) { "combined pipeline mode" } else { "legacy per-job mode" })) -ForegroundColor Yellow
Write-Host ("Trajectory directions: {0}" -f $(if ($IncludeForwardTrajectory) { "backward + forward" } else { "backward only" })) -ForegroundColor Yellow
Write-Host ("Online inference feature source: {0}" -f $(if ($onlineServingEnabled) { "Cassandra feature state" } else { "Iceberg serving features" })) -ForegroundColor Yellow
Write-Host ("New-data realtime loop: {0}" -f $(if ($SkipRealtimeNewData) { "disabled" } else { "weather/openaq poll=${RealtimePollSeconds}s lookback=${RealtimeLookbackMinutes}m bronze_trigger='$RealtimeProcessingTime'" })) -ForegroundColor Yellow
Write-Host ("Demo near-realtime feed: {0}" -f $(if ($SkipDemoRealtimeFeed) { "disabled" } else { "enabled sources=$DemoFeedSources step=${DemoFeedStepMinutes}m ticks=$DemoFeedMaxBatches interval=${DemoFeedBatchIntervalSeconds}s noise=$DemoFeedNoiseRatio" })) -ForegroundColor Yellow
Write-Host ("Realtime online feature loop: enabled interval=${OnlineFeatureIntervalSeconds}s lookback=${OnlineFeatureLookbackHours}h prediction_interval=${RealtimePredictionIntervalSeconds}s") -ForegroundColor Yellow
Write-Host "K8s host bridge: $k8sHostBridge" -ForegroundColor Yellow
Write-Host "K8s Kafka bootstrap: $K8sKafkaBootstrapServers" -ForegroundColor Yellow
Write-Host "K8s Cassandra endpoint: ${k8sCassandraHost}:9042" -ForegroundColor Yellow
Write-Host "HDFS host endpoint: $k8sHdfsNamenode" -ForegroundColor Yellow
Write-Host "HDFS topology: single Compose NameNode + DataNode" -ForegroundColor Yellow
Write-Host "Spark Master UI host endpoint: http://localhost:$resolvedSparkMasterUiHostPort" -ForegroundColor Yellow

if (Should-RunStep 1) {
    Step "1) Start core infra" {
        Start-CoreInfra
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
        Patch-ComposeBridgeEndpoints -HostAddress $k8sHostBridge
        kubectl apply -f deploy/k8s/spark/spark-serviceaccount.yaml | Out-Host
        kubectl apply -f deploy/k8s/spark/spark-rbac.yaml | Out-Host

        Patch-RuntimeConfig @{
            KAFKA_BOOTSTRAP_SERVERS = $K8sKafkaBootstrapServers
            HDFS_NAMENODE = $k8sHdfsNamenode
            HDFS_DEFAULT_FS = $k8sHdfsNamenode
            HADOOP_DEFAULT_FS = $k8sHdfsNamenode
            HDFS_WEBHDFS_BASE = $k8sWebHdfsBase
            ICEBERG_WAREHOUSE = $k8sIcebergWarehouse
            MODEL_ARTIFACT_BASE_URI = "$k8sHdfsNamenode/models"
            VIS_CACHE_BASE_URI = "$k8sHdfsNamenode/visualization_cache"
            CASSANDRA_HOST = $k8sCassandraHost
            CASSANDRA_PORT = "9042"
            HISTORICAL_BACKFILL_END_DATE = $historicalBackfillEndDate
            REALTIME_SIMULATION_DATE = $realtimeSimulationDate
            ENABLE_REALTIME_ONLINE_FEATURES = "1"
            ONLINE_FEATURE_INTERVAL_SECONDS = "$OnlineFeatureIntervalSeconds"
            REALTIME_PREDICTION_INTERVAL_SECONDS = "$RealtimePredictionIntervalSeconds"
            ONLINE_FEATURE_LOOKBACK_HOURS = "$OnlineFeatureLookbackHours"
            HOURLY_CONTEXT_MODE = $HourlyContextMode.ToLowerInvariant()
            HOURLY_CONTEXT_STATIC_DATE = $historicalBackfillEndDate
            HOURLY_CONTEXT_STATIC_HOUR = $staticHourlyContextHour
            BASE_TIME = $simulatedBaseTime
            BASE_HOUR = $simulatedBaseHour
            FEATURE_SOURCE = $(if ($onlineServingEnabled) { "cassandra" } else { "iceberg" })
            WRITE_CASSANDRA_FORECAST = $(if ($onlineServingEnabled) { "1" } else { "0" })
            VIS_FORECAST_SOURCE = $(if ($onlineServingEnabled) { "cassandra" } else { "cache" })
        }
        Write-Host "[INFO] K8s Spark Kafka bootstrap: $K8sKafkaBootstrapServers" -ForegroundColor Yellow
        Cleanup-FinishedK8sCompute -KeepNewestJobs 8
    }
}
else { Write-Host "[SKIP] Step 2 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipBuildImages) {
    if (Should-RunStep 3) {
        Step "3) Build runtime images" {
            docker build -t ais-spark-runtime:local -f spark/Dockerfile . | Out-Host
            docker build -t ais-ml-runtime:local -f ml/Dockerfile . | Out-Host
            docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile . | Out-Host
            docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile . | Out-Host
            docker build -t ais-ui:local -f ui/Dockerfile . | Out-Host
            docker compose build ingest openaq-ingest sentinel5p-ingest maiac-ingest demo-realtime-feed | Out-Host
            Ensure-ComposeKafkaProducersDoNotAutoRestart
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
            Stop-ComposeKafkaProducers
            if ($ResetKafkaBackfillTopics) {
                Reset-BackfillKafkaTopics
            }
            $backfillCacheDir = Get-BackfillCacheDir
            $hasBackfillCache = Test-BackfillKafkaCache -CacheDir $backfillCacheDir
            if ($BackfillCacheMode -eq "Refresh") {
                Write-Host "[INFO] BackfillCacheMode=Refresh; running source ingest and overwriting cache." -ForegroundColor Yellow
                $hasBackfillCache = $false
            }

            if ($BackfillCacheMode -eq "SnapshotOnly") {
                Write-Host "[INFO] BackfillCacheMode=SnapshotOnly; saving current Kafka topics to cache without source ingest." -ForegroundColor Yellow
                Save-BackfillKafkaCache -CacheDir $backfillCacheDir
            }
            elseif ($hasBackfillCache) {
                Restore-BackfillKafkaCache -CacheDir $backfillCacheDir
            }
            elseif ($BackfillCacheMode -eq "Require") {
                throw "Backfill cache is required but missing/incomplete: $backfillCacheDir. Run once with -BackfillCacheMode Auto or Refresh while network/source data is available to create it, or SnapshotOnly if Kafka already contains the completed backfill."
            }
            else {
                Write-Host "[INFO] No complete backfill cache found; running source ingest normally, then saving cache for future offline runs." -ForegroundColor Yellow
                Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh openaq-ingest"
                Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh weather-ingest"
                Invoke-Bash "${composeHdfsEnv}LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' S5P_DOWNLOAD_RAW=true S5P_RAW_HDFS_BASE_PATH='/raw/sentinel5p' bash scripts/submit_spark.sh sentinel5p-ingest"
                Invoke-Bash "LOOKBACK_DAYS=$LookbackDays WINDOW_START_UTC='${resolvedStartDate}T00:00:00Z' WINDOW_END_UTC='${resolvedEndDate}T23:59:59Z' bash scripts/submit_spark.sh maiac-ingest"
                Invoke-Bash "${composeHdfsEnv}LOOKBACK_DAYS=$LookbackDays ERA5_START_DATE='$resolvedStartDate' ERA5_END_DATE='$resolvedEndDate' ERA5_DATASET_TYPE='surface' bash scripts/submit_spark.sh era5-ingest"
                Invoke-Bash "${composeHdfsEnv}LOOKBACK_DAYS=$LookbackDays ERA5_START_DATE='$resolvedStartDate' ERA5_END_DATE='$resolvedEndDate' ERA5_DATASET_TYPE='pressure_levels' bash scripts/submit_spark.sh era5-ingest"
                Save-BackfillKafkaCache -CacheDir $backfillCacheDir
            }
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
            Cleanup-FinishedK8sCompute -KeepNewestJobs 8
            Stop-ComposeKafkaProducers
            $bronzeCheckpointRunId = "todo4_${resolvedStartDate}_${resolvedEndDate}_$(Get-Date -Format 'yyyyMMddHHmmss')"
            $bronzeCheckpointEnv = "BRONZE_CHECKPOINT_RUN_ID='$bronzeCheckpointRunId' "
            Write-Host "[INFO] Bronze checkpoint run id: $bronzeCheckpointRunId" -ForegroundColor Yellow
            if ($combinedMode) {
                Submit-SparkK8s "bronze-pipeline" ($bronzeCheckpointEnv + "KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true PIPELINE_SOURCES='openaq,weather,sentinel5p,maiac,era5-files' PIPELINE_CONTINUE_ON_ERROR=false ") -NoDateRange
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
            Submit-SparkK8s "pm25-feature-pipeline" ($asofEnv + "PIPELINE_STEPS='openaq-station,weather-proxy,era5-surface,sentinel5p-silver,maiac-silver' ")
        }
        else {
            Submit-Todo1Job "hanoi-openaq-silver" $asofEnv
            Submit-Todo1Job "hanoi-weather-silver" $asofEnv
            Submit-Todo1Job "era5-surface-hanoi-silver" $asofEnv
            Submit-Todo1Job "sentinel5p-hanoi-silver"
            Submit-Todo1Job "maiac-hanoi-silver"
            Submit-Todo1Job "hanoi-master-features-gold" $asofEnv
            Submit-Todo1Job "hanoi-training-dataset-gold"
        }
    }
}
else { Write-Host "[SKIP] Step 7 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipTodo2) {
    if (Should-RunStep 8) {
        Step "8) TODO2 trajectory and source feature tables" {
            Cleanup-FinishedK8sCompute -KeepNewestJobs 8
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
                $directions = @("backward")
                if ($IncludeForwardTrajectory) {
                    $directions += "forward"
                }
                foreach ($direction in $directions) {
                    foreach ($shardId in 0..($HysplitShardCount - 1)) {
                        $jobName = "todo4-hysplit-$direction-s$shardId-$stamp"
                        $extraEnv = $hysplitEnv + "DIRECTION=$direction HYSPLIT_SHARD_ID=$shardId HYSPLIT_SHARD_COUNT=$HysplitShardCount "
                        $submittedJobs += Start-SparkK8sAsync "hysplit-run" -SubmitJobName $jobName -ExtraEnv $extraEnv
                    }
                }
                Wait-K8sSubmitJobs -JobNames $submittedJobs -TimeoutSeconds 3600 -AllowFailures:$AllowTrajectoryDegraded
                Cleanup-FinishedK8sCompute -KeepNewestJobs 8
                $postDirection = if ($IncludeForwardTrajectory) { "both" } else { "backward" }
                if ($AllowTrajectoryDegraded) {
                    Submit-SparkK8sBestEffort "trajectory-post-pipeline" "DIRECTION=$postDirection TRAJ_SPATIAL_BUCKET_DEG=0.25 MAX_DISTANCE_DEG=0.25 " | Out-Null
                }
                else {
                    Submit-SparkK8s "trajectory-post-pipeline" "DIRECTION=$postDirection TRAJ_SPATIAL_BUCKET_DEG=0.25 MAX_DISTANCE_DEG=0.25 "
                }
            }
            else {
                if ($AllowTrajectoryDegraded) {
                    Submit-SparkK8sBestEffort "era5-pressure-arl" | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-run" ($hysplitEnv + "DIRECTION=backward ") | Out-Null
                    Submit-SparkK8sBestEffort "hysplit-parse" "DIRECTION=backward " | Out-Null
                    if ($IncludeForwardTrajectory) {
                        Submit-SparkK8sBestEffort "hysplit-run" ($hysplitEnv + "DIRECTION=forward ") | Out-Null
                        Submit-SparkK8sBestEffort "hysplit-parse" "DIRECTION=forward " | Out-Null
                    }
                    Submit-SparkK8sBestEffort "hysplit-cluster" | Out-Null
                }
                else {
                    Submit-SparkK8s "era5-pressure-arl"
                    Submit-SparkK8s "hysplit-run" ($hysplitEnv + "DIRECTION=backward ")
                    Submit-SparkK8s "hysplit-parse" "DIRECTION=backward "
                    if ($IncludeForwardTrajectory) {
                        Submit-SparkK8s "hysplit-run" ($hysplitEnv + "DIRECTION=forward ")
                        Submit-SparkK8s "hysplit-parse" "DIRECTION=forward "
                    }
                    Submit-SparkK8s "hysplit-cluster"
                }
                Submit-SparkK8s "openaq-gradient" $asofEnv
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
                if ($AllowTrajectoryDegraded) {
                    Submit-SparkK8sBestEffort "openaq-gradient" $asofEnv | Out-Null
                    Submit-SparkK8sBestEffort "s5p-grid-silver" | Out-Null
                    Submit-SparkK8s "hanoi-master-features-gold" $asofEnv
                    Submit-SparkK8s "hanoi-training-dataset-gold"
                }
                else {
                    Submit-SparkK8s "pm25-feature-pipeline" ($asofEnv + "PIPELINE_STEPS='openaq-gradient,s5p-grid,master-features,training-dataset' ")
                }
            }
            else {
                Submit-SparkK8s "hanoi-master-features-gold" $asofEnv
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
    Step "12) TODO3 historical serving features" {
        Submit-SparkK8s "hanoi-serving-features-gold" $asofEnv
        if ($onlineServingEnabled) {
            Write-Host "[INFO] Online serving enabled; realtime feature_state and prediction are deferred until after realtime replay/streaming." -ForegroundColor Yellow
            Invoke-Bash "bash scripts/ensure_cassandra_online_schema.sh"

            Patch-RuntimeConfig @{
                FEATURE_SOURCE = "cassandra"
                WRITE_CASSANDRA_FORECAST = "1"
                VIS_FORECAST_SOURCE = "cassandra"
                CASSANDRA_HOST = $k8sCassandraHost
                CASSANDRA_PORT = "9042"
                BASE_HOUR = $simulatedBaseHour
                BASE_TIME = $simulatedBaseTime
            }
        }
        else {
            Patch-RuntimeConfig @{
                FEATURE_SOURCE = "iceberg"
                WRITE_CASSANDRA_FORECAST = "0"
                VIS_FORECAST_SOURCE = "cache"
                BASE_HOUR = $simulatedBaseHour
                BASE_TIME = $simulatedBaseTime
            }
            kubectl -n ais delete job pm25-predict --ignore-not-found | Out-Host
            kubectl apply -f deploy/k8s/ml/pm25-predict-job.yaml | Out-Host
            kubectl -n ais wait --for=condition=complete --timeout=600s job/pm25-predict | Out-Host
        }
    }
}
else { Write-Host "[SKIP] Step 12 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipVisualization) {
    if (Should-RunStep 13) {
        Step "13) TODO4 visualization gold tables" {
            $visEnv = "VIS_MAX_TRAJECTORIES=$TrajectoryMaxPaths VIS_MAX_POINTS_PER_TRAJECTORY=$TrajectoryMaxPoints VIS_MAX_GEOJSON_FEATURES=$VisMaxGeoJsonFeatures "
            Write-Host ("[INFO] Visualization limits max_paths={0} max_points={1} max_geojson_features={2}" -f $TrajectoryMaxPaths, $TrajectoryMaxPoints, $VisMaxGeoJsonFeatures) -ForegroundColor Yellow
            if ($combinedMode) {
                if ($AllowTrajectoryDegraded) {
                    $coreBaseTime = $simulatedBaseTime
                    $coreBaseDate = $resolvedEndDate
                    Write-Host "[INFO] Visualization core BASE_TIME=$coreBaseTime" -ForegroundColor Yellow
                    Submit-SparkK8s "visualization-pipeline" ($visEnv + "START_DATE='$coreBaseDate' END_DATE='$coreBaseDate' BASE_TIME='$coreBaseTime' PIPELINE_LAYERS='heatmap,source_attribution,stations,forecast,timeseries' EXPORT_CACHE=true ")

                    $trajBaseTime = Resolve-VisualizationBaseTime -RequestedEndDate $resolvedEndDate -RequestedBaseTime $simulatedBaseTime
                    $trajBaseDate = $trajBaseTime.Substring(0, 10)
                    Write-Host "[INFO] Visualization trajectory BASE_TIME=$trajBaseTime" -ForegroundColor Yellow
                    Submit-SparkK8sBestEffort "visualization-pipeline" ($visEnv + "START_DATE='$trajBaseDate' END_DATE='$trajBaseDate' BASE_TIME='$trajBaseTime' PIPELINE_LAYERS='backward_trajectories' EXPORT_CACHE=false ") | Out-Null
                    if ($IncludeForwardTrajectory) {
                        Submit-SparkK8sBestEffort "visualization-pipeline" ($visEnv + "START_DATE='$coreBaseDate' END_DATE='$coreBaseDate' BASE_TIME='$coreBaseTime' PIPELINE_LAYERS='forward_plume' EXPORT_CACHE=false ") | Out-Null
                    }
                }
                else {
                    $baseTime = Resolve-VisualizationBaseTime -RequestedEndDate $resolvedEndDate -RequestedBaseTime $simulatedBaseTime
                    $baseDate = $baseTime.Substring(0, 10)
                    Write-Host "[INFO] Visualization BASE_TIME=$baseTime" -ForegroundColor Yellow
                    Submit-SparkK8s "visualization-pipeline" ($visEnv + "START_DATE='$baseDate' END_DATE='$baseDate' BASE_TIME='$baseTime' PIPELINE_LAYERS='heatmap,backward_trajectories,forward_plume,source_attribution,stations,forecast,timeseries' EXPORT_CACHE=true ")
                }
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
    Step "15) Show latest historical/audit forecast rows" {
        if ($onlineServingEnabled) {
            Write-Host "[INFO] Online serving enabled; latest forecast will be produced after realtime feature_state." -ForegroundColor Yellow
        }
        else {
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
                --conf "spark.hadoop.fs.defaultFS=$composeHdfsNamenode" `
                --conf "spark.sql.catalog.ais.warehouse=$composeIcebergWarehouse" `
                -e $verifySql | Out-Host
        }
    }
}
else { Write-Host "[SKIP] Step 15 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipRealtimeNewData) {
    if (Should-RunStep 16) {
        Step "16) Start near-realtime Weather/OpenAQ new-data loops" {
            Start-RealtimeNewData
        }
    }
    else { Write-Host "[SKIP] Step 16 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip external API new-data loops due to -SkipRealtimeNewData; demo near-realtime feed still runs in step 19 unless -SkipDemoRealtimeFeed is set."
}

if (Should-RunStep 17) {
    Step "17) Start Spark streaming Kafka to Bronze for realtime audit" {
        Start-RealtimeBronzeStreaming
    }
}
else { Write-Host "[SKIP] Step 17 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (Should-RunStep 18) {
    Step "18) Install static hourly context hook" {
        Patch-RuntimeConfig @{
            HOURLY_CONTEXT_MODE = $HourlyContextMode.ToLowerInvariant()
            HOURLY_CONTEXT_STATIC_DATE = $historicalBackfillEndDate
            HOURLY_CONTEXT_STATIC_HOUR = $staticHourlyContextHour
        }
        kubectl apply -f deploy/k8s/hourly/ais-hourly-context-updater-cronjob.yaml | Out-Host
        Set-K8sCronJobSuspended -Name "ais-hourly-context-updater" -Suspended (-not $EnableHourlyContextUpdater)
        if ($EnableHourlyContextUpdater) {
            if ($HourlyContextMode -eq "Refresh") {
                Write-Host "[WARN] Hourly context cron enabled in Refresh mode; this runs the heavy ERA5/HYSPLIT context pipeline." -ForegroundColor Yellow
            }
            else {
                Write-Host "[INFO] Hourly context cron enabled in Static mode; it only writes the cached context marker and exits." -ForegroundColor Yellow
            }
        }
        else {
            Write-Host "[INFO] Hourly context hook installed suspended. Static context is pinned to $staticHourlyContextHour; no hourly ERA5/HYSPLIT refresh will run." -ForegroundColor Yellow
        }
    }
}
else { Write-Host "[SKIP] Step 18 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }

if (-not $SkipDemoRealtimeFeed) {
    if (Should-RunStep 19) {
        Step "19) Prepare and replay EndDate demo near-realtime feed" {
            Start-DemoRealtimeFeed
        }
    }
    else { Write-Host "[SKIP] Step 19 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Skip demo interpolated near-realtime feed due to -SkipDemoRealtimeFeed"
}

if ($onlineServingEnabled) {
    if (Should-RunStep 20) {
        Step "20) Bootstrap asynchronous online feature and prediction cycle" {
            Write-Host ("[INFO] current base_time={0} online_feature_interval={1}s prediction_interval={2}s" -f $simulatedBaseTime, $OnlineFeatureIntervalSeconds, $RealtimePredictionIntervalSeconds) -ForegroundColor Yellow
            Ensure-OnlineServingInfra
            Invoke-Bash "bash scripts/ensure_cassandra_online_schema.sh"
            Patch-RuntimeConfig @{
                FEATURE_SOURCE = "cassandra"
                WRITE_CASSANDRA_FORECAST = "1"
                VIS_FORECAST_SOURCE = "cassandra"
                CASSANDRA_HOST = $k8sCassandraHost
                CASSANDRA_PORT = "9042"
                BASE_HOUR = $simulatedBaseHour
                BASE_TIME = $simulatedBaseTime
                HISTORICAL_BACKFILL_END_DATE = $historicalBackfillEndDate
                REALTIME_SIMULATION_DATE = $realtimeSimulationDate
                ONLINE_FEATURE_INTERVAL_SECONDS = "$OnlineFeatureIntervalSeconds"
                REALTIME_PREDICTION_INTERVAL_SECONDS = "$RealtimePredictionIntervalSeconds"
                ONLINE_FEATURE_LOOKBACK_HOURS = "$OnlineFeatureLookbackHours"
            }
            Patch-ComposeBridgeEndpoints -HostAddress $k8sHostBridge
            Cleanup-FinishedK8sCompute -DeleteAllFinished -BestEffort
            kubectl apply -f deploy/k8s/ml/online-pm25-features-cronjob.yaml | Out-Host
            kubectl apply -f deploy/k8s/ml/pm25-predict-cronjob.yaml | Out-Host
            Set-K8sCronJobSuspended -Name "pm25-predict" -Suspended $true
            Set-K8sCronJobSuspended -Name "online-pm25-features" -Suspended (-not $EnableOnlineCron)
            if ($RunOnlineCycleNow) {
                $manualJobName = "online-pm25-features-manual-$(Get-Date -Format 'yyyyMMddHHmmss')"
                Invoke-Kubectl @("create", "job", "-n", "ais", "--from=cronjob/online-pm25-features", $manualJobName)
                Write-Host "[INFO] Started one manual online cycle job: $manualJobName" -ForegroundColor Yellow
            }
            if ($EnableOnlineCron) {
                Write-Host "[INFO] Online feature+prediction cron enabled. pm25-predict remains suspended because prediction is already run inside online-pm25-features." -ForegroundColor Yellow
            }
            else {
                Write-Host "[INFO] Online cronjobs installed suspended; pass -RunOnlineCycleNow for one update or -EnableOnlineCron for scheduled updates." -ForegroundColor Yellow
            }
        }
    }
    else { Write-Host "[SKIP] Step 20 due to -ResumeFromStep $ResumeFromStep" -ForegroundColor Yellow }
}
else {
    Write-Host "[INFO] Online feature_state/prediction step disabled because -UseIcebergPredictionInput was set."
}

if (-not $KeepFinishedBatchJobs) {
    Cleanup-FinishedK8sCompute -DeleteAllFinished -BestEffort
}

Write-Host ""
Write-Host "TODO4 stack run completed." -ForegroundColor Green
Write-Host "Historical batch range: $resolvedStartDate -> $historicalBackfillEndDate"
Write-Host "Realtime simulation date: $realtimeSimulationDate"
