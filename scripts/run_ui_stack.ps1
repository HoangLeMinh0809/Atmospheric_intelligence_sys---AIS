# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
param(
    [switch]$SkipBuildImages,
    [switch]$SkipVisualizationApi,
    [switch]$SkipPortForward,
    [switch]$KeepExistingPortForward,
    [switch]$UseK8sCassandra,
    [int]$UiLocalPort = 3000,
    [int]$WaitTimeoutSeconds = 180
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

# Khai bao class Stop de gom state, cau hinh hoac hanh vi lien quan.
function Stop-UiPortForward {
    param([Parameter(Mandatory = $true)][int]$Port)

    $pattern = "port-forward.*svc/ais-ui.*${Port}:80"
    $processes = Get-CimInstance Win32_Process -Filter "name = 'kubectl.exe'" |
        Where-Object { $_.CommandLine -match $pattern }

    foreach ($process in $processes) {
        Write-Host ("[INFO] Stop old UI port-forward pid={0}" -f $process.ProcessId) -ForegroundColor Yellow
        Stop-Process -Id $process.ProcessId -Force
    }
}

# Khai bao class Start de gom state, cau hinh hoac hanh vi lien quan.
function Start-UiPortForward {
    param([Parameter(Mandatory = $true)][int]$Port)

    $outLog = Join-Path $rootDir ("port-forward-ais-ui-{0}.log" -f $Port)
    $errLog = Join-Path $rootDir ("port-forward-ais-ui-{0}.err.log" -f $Port)

    $process = Start-Process -FilePath kubectl `
        -ArgumentList @("-n", "ais", "port-forward", "svc/ais-ui", "${Port}:80") `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -WindowStyle Hidden `
        -PassThru

    Start-Sleep -Seconds 3

    $listener = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq "Listen" -and $_.OwningProcess -eq $process.Id } |
        Select-Object -First 1

    if (-not $listener) {
        $errText = if (Test-Path $errLog) { Get-Content -Path $errLog -Tail 40 | Out-String } else { "" }
        throw "UI port-forward did not start on localhost:${Port}. stderr:`n$errText"
    }

    Write-Host ("[INFO] UI port-forward pid={0}" -f $process.Id) -ForegroundColor Yellow
    Write-Host ("[INFO] UI URL: http://127.0.0.1:{0}" -f $Port) -ForegroundColor Yellow
}

# Khai bao class Get de gom state, cau hinh hoac hanh vi lien quan.
function Get-RuntimeConfigValues {
    $json = kubectl -n ais get configmap ais-runtime-config -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($json)) {
        return @{}
    }
    $data = ($json | ConvertFrom-Json).data
    $result = @{}
    foreach ($key in @("FEATURE_SOURCE", "WRITE_CASSANDRA_FORECAST", "BASE_HOUR", "BASE_TIME")) {
        if ($null -ne $data.$key -and -not [string]::IsNullOrWhiteSpace([string]$data.$key)) {
            $result[$key] = [string]$data.$key
        }
    }
    return $result
}

# Khai bao class Patch de gom state, cau hinh hoac hanh vi lien quan.
function Patch-RuntimeConfig {
    param([Parameter(Mandatory = $true)][hashtable]$Data)
    if ($Data.Count -eq 0) {
        return
    }

    $patch = @{ data = $Data } | ConvertTo-Json -Compress -Depth 4
    $patchFile = Join-Path ([System.IO.Path]::GetTempPath()) ("ais-runtime-config-{0}.json" -f ([guid]::NewGuid().ToString("N")))
    try {
        Set-Content -Path $patchFile -Value $patch -Encoding UTF8
        kubectl patch configmap ais-runtime-config -n ais --type merge --patch-file $patchFile | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to patch ais-runtime-config"
        }
    }
    finally {
        Remove-Item -LiteralPath $patchFile -ErrorAction SilentlyContinue
    }
}

Require-Command -CommandName "docker"
Require-Command -CommandName "kubectl"

if (-not $SkipBuildImages) {
    Step "1) Build visualization API and UI images" {
        if (-not $SkipVisualizationApi) {
            docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile . | Out-Host
        }
        docker build -t ais-ui:local -f ui/Dockerfile . | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip image build due to -SkipBuildImages" -ForegroundColor Yellow
}

Step "2) Ensure namespace and shared runtime config" {
    $runtimeOverrides = Get-RuntimeConfigValues
    kubectl create namespace ais --dry-run=client -o yaml | kubectl apply -f - | Out-Host
    kubectl apply -f deploy/k8s/serviceaccount.yaml | Out-Host
    kubectl apply -f deploy/k8s/rbac.yaml | Out-Host
    kubectl apply -f deploy/k8s/configmap.yaml | Out-Host
    if ($UseK8sCassandra) {
        kubectl apply -f deploy/k8s/cassandra/cassandra-statefulset.yaml | Out-Host
        kubectl -n ais rollout status statefulset/cassandra --timeout="${WaitTimeoutSeconds}s" | Out-Host
    }
    else {
        docker compose up -d cassandra | Out-Host
        $runtimeOverrides["CASSANDRA_HOST"] = "192.168.65.254"
        $runtimeOverrides["CASSANDRA_PORT"] = "9042"
    }
    kubectl apply -f deploy/k8s/compose-bridge-services.yaml | Out-Host
    Patch-RuntimeConfig $runtimeOverrides
}

if (-not $SkipVisualizationApi) {
    Step "3) Deploy visualization API first" {
        kubectl -n ais apply -f deploy/k8s/visualization-api | Out-Host
        kubectl -n ais rollout status deployment/visualization-api --timeout="${WaitTimeoutSeconds}s" | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip visualization API deploy due to -SkipVisualizationApi" -ForegroundColor Yellow
}

Step "4) Deploy and restart UI after API service exists" {
    kubectl -n ais apply -f deploy/k8s/ui | Out-Host
    kubectl -n ais rollout restart deployment/ais-ui | Out-Host
    kubectl -n ais rollout status deployment/ais-ui --timeout="${WaitTimeoutSeconds}s" | Out-Host
}

Step "5) Smoke test UI and visualization API proxy" {
    kubectl -n ais get deploy,pod,svc | Select-String -Pattern "ais-ui|visualization-api" | Out-Host

    $manifestUrl = "http://127.0.0.1:${UiLocalPort}/api/v1/visualization/manifest/latest"
    if (-not $SkipPortForward) {
        if (-not $KeepExistingPortForward) {
            Stop-UiPortForward -Port $UiLocalPort
        }
        Start-UiPortForward -Port $UiLocalPort
    }

    if (-not $SkipPortForward) {
        $uiResponse = Invoke-WebRequest -Uri "http://127.0.0.1:${UiLocalPort}/" -UseBasicParsing -TimeoutSec 10
        Write-Host ("[CHECK] UI / status={0}" -f $uiResponse.StatusCode) -ForegroundColor Yellow

        $manifestResponse = Invoke-WebRequest -Uri $manifestUrl -UseBasicParsing -TimeoutSec 20
        Write-Host ("[CHECK] Visualization manifest status={0} bytes={1}" -f $manifestResponse.StatusCode, $manifestResponse.Content.Length) -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "AIS UI stack is ready." -ForegroundColor Green
if (-not $SkipPortForward) {
    Write-Host ("Open: http://127.0.0.1:{0}" -f $UiLocalPort) -ForegroundColor Green
}
