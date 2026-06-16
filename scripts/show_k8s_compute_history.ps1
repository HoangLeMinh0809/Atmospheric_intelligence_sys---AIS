# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
param(
    [string]$Namespace = "ais",
    [string]$HistoryPath = "logs/k8s_compute_history.jsonl",
    [int]$Last = 40,
    [switch]$IncludeCurrent
)

$ErrorActionPreference = "Stop"

$rootDir = Resolve-Path (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")
Set-Location $rootDir

if ($IncludeCurrent) {
    Write-Host ""
    Write-Host "=== Current Kubernetes compute ===" -ForegroundColor Cyan
    kubectl -n $Namespace get pods,jobs -o wide | Out-Host
}

$historyFullPath = Join-Path $rootDir $HistoryPath
if (-not (Test-Path $historyFullPath)) {
    Write-Host "[INFO] No released compute history yet: $HistoryPath" -ForegroundColor Yellow
    exit 0
}

$records = @(
    Get-Content -Path $historyFullPath |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        ForEach-Object {
            try { $_ | ConvertFrom-Json } catch { $null }
        } |
        Where-Object { $null -ne $_ } |
        Sort-Object archived_at -Descending |
        Select-Object -First $Last
)

Write-Host ""
Write-Host "=== Released Kubernetes compute history ===" -ForegroundColor Cyan
$records |
    Select-Object archived_at, kind, name, phase, succeeded, failed, job_type, spark_role, spark_app_name, owner |
    Format-Table -AutoSize
