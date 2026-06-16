# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
param(
    [string]$Namespace = "ais",
    [int]$KeepNewestJobs = 20,
    [switch]$DeleteAllFinished,
    [string]$HistoryPath = "logs/k8s_compute_history.jsonl"
)

$ErrorActionPreference = "Stop"

# Khai bao class Remove de gom state, cau hinh hoac hanh vi lien quan.
function Remove-Names {
    param(
        [Parameter(Mandatory = $true)][string]$Kind,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Names
    )

    if ($Names.Count -eq 0) {
        return
    }
    kubectl -n $Namespace delete $Kind @Names --ignore-not-found --wait=false | Out-Host
}

$historyFullPath = Join-Path (Get-Location) $HistoryPath

# Khai bao class Add de gom state, cau hinh hoac hanh vi lien quan.
function Add-HistoryRecord {
    param(
        [Parameter(Mandatory = $true)][string]$Kind,
        [Parameter(Mandatory = $true)]$Item,
        [Parameter(Mandatory = $true)][string]$Action
    )

    $labels = @{}
    if ($Item.metadata.labels) {
        foreach ($property in $Item.metadata.labels.PSObject.Properties) {
            $labels[$property.Name] = [string]$property.Value
        }
    }

    $owner = ""
    if ($Item.metadata.ownerReferences) {
        $owner = (($Item.metadata.ownerReferences | ForEach-Object { "$($_.kind)/$($_.name)" }) -join ",")
    }

    $record = [ordered]@{
        archived_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        action = $Action
        namespace = $Namespace
        kind = $Kind
        name = $Item.metadata.name
        phase = $Item.status.phase
        succeeded = $Item.status.succeeded
        failed = $Item.status.failed
        start_time = $Item.status.startTime
        completion_time = $Item.status.completionTime
        owner = $owner
        job_type = $labels["ais/job-type"]
        spark_role = $labels["spark-role"]
        spark_app_name = $labels["spark-app-name"]
        labels = $labels
    }

    $dir = Split-Path -Parent $historyFullPath
    if (-not [string]::IsNullOrWhiteSpace($dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    Add-Content -Path $historyFullPath -Value ($record | ConvertTo-Json -Compress -Depth 8) -Encoding UTF8
}

$jobsJson = kubectl -n $Namespace get jobs -o json | ConvertFrom-Json
$podsJson = kubectl -n $Namespace get pods -o json | ConvertFrom-Json

$finishedJobs = @(
    $jobsJson.items |
        Where-Object { $_.status.completionTime -or $_.status.failed -gt 0 } |
        Sort-Object {
            if ($_.status.completionTime) { $_.status.completionTime } else { $_.status.startTime }
        } -Descending |
        Select-Object -Skip $KeepNewestJobs |
        ForEach-Object { $_.metadata.name }
)

if ($DeleteAllFinished) {
    $finishedJobs = @(
        $jobsJson.items |
            Where-Object { $_.status.completionTime -or $_.status.failed -gt 0 } |
            ForEach-Object { $_.metadata.name }
    )
}

$finishedSparkPods = @(
    $podsJson.items |
        Where-Object { $_.metadata.labels."spark-role" } |
        Where-Object { $_.status.phase -in @("Succeeded", "Failed") } |
        ForEach-Object { $_.metadata.name }
)

$finishedJobPods = @(
    $podsJson.items |
        Where-Object {
            $_.status.phase -in @("Succeeded", "Failed") -and
            ($_.metadata.ownerReferences | Where-Object { $_.kind -eq "Job" })
        } |
        ForEach-Object { $_.metadata.name }
)

$finishedPods = @(@($finishedSparkPods) + @($finishedJobPods) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique)

foreach ($item in @($jobsJson.items | Where-Object { $finishedJobs -contains $_.metadata.name })) {
    Add-HistoryRecord -Kind "job" -Item $item -Action "delete_finished"
}

foreach ($item in @($podsJson.items | Where-Object { $finishedPods -contains $_.metadata.name })) {
    Add-HistoryRecord -Kind "pod" -Item $item -Action "delete_finished"
}

Remove-Names -Kind "job" -Names @($finishedJobs)
Remove-Names -Kind "pod" -Names $finishedPods

Write-Host "[OK] Finished compute cleanup complete: jobs=$($finishedJobs.Count), spark_pods=$($finishedSparkPods.Count), job_pods=$($finishedJobPods.Count), history=$HistoryPath"
