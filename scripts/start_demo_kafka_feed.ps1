# File nay: script van hanh local/K8s, submit Spark, check hoac cleanup infra.
param(
    [ValidateSet("weather", "openaq", "weather,openaq", "openaq,weather")]
    [string]$Sources = "weather,openaq",
    [ValidateRange(30, 60)]
    [int]$IntervalSeconds = 30,
    [ValidateRange(1, 10000)]
    [int]$BatchSize = 24,
    [switch]$Build
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir

$weatherFeed = Join-Path $rootDir "data/demo_realtime_feed/weather_demo_feed.jsonl"
$openaqFeed = Join-Path $rootDir "data/demo_realtime_feed/openaq_demo_feed.jsonl"

if ($Sources -match "weather" -and -not (Test-Path $weatherFeed)) {
    throw "Missing weather demo feed: $weatherFeed"
}
if ($Sources -match "openaq" -and -not (Test-Path $openaqFeed)) {
    throw "Missing OpenAQ demo feed: $openaqFeed"
}

$env:DEMO_FEED_MODE = "file-replay-loop"
$env:DEMO_FEED_SOURCES = $Sources
$env:DEMO_FEED_BATCH_INTERVAL_SECONDS = "$IntervalSeconds"
$env:DEMO_FEED_MIN_BATCH_INTERVAL_SECONDS = "30"
$env:DEMO_FEED_MAX_BATCH_INTERVAL_SECONDS = "60"
$env:DEMO_FEED_BATCH_SIZE = "$BatchSize"

docker compose rm -f -s demo-realtime-feed | Out-Host

if ($Build) {
    docker compose up -d --build demo-realtime-feed | Out-Host
}
else {
    docker compose up -d demo-realtime-feed | Out-Host
}

Write-Host "[OK] demo-realtime-feed is replaying local JSONL files to Kafka every $IntervalSeconds second(s)." -ForegroundColor Green
Write-Host "     weather -> weather_history, openaq -> openaq-hourly"
Write-Host "     Follow logs: docker compose logs -f demo-realtime-feed"
