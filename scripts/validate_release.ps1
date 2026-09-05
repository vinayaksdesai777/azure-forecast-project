param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\")).Path
)

$ErrorActionPreference = "Stop"

Write-Host "Running static validation..."
python (Join-Path $RepoRoot "tests\validate_adf_artifacts.py")
if ($LASTEXITCODE -ne 0) {
    throw "ADF artifact validation failed."
}

Write-Host "Running seed key check..."
python (Join-Path $RepoRoot "tests\check_seed_keys.py")
if ($LASTEXITCODE -ne 0) {
    throw "Seed business-key derivation is broken."
}

Write-Host "Running test suite..."
$testOutput = python -m pytest (Join-Path $RepoRoot "tests") -q -rs 2>&1
$testOutput | Write-Host
if ($LASTEXITCODE -ne 0) {
    throw "Repository tests failed."
}

# A suite that skipped everything is not a passing suite. On Windows the Spark
# launcher scripts under SPARK_HOME/bin do not quote paths, so an install path
# containing a space skips every Spark-backed test — which pytest reports as
# success. Say so rather than printing "passed" over a run that exercised
# nothing.
if ($testOutput -match "(\d+) skipped" -and $testOutput -notmatch "\d+ passed") {
    Write-Host ""
    Write-Host "WARNING: every test skipped - no Spark session was available here."
    Write-Host "         Static validation passed, but nothing was exercised."
    Write-Host "         Run the suite on a Databricks cluster; see tests/README.md."
    exit 2
}

Write-Host "Release validation passed."
