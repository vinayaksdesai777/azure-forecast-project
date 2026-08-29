<#
.SYNOPSIS
    Deploy the ADF failure alerts: one action group and three metric rules.

.DESCRIPTION
    The pipeline runs unattended on four schedule triggers. Without alerting a
    06:00 failure is invisible until somebody thinks to look at the run history,
    and the medallion silently serves stale data for that subject in the
    meantime.

    Three rules, because they fail differently:

      PipelineFailedRuns  Sev 1  a run failed outright
      ActivityFailedRuns  Sev 2  an activity failed even if a retry saved the
                                 run — catches a stopped HANA instance, an
                                 offline SHIR, a terminated cluster
      TriggerFailedRuns   Sev 1  the trigger never fired the pipeline at all.
                                 The worst case: no run appears in the history,
                                 so silence is indistinguishable from success.

    Re-runnable. Every PUT is idempotent, so this can be run after any change.

.EXAMPLE
    ./monitoring/deploy_alerts.ps1 -Email you@example.com
#>
param(
    [Parameter(Mandatory = $true)][string] $Email,
    [string] $ResourceGroup = "rg-hpe-forecast-dev",
    [string] $FactoryName   = "adf-hpe-forecast"
)

$ErrorActionPreference = "Stop"

$sub = az account show --query id -o tsv
if (-not $sub) { throw "Not logged in. Run 'az login' first." }

$adfId = "/subscriptions/$sub/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$FactoryName"
$agId  = "/subscriptions/$sub/resourceGroups/$ResourceGroup/providers/Microsoft.Insights/actionGroups/ag-hpe-forecast"
$root  = Split-Path $PSScriptRoot -Parent
$tmp   = [System.IO.Path]::GetTempPath()

Write-Output "Factory : $FactoryName"
Write-Output "Notify  : $Email"
Write-Output ""

# ── Action group ──────────────────────────────────────────────
$agBody = Get-Content "$root/monitoring/action_group.json" -Raw
$agBody = $agBody.Replace("REPLACE_WITH_EMAIL", $Email)
$agFile = Join-Path $tmp "ag.json"
$agBody | Out-File $agFile -Encoding utf8

az rest --method put `
    --url "https://management.azure.com$($agId)?api-version=2023-01-01" `
    --body "@$agFile" --query "name" -o tsv | Out-Null
Write-Output "  action group  ag-hpe-forecast"

# Azure emails the recipient a confirmation on first creation; the address is
# not armed until they accept it.
Write-Output "  -> check $Email for the Azure confirmation mail"
Write-Output ""

# ── Alert rules ───────────────────────────────────────────────
$rules = @{
    "ar-adf-pipeline-failed" = "alert_pipeline_failed.json"
    "ar-adf-activity-failed" = "alert_activity_failed.json"
    "ar-adf-trigger-failed"  = "alert_trigger_failed.json"
}

foreach ($name in $rules.Keys) {
    $body = Get-Content "$root/monitoring/$($rules[$name])" -Raw
    $body = $body.Replace("__ADF_RESOURCE_ID__", $adfId).Replace("__ACTION_GROUP_ID__", $agId)
    $file = Join-Path $tmp "$name.json"
    $body | Out-File $file -Encoding utf8

    az rest --method put `
        --url "https://management.azure.com/subscriptions/$sub/resourceGroups/$ResourceGroup/providers/Microsoft.Insights/metricAlerts/$($name)?api-version=2018-03-01" `
        --body "@$file" --query "name" -o tsv | Out-Null
    Write-Output "  alert rule    $name"
}

Write-Output ""
Write-Output "Verify:"
Write-Output "  az rest --method get --url `"https://management.azure.com/subscriptions/$sub/resourceGroups/$ResourceGroup/providers/Microsoft.Insights/metricAlerts?api-version=2018-03-01`" --query `"value[].{name:name,enabled:properties.enabled}`" -o table"
