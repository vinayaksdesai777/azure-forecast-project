<#
.SYNOPSIS
    Deploy every ADF artifact under adf/ to a Data Factory.

.DESCRIPTION
    Deploys in dependency order — integration runtimes, then linked services,
    datasets, pipelines and finally triggers — because a pipeline cannot be
    created before the datasets it references.

    Uses the management REST API through `az rest` rather than the
    `az datafactory` extension. Two reasons, both encountered on this project:

      * The extension is a separate install that can break independently of the
        CLI. On the machine this was written, every `az datafactory` command
        failed with "Permission denied: azext_metadata.json" from a corrupted
        extension directory, while `az rest` kept working.
      * ADF Studio's own Publish is not a reliable fallback either: it failed
        with "Cannot read properties of undefined
        (reading '__LAST_PUBLISHED_COMMIT_ID__')" and Debug submitted nothing at
        all, which is why deployment moved to REST in the first place.

    Idempotent: PUT creates or replaces, so re-running is safe.

    Triggers are deployed but NOT started. Starting them is a deliberate act —
    see -StartTriggers.

.EXAMPLE
    ./scripts/deploy_adf.ps1 -ResourceGroup rg-hpe-forecast-dev -FactoryName adf-hpe-forecast

.EXAMPLE
    ./scripts/deploy_adf.ps1 -ResourceGroup rg-hpe-forecast-dev -FactoryName adf-hpe-forecast -WhatIf
#>
param(
    [Parameter(Mandatory = $true)][string] $ResourceGroup,
    [Parameter(Mandatory = $true)][string] $FactoryName,
    [string] $AdfRoot = (Join-Path (Split-Path $PSScriptRoot -Parent) "adf"),
    [switch] $StartTriggers,
    [switch] $WhatIf
)

$ErrorActionPreference = "Stop"
$ApiVersion = "2018-06-01"

$sub = az account show --query id -o tsv
if (-not $sub) { throw "Not logged in. Run 'az login' first." }

$base = "https://management.azure.com/subscriptions/$sub/resourceGroups/$ResourceGroup" +
        "/providers/Microsoft.DataFactory/factories/$FactoryName"

# Folder under adf/ -> REST collection name. Order matters: each kind may
# reference the ones above it.
$kinds = [ordered]@{
    "integrationRuntime" = "integrationruntimes"
    "linkedService"      = "linkedservices"
    "dataset"            = "datasets"
    "pipeline"           = "pipelines"
    "trigger"            = "triggers"
}

# Several artifacts under adf/ are retained for history and are referenced by
# nothing — ls_keyvault and ds_azure_sql_metadata from the revision that used
# Azure SQL, ds_landing_csv from before landing became Parquet, and
# ls_azure_databricks, an earlier duplicate of ls_databricks. Deploying them
# creates dead objects in the factory and, worse, invites someone to wire them
# up later. Work out what the pipelines, triggers and datasets actually
# reference and deploy only that, plus the pipelines and triggers themselves.
$referenced = [System.Collections.Generic.HashSet[string]]::new()

function Add-References {
    param($Node)
    if ($null -eq $Node) { return }
    if ($Node -is [System.Collections.IEnumerable] -and $Node -isnot [string]) {
        foreach ($item in $Node) { Add-References $item }
        return
    }
    if ($Node -is [PSCustomObject]) {
        foreach ($prop in $Node.PSObject.Properties) {
            if ($prop.Name -eq "referenceName" -and $prop.Value -is [string]) {
                [void]$referenced.Add($prop.Value)
            }
            Add-References $prop.Value
        }
    }
}

foreach ($folder in @("pipeline", "trigger", "dataset")) {
    $dir = Join-Path $AdfRoot $folder
    if (-not (Test-Path $dir)) { continue }
    Get-ChildItem -Path $dir -Filter *.json | ForEach-Object {
        Add-References (Get-Content $_.FullName -Raw | ConvertFrom-Json)
    }
}

Write-Output "Factory : $FactoryName ($ResourceGroup)"
Write-Output "Source  : $AdfRoot"
if ($WhatIf) { Write-Output "Mode    : WhatIf - nothing will be written" }
Write-Output ""

$deployed = 0
$failed = @()

foreach ($folder in $kinds.Keys) {
    $dir = Join-Path $AdfRoot $folder
    if (-not (Test-Path $dir)) { continue }

    $collection = $kinds[$folder]

    Get-ChildItem -Path $dir -Filter *.json | ForEach-Object {
        $doc = Get-Content $_.FullName -Raw | ConvertFrom-Json
        $name = $doc.name
        if (-not $name) { $name = $_.BaseName }

        # Pipelines and triggers are always deployed; the supporting kinds only
        # if something references them.
        if ($folder -notin @("pipeline", "trigger") -and -not $referenced.Contains($name)) {
            Write-Output "  skipped       $folder/$name (referenced by nothing)"
            return
        }

        if ($WhatIf) {
            Write-Output "  would deploy  $folder/$name"
            return
        }

        # The REST body carries only the properties block; the name lives in
        # the URL. Writing it through a temp file avoids every shell quoting
        # problem with embedded JSON.
        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) "adf_$name.json"
        @{ properties = $doc.properties } | ConvertTo-Json -Depth 100 | Out-File $tmp -Encoding utf8

        $url = "$base/$collection/$($name)?api-version=$ApiVersion"
        $result = az rest --method put --url $url --body "@$tmp" --query "name" -o tsv 2>&1
        Remove-Item $tmp -ErrorAction SilentlyContinue

        if ($LASTEXITCODE -eq 0 -and $result -eq $name) {
            Write-Output "  deployed      $folder/$name"
            $script:deployed++
        }
        else {
            Write-Output "  FAILED        $folder/$name"
            Write-Output "                $result"
            $script:failed += "$folder/$name"
        }
    }
}

Write-Output ""
if ($failed.Count) {
    Write-Output "$deployed deployed, $($failed.Count) failed:"
    $failed | ForEach-Object { Write-Output "  $_" }
    exit 1
}
Write-Output "$deployed artifact(s) deployed."

# ── Triggers ──────────────────────────────────────────────────
# Deploying a trigger does not start it, and that is deliberate: a deployment
# should never silently begin running pipelines on a schedule.
if ($StartTriggers -and -not $WhatIf) {
    Write-Output ""
    Get-ChildItem -Path (Join-Path $AdfRoot "trigger") -Filter *.json | ForEach-Object {
        $name = (Get-Content $_.FullName -Raw | ConvertFrom-Json).name
        az rest --method post --url "$base/triggers/$($name)/start?api-version=$ApiVersion" | Out-Null
        Write-Output "  started       $name"
    }
}
elseif (-not $WhatIf) {
    Write-Output ""
    Write-Output "Triggers deployed but not started. Re-run with -StartTriggers, or:"
    Write-Output "  az rest --method post --url `"$base/triggers/<name>/start?api-version=$ApiVersion`""
}
