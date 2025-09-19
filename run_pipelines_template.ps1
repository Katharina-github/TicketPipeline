<#  Template PowerShell script for running ETL pipeline
    Adjust paths before use

    run_both_pipelines.ps1
    Runs ETL then ELT with conda env. Repeats the pair 2 more times every 10 minutes.
    Writes a log file under .\logs\pipeline_runs.log
#>

param(
  [string]$EnvName = "ticketpipeline",
  [string]$ProjectDir = "C:\path\to\project\",
  [string]$ETL = "pipeline_etl.py",
  [string]$ELT = "pipeline_elt.py",
  [int]$Repeats = 2,              # number of extra pairs after the first (so total = Repeats+1)
  [int]$IntervalMinutes = 10
)

# --- setup ---
$ErrorActionPreference = "Stop"
$logDir = Join-Path $ProjectDir "logs"
$newLine = [Environment]::NewLine
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$logFile = Join-Path $logDir "pipeline_runs.log"

function Log {
  param([string]$msg)
  $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "$stamp  $msg"
  $line | Tee-Object -FilePath $logFile -Append
}

# --- locate python in conda env ---
Log "Locating python for env '$EnvName'..."
$pythonPath = conda run -n $EnvName where python | Select-Object -First 1
if (-not $pythonPath) {
  Log "ERROR: Could not find python.exe for env '$EnvName'. Run 'conda env list' to check."
  throw "python.exe not found for env '$EnvName'"
}
Log "Found: $pythonPath"

# --- helper to run a pipeline script ---
function Run-Pipeline {
  param([string]$scriptName)

  $scriptPath = Join-Path $ProjectDir $scriptName
  if (-not (Test-Path $scriptPath)) {
    Log "ERROR: Script not found: $scriptPath"
    throw "Missing $scriptPath"
  }

  Log "START $scriptName"
  Push-Location $ProjectDir
  try {
    & $pythonPath $scriptPath
    $code = $LASTEXITCODE
  } finally {
    Pop-Location
  }

  if ($code -ne 0) {
    Log "FAIL  $scriptName (exit $code)"
    throw "$scriptName failed with exit code $code"
  } else {
    Log "OK    $scriptName"
  }
}

# --- one full cycle: ETL then ELT ---
function Run-Cycle {
  Log "=== CYCLE BEGIN ==="
  Run-Pipeline -scriptName $ETL
  Run-Pipeline -scriptName $ELT
  Log "=== CYCLE END   ==="
}

# --- first run immediately ---
Run-Cycle

# --- repeat N times every IntervalMinutes ---
for ($i = 1; $i -le $Repeats; $i++) {
  Log "Sleeping $IntervalMinutes minute(s) before next cycle ($i of $Repeats)..."
  Start-Sleep -Seconds ($IntervalMinutes * 60)
  Run-Cycle
}

Log "ALL DONE"
Write-Host ""
Write-Host "All cycles finished. Log at: $logFile"
