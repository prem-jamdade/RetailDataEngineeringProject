# Register a daily Windows Task Scheduler job for the Retail Sales ETL Pipeline
param(
    [string]$TaskName = "RetailSalesDailyETL",
    [string]$RunAt = "02:00AM"
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BatPath = Join-Path $ScriptDir "run_daily_pipeline.bat"

$Action = New-ScheduledTaskAction -Execute $BatPath
$Trigger = New-ScheduledTaskTrigger -Daily -At $RunAt
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Force
Write-Host "Registered scheduled task '$TaskName' to run daily at $RunAt ($BatPath)"
