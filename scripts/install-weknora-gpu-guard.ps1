[CmdletBinding()]
param([switch]$Uninstall)

$ErrorActionPreference = 'Stop'
$taskName = 'WeKnora-GPU-Guard'

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }
    Write-Output "$taskName uninstalled"
    exit 0
}

$guardPath = Join-Path $PSScriptRoot 'weknora-gpu-guard.ps1'
if (-not (Test-Path -LiteralPath $guardPath -PathType Leaf)) {
    throw "GPU guard not found: $guardPath"
}

$launcherPath = Join-Path $PSScriptRoot 'launch-weknora-gpu-guard-hidden.vbs'
if (-not (Test-Path -LiteralPath $launcherPath -PathType Leaf)) {
    throw "GPU guard launcher not found: $launcherPath"
}

# pwsh allocates a terminal before it can process -WindowStyle Hidden, which
# leaves a Windows Terminal taskbar entry on systems where Terminal is the
# default console host. wscript is a GUI-subsystem host; the launcher starts
# pwsh with SW_HIDE and waits so Task Scheduler still monitors the real guard.
$wscript = Join-Path $env:WINDIR 'System32\wscript.exe'
$quotedLauncherPath = '"' + $launcherPath + '"'
$action = New-ScheduledTaskAction -Execute $wscript -Argument "//B //NoLogo $quotedLauncherPath"
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
# The long-running guard can receive 0xC000013A when an interactive PowerShell
# host/session is torn down. Re-arm it every five minutes; IgnoreNew keeps this
# watchdog trigger a no-op while the normal guard process is still running.
$watchdogTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At ((Get-Date).AddMinutes(5)) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal `
    -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger @($logonTrigger, $watchdogTrigger) `
    -Settings $settings `
    -Principal $principal `
    -Description 'WeKnora runtime guard: Ollama embedding watchdog, PaddleOCR-VL vLLM 20%, OCR critical pause, and GPU/CPU rerank hysteresis.' `
    -Force | Out-Null

Start-ScheduledTask -TaskName $taskName
Write-Output "$taskName installed and started"
