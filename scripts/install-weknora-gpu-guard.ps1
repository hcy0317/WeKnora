[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA 'WeKnora\host-guards'),
    [string]$OwnerPath = (Join-Path $env:ProgramData 'WeKnora\engine-ownership\owner.txt'),
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$gpuTaskName = 'WeKnora-GPU-Guard'
$ollamaTaskName = 'WeKnora-Ollama-Watchdog'
$modePath = Join-Path $env:LOCALAPPDATA 'WeKnora\gpu-guard-mode.txt'

if ($Uninstall) {
    foreach ($taskName in @($gpuTaskName, $ollamaTaskName)) {
        if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        }
    }
    Write-Output 'WeKnora host guard tasks uninstalled; installed files and state were preserved'
    exit 0
}

$sourceFiles = @(
    'weknora-gpu-guard.ps1',
    'weknora-ollama-watchdog.ps1',
    'weknora-engine-interlock.ps1',
    'weknora-process.ps1',
    'launch-weknora-gpu-guard-hidden.vbs',
    'launch-weknora-ollama-watchdog-hidden.vbs'
)
foreach ($name in $sourceFiles) {
    $source = Join-Path $PSScriptRoot $name
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "host guard asset not found: $source" }
}
if (-not (Test-Path -LiteralPath $OwnerPath -PathType Leaf)) {
    throw "engine owner state must be initialized by install-engine-host-controller.ps1 first: $OwnerPath"
}

$installDirectory = [IO.Path]::GetFullPath($InstallRoot)
$localRoot = [IO.Path]::GetFullPath($env:LOCALAPPDATA)
if (-not $installDirectory.StartsWith($localRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "InstallRoot must stay under LOCALAPPDATA: $installDirectory"
}
if ($PSCmdlet.ShouldProcess($installDirectory, 'Install stable host guard scripts')) {
    New-Item -ItemType Directory -Path $installDirectory -Force | Out-Null
    foreach ($name in $sourceFiles) {
        Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination (Join-Path $installDirectory $name) -Force
    }
}

. (Join-Path $installDirectory 'weknora-engine-interlock.ps1')
$owner = Get-WeKnoraEngineOwner -Path $OwnerPath
if (-not (Test-Path -LiteralPath $modePath -PathType Leaf)) {
    Set-WeKnoraAtomicText -Path $modePath -Value legacy
}
$mode = (Get-Content -LiteralPath $modePath -Raw).Trim()
if ($mode -notin @('legacy', 'observe', 'disabled')) { throw "invalid GPU Guard mode: $mode" }

$wscript = Join-Path $env:WINDIR 'System32\wscript.exe'
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
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

function Register-HostGuardTask {
    param(
        [Parameter(Mandatory)][string]$TaskName,
        [Parameter(Mandatory)][string]$Launcher,
        [Parameter(Mandatory)][string]$Description
    )
    $quotedLauncher = '"' + (Join-Path $installDirectory $Launcher) + '"'
    $action = New-ScheduledTaskAction -Execute $wscript -Argument "//B //NoLogo $quotedLauncher"
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger @($logonTrigger, $watchdogTrigger) `
        -Settings $settings `
        -Principal $principal `
        -Description $Description `
        -Force | Out-Null
}

# Preserve the Ollama behavior before replacing the legacy combined task.
Register-HostGuardTask `
    -TaskName $ollamaTaskName `
    -Launcher 'launch-weknora-ollama-watchdog-hidden.vbs' `
    -Description 'WeKnora Ollama availability watchdog. Does not access managed engine containers.'
Start-ScheduledTask -TaskName $ollamaTaskName

$existingGpuTask = Get-ScheduledTask -TaskName $gpuTaskName -ErrorAction SilentlyContinue
if ($existingGpuTask -and $existingGpuTask.State -eq 'Running') {
    Stop-ScheduledTask -TaskName $gpuTaskName
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(15)
    do {
        Start-Sleep -Milliseconds 250
        $state = (Get-ScheduledTask -TaskName $gpuTaskName).State
    } while ($state -eq 'Running' -and [DateTimeOffset]::UtcNow -lt $deadline)
    if ($state -eq 'Running') { throw 'legacy GPU Guard task did not stop; new guarded task was not started' }
}
Wait-WeKnoraEngineProcessesExit `
    -CommandLineNeedles @('\weknora-gpu-guard.ps1', '\launch-weknora-gpu-guard-hidden.vbs') `
    -TimeoutSeconds 15

Register-HostGuardTask `
    -TaskName $gpuTaskName `
    -Launcher 'launch-weknora-gpu-guard-hidden.vbs' `
    -Description 'WeKnora legacy GPU observer/actuator with explicit mode, named mutex, owner check, and heartbeat.'
Start-ScheduledTask -TaskName $gpuTaskName

[pscustomobject]@{
    GPUGuardTask = $gpuTaskName
    OllamaWatchdogTask = $ollamaTaskName
    InstallRoot = $installDirectory
    Owner = $owner
    Mode = $mode
}
