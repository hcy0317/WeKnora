[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Parameter(Mandatory)][ValidateSet('legacy', 'controller')][string]$Target,
    [string]$ServiceName = 'WeKnoraEngineHostController',
    [string]$GuardTaskName = 'WeKnora-GPU-Guard',
    [string]$InstallRoot = (Join-Path $env:ProgramData 'WeKnora\engine-controller'),
    [string]$OwnerPath = (Join-Path $env:ProgramData 'WeKnora\engine-ownership\owner.txt'),
    [string]$OwnerMutex = 'Global\WeKnoraEngineDockerOwner',
    [string]$GuardModePath = (Join-Path $env:LOCALAPPDATA 'WeKnora\gpu-guard-mode.txt'),
    [string]$GuardHeartbeatPath = (Join-Path $env:LOCALAPPDATA 'WeKnora\gpu-guard-heartbeat.json'),
    [ValidateRange(10, 180)][int]$HeartbeatTimeoutSeconds = 60
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'weknora-engine-interlock.ps1')

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'weknora-engine-handoff.ps1 must run from an elevated PowerShell session'
}

$binaryPath = Join-Path $InstallRoot 'engine-host-controller.exe'
$configPath = Join-Path $InstallRoot 'config.yaml'
$auditPath = Join-Path $InstallRoot 'ownership-handoff.log'
$service = Get-Service -Name $ServiceName -ErrorAction Stop
$guardTask = Get-ScheduledTask -TaskName $GuardTaskName -ErrorAction Stop
foreach ($requiredPath in @($binaryPath, $configPath, $OwnerPath, $GuardModePath)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "required handoff state is missing: $requiredPath"
    }
}

function Write-HandoffAudit {
    param([Parameter(Mandatory)][string]$Message)
    Add-Content -LiteralPath $auditPath -Value "$(Get-Date -Format o) $Message" -Encoding utf8
}

function Set-GuardMode {
    param([Parameter(Mandatory)][ValidateSet('legacy', 'observe', 'disabled')][string]$Mode)
    Set-WeKnoraAtomicText -Path $GuardModePath -Value $Mode
}

function Wait-GuardHeartbeat {
    param(
        [Parameter(Mandatory)][ValidateSet('legacy', 'observe', 'disabled')][string]$Mode,
        [Parameter(Mandatory)][DateTimeOffset]$NotBefore
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($HeartbeatTimeoutSeconds)
    do {
        if (Test-Path -LiteralPath $GuardHeartbeatPath -PathType Leaf) {
            try {
                $heartbeat = Get-Content -LiteralPath $GuardHeartbeatPath -Raw | ConvertFrom-Json
                $updated = [DateTimeOffset]::Parse([string]$heartbeat.updated_at)
                if ($heartbeat.mode -eq $Mode -and $updated -ge $NotBefore) { return $heartbeat }
            }
            catch {}
        }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "GPU Guard did not confirm mode $Mode within $HeartbeatTimeoutSeconds seconds"
}

function Set-ControllerObserveOnly {
    param([Parameter(Mandatory)][bool]$Value)
    & $binaryPath -config $configPath -set-observe-only $Value.ToString().ToLowerInvariant()
    if ($LASTEXITCODE -ne 0) { throw "failed to set controller observe_only=$Value" }
    Restart-Service -Name $ServiceName
    (Get-Service -Name $ServiceName).WaitForStatus('Running', [TimeSpan]::FromSeconds(45))
}

function Switch-Owner {
    param([Parameter(Mandatory)][ValidateSet('legacy', 'controller')][string]$NextOwner)
    $currentOwner = Get-WeKnoraEngineOwner -Path $OwnerPath
    if ($currentOwner -eq $NextOwner) { return }
    Invoke-WeKnoraEngineActuation `
        -OwnerPath $OwnerPath `
        -OwnerMutex $OwnerMutex `
        -ExpectedOwner $currentOwner `
        -Action { Set-WeKnoraEngineOwner -Path $OwnerPath -Owner $NextOwner }
}

if (-not $PSCmdlet.ShouldProcess("engine Docker owner -> $Target", 'Perform atomic ownership handoff')) {
    return
}

$beforeOwner = Get-WeKnoraEngineOwner -Path $OwnerPath
Write-HandoffAudit "begin target=$Target previous_owner=$beforeOwner service=$($service.Status) guard=$($guardTask.State)"

if ($Target -eq 'controller') {
    Set-ControllerObserveOnly -Value $true
    $transition = [DateTimeOffset]::UtcNow
    Set-GuardMode -Mode observe
    if ((Get-ScheduledTask -TaskName $GuardTaskName).State -ne 'Running') {
        Start-ScheduledTask -TaskName $GuardTaskName
    }
    $heartbeat = Wait-GuardHeartbeat -Mode observe -NotBefore $transition
    Switch-Owner -NextOwner controller
    Set-ControllerObserveOnly -Value $false
    Write-HandoffAudit "complete target=controller guard_pid=$($heartbeat.pid) owner=controller observe_only=false"
}
else {
    Set-ControllerObserveOnly -Value $true
    Switch-Owner -NextOwner legacy
    $transition = [DateTimeOffset]::UtcNow
    Set-GuardMode -Mode legacy
    if ((Get-ScheduledTask -TaskName $GuardTaskName).State -ne 'Running') {
        Start-ScheduledTask -TaskName $GuardTaskName
    }
    $heartbeat = Wait-GuardHeartbeat -Mode legacy -NotBefore $transition
    Write-HandoffAudit "complete target=legacy guard_pid=$($heartbeat.pid) owner=legacy observe_only=true"
}

[pscustomobject]@{
    Owner = Get-WeKnoraEngineOwner -Path $OwnerPath
    GuardMode = (Get-Content -LiteralPath $GuardModePath -Raw).Trim()
    ControllerStatus = (Get-Service -Name $ServiceName).Status
    Audit = $auditPath
}
