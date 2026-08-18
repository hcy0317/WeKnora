[CmdletBinding()]
param(
    [int]$RerankerEnableBelowPercent = 70,
    [int]$RerankerDisableAtPercent = 85,
    [int]$RerankerMinimumFreeMiB = 3500,
    [int]$OcrEnableBelowPercent = 60,
    [int]$OcrDisableAtPercent = 92,
    [int]$OcrMinimumFreeMiB = 7000,
    [int]$OcrCriticalFreeMiB = 1024,
    [int]$ReenableCooldownSeconds = 300,
    [int]$PollSeconds = 20,
    [int]$DockerCommandTimeoutSeconds = 15,
    [string]$OwnerMutex = 'Global\WeKnoraEngineDockerOwner',
    [string]$OwnerPath = (Join-Path $env:ProgramData 'WeKnora\engine-ownership\owner.txt'),
    [string]$ModePath = (Join-Path $env:LOCALAPPDATA 'WeKnora\gpu-guard-mode.txt'),
    [string]$HeartbeatPath = (Join-Path $env:LOCALAPPDATA 'WeKnora\gpu-guard-heartbeat.json'),
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'weknora-process.ps1')
. (Join-Path $PSScriptRoot 'weknora-engine-interlock.ps1')

$rerankerContainer = 'WeKnora-qwen-reranker-gpu'
$ocrApiContainer = 'WeKnora-paddleocr-vl'
$ocrVlmContainer = 'WeKnora-paddleocr-vlm-server'
$stateRoot = Join-Path $env:LOCALAPPDATA 'WeKnora'
$logPath = Join-Path $stateRoot 'gpu-guard.log'
$rerankerCooldownPath = Join-Path $stateRoot 'gpu-guard-reranker-cooldown-until.txt'
$ocrCooldownPath = Join-Path $stateRoot 'gpu-guard-ocr-cooldown-until.txt'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null

# Supported modes: legacy, observe, disabled. Only legacy may actuate, and it
# must still hold the shared mutex while the owner state says legacy.
function Get-GuardMode {
    if (-not (Test-Path -LiteralPath $ModePath -PathType Leaf)) {
        throw "GPU Guard mode is missing: $ModePath"
    }
    $mode = (Get-Content -LiteralPath $ModePath -Raw).Trim()
    if ($mode -notin @('legacy', 'observe', 'disabled')) {
        throw "invalid GPU Guard mode: $mode"
    }
    return $mode
}

function Write-GuardHeartbeat {
    param([Parameter(Mandatory)][string]$Mode, [string]$Decision = 'poll')
    $owner = try { Get-WeKnoraEngineOwner -Path $OwnerPath } catch { 'unavailable' }
    $heartbeat = [ordered]@{
        schema_version = 1
        updated_at = [DateTimeOffset]::UtcNow.ToString('o')
        pid = $PID
        mode = $Mode
        owner = $owner
        decision = $Decision
    } | ConvertTo-Json -Compress
    Set-WeKnoraAtomicText -Path $HeartbeatPath -Value $heartbeat
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]]$ArgumentList,
        [int]$TimeoutSeconds = $DockerCommandTimeoutSeconds
    )
    $docker = Get-Command docker.exe -ErrorAction Stop
    Invoke-GuardProcess -FilePath $docker.Source -ArgumentList $ArgumentList -TimeoutSeconds $TimeoutSeconds
}

function Write-GuardLog {
    param([Parameter(Mandatory)][string]$Message, [switch]$ErrorRecord)
    $level = if ($ErrorRecord) { 'ERROR' } else { 'INFO' }
    $line = "$(Get-Date -Format o) [$level] $Message"
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
    if ($Once) {
        if ($ErrorRecord) { [Console]::Error.WriteLine($line) } else { Write-Output $line }
    }
}

function Get-CooldownRemaining {
    param([Parameter(Mandatory)][string]$Path)
    if ($ReenableCooldownSeconds -le 0 -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) { return 0 }
    $raw = (Get-Content -LiteralPath $Path -Raw -ErrorAction SilentlyContinue).Trim()
    $until = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse($raw, [ref]$until)) { return 0 }
    return [math]::Max(0, [math]::Ceiling(($until - [DateTimeOffset]::UtcNow).TotalSeconds))
}

function Start-Cooldown {
    param([Parameter(Mandatory)][string]$Path)
    if ($ReenableCooldownSeconds -le 0) { return }
    [DateTimeOffset]::UtcNow.AddSeconds($ReenableCooldownSeconds).ToString('o') |
        Set-Content -LiteralPath $Path -Encoding ascii
}

function Get-GpuPressure {
    $nvidiaSmi = Get-Command nvidia-smi.exe -ErrorAction Stop
    $lines = @(& $nvidiaSmi.Source '--query-gpu=memory.total,memory.used,memory.free,utilization.gpu' '--format=csv,noheader,nounits')
    $line = $lines | Select-Object -First 1
    if ($LASTEXITCODE -ne 0 -or -not $line) { throw 'nvidia-smi did not return GPU data' }
    $values = $line -split ',' | ForEach-Object { [int]$_.Trim() }
    [pscustomobject]@{
        TotalMiB = $values[0]
        UsedMiB = $values[1]
        FreeMiB = $values[2]
        UtilizationPercent = $values[3]
        UsedPercent = [math]::Round(($values[1] * 100.0) / $values[0], 1)
    }
}

function Test-ContainerRunning {
    param([Parameter(Mandatory)][string]$Name)
    $result = Invoke-Docker -ArgumentList @('inspect', '--format', '{{.State.Running}}', $Name)
    $status = $result.Output | Select-Object -First 1
    return $result.ExitCode -eq 0 -and $status -eq 'true'
}

function Test-ImageReady {
    param([Parameter(Mandatory)][string]$Name)
    $result = Invoke-Docker -ArgumentList @('image', 'inspect', $Name)
    return $result.ExitCode -eq 0
}

function Test-OcrRunning {
    return (Test-ContainerRunning $ocrApiContainer) -and (Test-ContainerRunning $ocrVlmContainer)
}

function Test-OcrImagesReady {
    return (Test-ImageReady 'ccr-2vdh3abv-pub.cnc.bj.baidubce.com/paddlepaddle/paddleocr-vl:latest-nvidia-gpu') -and
        (Test-ImageReady 'ccr-2vdh3abv-pub.cnc.bj.baidubce.com/paddlepaddle/paddleocr-genai-vllm-server:latest-nvidia-gpu')
}

function Assert-DockerSucceeded {
    param([Parameter(Mandatory)]$Result, [Parameter(Mandatory)][string]$Message)
    if ($Result.ExitCode -ne 0) { throw "$Message`: $($Result.ErrorOutput -join ' ')" }
}

function Enable-GpuOcr {
    Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('start', $ocrVlmContainer) -TimeoutSeconds 120) 'failed to start PaddleOCR-VL backend'
    Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('start', $ocrApiContainer) -TimeoutSeconds 120) 'failed to start PaddleOCR-VL API'
}

function Disable-GpuOcr {
    if (Test-ContainerRunning $ocrApiContainer) {
        Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('stop', '--timeout', '1000', $ocrApiContainer) -TimeoutSeconds 1020) 'failed to drain PaddleOCR-VL API'
    }
    if (Test-ContainerRunning $ocrVlmContainer) {
        Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('stop', '--timeout', '60', $ocrVlmContainer) -TimeoutSeconds 80) 'failed to stop PaddleOCR-VL backend'
    }
}

function Enable-GpuReranker {
    Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('start', $rerankerContainer) -TimeoutSeconds 120) 'failed to start GPU reranker'
}

function Disable-GpuReranker {
    if (Test-ContainerRunning $rerankerContainer) {
        Assert-DockerSucceeded (Invoke-Docker -ArgumentList @('stop', '--timeout', '20', $rerankerContainer) -TimeoutSeconds 40) 'failed to stop GPU reranker'
    }
}

function Invoke-GuardDecision {
    param(
        [Parameter(Mandatory)][string]$Mode,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][scriptblock]$Action
    )
    if ($Mode -eq 'observe') {
        Write-GuardLog "observe would=$Name"
        Write-GuardHeartbeat -Mode $Mode -Decision "would:$Name"
        return
    }
    if ($Mode -ne 'legacy') { throw "GPU Guard cannot actuate in mode $Mode" }
    Invoke-WeKnoraEngineActuation -OwnerPath $OwnerPath -OwnerMutex $OwnerMutex -ExpectedOwner legacy -Action $Action
    Write-GuardHeartbeat -Mode $Mode -Decision $Name
}

Write-GuardLog "started reranker_enable<=${RerankerEnableBelowPercent}% reranker_disable>=${RerankerDisableAtPercent}% ocr_enable<=${OcrEnableBelowPercent}% ocr_disable>=${OcrDisableAtPercent}% poll=${PollSeconds}s"

do {
    $mode = Get-GuardMode
    Write-GuardHeartbeat -Mode $mode
    if ($mode -eq 'disabled') {
        Write-GuardLog 'disabled; no managed engine actuation will run'
        exit 0
    }

    try {
        $pressure = Get-GpuPressure
        $ocrRunning = Test-OcrRunning
        $rerankerRunning = Test-ContainerRunning $rerankerContainer
        $ocrCooldown = Get-CooldownRemaining $ocrCooldownPath
        $rerankerCooldown = Get-CooldownRemaining $rerankerCooldownPath

        if ($ocrRunning -and ($pressure.UsedPercent -ge $OcrDisableAtPercent -or $pressure.FreeMiB -lt $OcrCriticalFreeMiB)) {
            Invoke-GuardDecision -Mode $mode -Name 'ocr-gpu->paused' -Action {
                Disable-GpuReranker
                Start-Cooldown $rerankerCooldownPath
                Disable-GpuOcr
                Start-Cooldown $ocrCooldownPath
            }
            Write-GuardLog "ocr-gpu->paused used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB cooldown=${ReenableCooldownSeconds}s"
        }
        elseif (-not $ocrRunning -and (Test-OcrImagesReady) -and $ocrCooldown -le 0 -and
                $pressure.UsedPercent -le $OcrEnableBelowPercent -and $pressure.FreeMiB -ge $OcrMinimumFreeMiB) {
            Invoke-GuardDecision -Mode $mode -Name 'ocr-paused->gpu' -Action {
                Disable-GpuReranker
                Start-Cooldown $rerankerCooldownPath
                Enable-GpuOcr
            }
            Write-GuardLog "ocr-paused->gpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB"
        }
        elseif ($rerankerRunning -and ($pressure.UsedPercent -ge $RerankerDisableAtPercent -or $pressure.FreeMiB -lt 2048)) {
            Invoke-GuardDecision -Mode $mode -Name 'reranker-gpu->cpu' -Action {
                Disable-GpuReranker
                Start-Cooldown $rerankerCooldownPath
            }
            Write-GuardLog "reranker-gpu->cpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB cooldown=${ReenableCooldownSeconds}s"
        }
        elseif (-not $rerankerRunning -and (Test-ImageReady 'weknora-qwen-reranker-gpu:latest') -and
                $rerankerCooldown -le 0 -and $pressure.UsedPercent -le $RerankerEnableBelowPercent -and
                $pressure.FreeMiB -ge $RerankerMinimumFreeMiB) {
            Invoke-GuardDecision -Mode $mode -Name 'reranker-cpu->gpu' -Action { Enable-GpuReranker }
            Write-GuardLog "reranker-cpu->gpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB"
        }
        elseif ($Once) {
            $ocrMode = if ($ocrRunning) { 'gpu' } else { 'paused' }
            $rerankerMode = if ($rerankerRunning) { 'gpu' } else { 'cpu' }
            Write-GuardLog "keep mode=$mode ocr=$ocrMode reranker=$rerankerMode used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB util=$($pressure.UtilizationPercent)%"
        }
    }
    catch {
        Write-GuardLog "GPU Guard failed closed without changing ownership: $($_.Exception.Message)" -ErrorRecord
        if ($Once) { exit 1 }
    }

    if (-not $Once) { Start-Sleep -Seconds ([math]::Max(10, $PollSeconds)) }
} while (-not $Once)
