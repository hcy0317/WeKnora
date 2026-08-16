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
    [int]$OllamaStartupTimeoutSeconds = 20,
    [int]$DockerCommandTimeoutSeconds = 15,
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'weknora-process.ps1')
$composeRoot = Split-Path -Parent $PSScriptRoot
$composeFiles = @(
    '-f', (Join-Path $composeRoot 'docker-compose.yml'),
    '-f', (Join-Path $composeRoot 'docker-compose.override.yml')
)
$rerankerContainer = 'WeKnora-qwen-reranker-gpu'
$ocrApiContainer = 'WeKnora-paddleocr-vl'
$ocrVlmContainer = 'WeKnora-paddleocr-vlm-server'
$stateRoot = Join-Path $env:LOCALAPPDATA 'WeKnora'
$logPath = Join-Path $stateRoot 'gpu-guard.log'
$rerankerCooldownPath = Join-Path $stateRoot 'gpu-guard-reranker-cooldown-until.txt'
$ocrCooldownPath = Join-Path $stateRoot 'gpu-guard-ocr-cooldown-until.txt'
$ollamaCooldownPath = Join-Path $stateRoot 'gpu-guard-ollama-cooldown-until.txt'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]]$ArgumentList,
        [int]$TimeoutSeconds = $DockerCommandTimeoutSeconds
    )
    $docker = Get-Command docker.exe -ErrorAction Stop
    Invoke-GuardProcess -FilePath $docker.Source -ArgumentList $ArgumentList -TimeoutSeconds $TimeoutSeconds
}

function Write-GuardLog {
    param(
        [Parameter(Mandatory)]
        [string]$Message,
        [switch]$ErrorRecord
    )
    $level = if ($ErrorRecord) { 'ERROR' } else { 'INFO' }
    $line = "$(Get-Date -Format o) [$level] $Message"
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
    if ($Once) {
        if ($ErrorRecord) { [Console]::Error.WriteLine($line) }
        else { Write-Output $line }
    }
}

function Get-CooldownRemaining {
    param([Parameter(Mandatory)][string]$Path)
    if ($ReenableCooldownSeconds -le 0 -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return 0
    }
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
    $exitCode = $LASTEXITCODE
    $line = $lines | Select-Object -First 1
    if ($exitCode -ne 0 -or -not $line) { throw 'nvidia-smi did not return GPU data' }
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

function Test-OllamaReady {
    try {
        Invoke-WebRequest `
            -UseBasicParsing `
            -Uri 'http://127.0.0.1:11434/api/tags' `
            -TimeoutSec 3 | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Ensure-OllamaRunning {
    if (Test-OllamaReady) { return }

    $ollama = Get-Command ollama.exe -ErrorAction Stop
    $serveProcess = Get-CimInstance Win32_Process -Filter "Name = 'ollama.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match '(?:^|\s)serve(?:\s|$)' } |
        Select-Object -First 1
    if (-not $serveProcess) {
        $started = Start-Process `
            -FilePath $ollama.Source `
            -ArgumentList 'serve' `
            -WindowStyle Hidden `
            -PassThru
        Write-GuardLog "ollama->starting pid=$($started.Id)"
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds([math]::Max(5, $OllamaStartupTimeoutSeconds))
    do {
        Start-Sleep -Seconds 1
        if (Test-OllamaReady) {
            Write-GuardLog 'ollama->ready endpoint=http://127.0.0.1:11434'
            return
        }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)

    throw "Ollama did not become ready within $OllamaStartupTimeoutSeconds seconds"
}

function Maintain-Ollama {
    if (Test-OllamaReady) {
        Remove-Item -LiteralPath $ollamaCooldownPath -Force -ErrorAction SilentlyContinue
        return $true
    }

    $cooldown = Get-CooldownRemaining $ollamaCooldownPath
    if ($cooldown -gt 0) {
        if ($Once) { Write-GuardLog "ollama unavailable; startup cooldown=${cooldown}s" -ErrorRecord }
        return $false
    }

    try {
        Ensure-OllamaRunning
        Remove-Item -LiteralPath $ollamaCooldownPath -Force -ErrorAction SilentlyContinue
        return $true
    }
    catch {
        Start-Cooldown $ollamaCooldownPath
        Write-GuardLog "Ollama watchdog failed closed; retry after ${ReenableCooldownSeconds}s: $($_.Exception.Message)" -ErrorRecord
        return $false
    }
}

function Enable-GpuOcr {
    $result = Invoke-Docker -ArgumentList (@('compose') + $composeFiles + @('--profile', 'paddle-gpu', 'up', '-d', '--no-build', 'paddleocr-vl')) -TimeoutSeconds 120
    if ($result.ExitCode -ne 0) { throw "failed to start PaddleOCR-VL GPU services: $($result.ErrorOutput -join ' ')" }
}

function Disable-GpuOcr {
    # Uvicorn drains an active /layout-parsing request after SIGTERM. Match the
    # WeKnora 1000 second request window before stopping the VLM backend.
    if (Test-ContainerRunning $ocrApiContainer) {
        $result = Invoke-Docker -ArgumentList @('stop', '--timeout', '1000', $ocrApiContainer) -TimeoutSeconds 1020
        if ($result.ExitCode -ne 0) { throw "failed to drain PaddleOCR-VL API: $($result.ErrorOutput -join ' ')" }
    }
    if (Test-ContainerRunning $ocrVlmContainer) {
        $result = Invoke-Docker -ArgumentList @('stop', '--timeout', '60', $ocrVlmContainer) -TimeoutSeconds 80
        if ($result.ExitCode -ne 0) { throw "failed to stop PaddleOCR-VL VLM backend: $($result.ErrorOutput -join ' ')" }
    }
}

function Enable-GpuReranker {
    $result = Invoke-Docker -ArgumentList (@('compose') + $composeFiles + @('--profile', 'gpu', 'up', '-d', '--no-build', 'qwen-reranker-gpu')) -TimeoutSeconds 120
    if ($result.ExitCode -ne 0) { throw "failed to start the GPU reranker: $($result.ErrorOutput -join ' ')" }
}

function Disable-GpuReranker {
    if (Test-ContainerRunning $rerankerContainer) {
        $result = Invoke-Docker -ArgumentList @('stop', '--timeout', '20', $rerankerContainer) -TimeoutSeconds 40
        if ($result.ExitCode -ne 0) { throw "failed to stop the GPU reranker: $($result.ErrorOutput -join ' ')" }
    }
}

Write-GuardLog "started reranker_enable<=${RerankerEnableBelowPercent}% reranker_disable>=${RerankerDisableAtPercent}% ocr_vllm=20% ocr_enable<=${OcrEnableBelowPercent}% ocr_disable>=${OcrDisableAtPercent}% poll=${PollSeconds}s"

do {
    # GPU pressure is safety-critical; never let a slow local Ollama startup
    # delay OCR/reranker load shedding.
    try {
        $pressure = Get-GpuPressure
        $ocrRunning = Test-OcrRunning
        $rerankerRunning = Test-ContainerRunning $rerankerContainer
        $ocrCooldown = Get-CooldownRemaining $ocrCooldownPath
        $rerankerCooldown = Get-CooldownRemaining $rerankerCooldownPath

        if ($ocrRunning -and ($pressure.UsedPercent -ge $OcrDisableAtPercent -or $pressure.FreeMiB -lt $OcrCriticalFreeMiB)) {
            Disable-GpuReranker
            Start-Cooldown $rerankerCooldownPath
            Disable-GpuOcr
            Start-Cooldown $ocrCooldownPath
            Write-GuardLog "ocr-gpu->paused used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB cooldown=${ReenableCooldownSeconds}s"
        }
        elseif (-not $ocrRunning -and (Test-OcrImagesReady) -and $ocrCooldown -le 0 -and
                $pressure.UsedPercent -le $OcrEnableBelowPercent -and $pressure.FreeMiB -ge $OcrMinimumFreeMiB) {
            Disable-GpuReranker
            Start-Cooldown $rerankerCooldownPath
            Enable-GpuOcr
            Write-GuardLog "ocr-paused->gpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB vllm=20%"
        }
        elseif ($rerankerRunning -and ($pressure.UsedPercent -ge $RerankerDisableAtPercent -or $pressure.FreeMiB -lt 2048)) {
            Disable-GpuReranker
            Start-Cooldown $rerankerCooldownPath
            Write-GuardLog "reranker-gpu->cpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB cooldown=${ReenableCooldownSeconds}s"
        }
        elseif (-not $rerankerRunning -and (Test-ImageReady 'weknora-qwen-reranker-gpu:latest') -and
                $rerankerCooldown -le 0 -and $pressure.UsedPercent -le $RerankerEnableBelowPercent -and
                $pressure.FreeMiB -ge $RerankerMinimumFreeMiB) {
            Enable-GpuReranker
            Write-GuardLog "reranker-cpu->gpu used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB"
        }
        elseif ($Once) {
            $ocrMode = if ($ocrRunning) { 'gpu' } else { 'paused' }
            $rerankerMode = if ($rerankerRunning) { 'gpu' } else { 'cpu' }
            Write-GuardLog "keep ocr=$ocrMode reranker=$rerankerMode used=$($pressure.UsedPercent)% free=$($pressure.FreeMiB)MiB util=$($pressure.UtilizationPercent)% ocr_cooldown=${ocrCooldown}s reranker_cooldown=${rerankerCooldown}s"
        }
    }
    catch {
        try { Disable-GpuReranker }
        catch { Write-GuardLog "failed to stop GPU reranker during guard error: $($_.Exception.Message)" -ErrorRecord }
        Start-Cooldown $rerankerCooldownPath
        Write-GuardLog "GPU guard kept OCR state and failed reranker closed to CPU: $($_.Exception.Message)" -ErrorRecord
        if ($Once) { exit 1 }
    }

    $ollamaReady = Maintain-Ollama
    if ($Once -and -not $ollamaReady) { exit 1 }

    if (-not $Once) { Start-Sleep -Seconds ([math]::Max(10, $PollSeconds)) }
} while (-not $Once)
