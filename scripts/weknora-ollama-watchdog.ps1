[CmdletBinding()]
param(
    [int]$ReenableCooldownSeconds = 300,
    [int]$PollSeconds = 20,
    [int]$OllamaStartupTimeoutSeconds = 20,
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$stateRoot = Join-Path $env:LOCALAPPDATA 'WeKnora'
$logPath = Join-Path $stateRoot 'ollama-watchdog.log'
$cooldownPath = Join-Path $stateRoot 'ollama-watchdog-cooldown-until.txt'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null

function Write-WatchdogLog {
    param([Parameter(Mandatory)][string]$Message, [switch]$ErrorRecord)
    $level = if ($ErrorRecord) { 'ERROR' } else { 'INFO' }
    $line = "$(Get-Date -Format o) [$level] $Message"
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
    if ($Once) {
        if ($ErrorRecord) { [Console]::Error.WriteLine($line) } else { Write-Output $line }
    }
}

function Get-CooldownRemaining {
    if ($ReenableCooldownSeconds -le 0 -or -not (Test-Path -LiteralPath $cooldownPath -PathType Leaf)) {
        return 0
    }
    $raw = (Get-Content -LiteralPath $cooldownPath -Raw -ErrorAction SilentlyContinue).Trim()
    $until = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse($raw, [ref]$until)) { return 0 }
    return [math]::Max(0, [math]::Ceiling(($until - [DateTimeOffset]::UtcNow).TotalSeconds))
}

function Start-Cooldown {
    if ($ReenableCooldownSeconds -le 0) { return }
    [DateTimeOffset]::UtcNow.AddSeconds($ReenableCooldownSeconds).ToString('o') |
        Set-Content -LiteralPath $cooldownPath -Encoding ascii
}

function Test-OllamaReady {
    try {
        Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:11434/api/tags' -TimeoutSec 3 | Out-Null
        return $true
    }
    catch { return $false }
}

function Ensure-OllamaRunning {
    if (Test-OllamaReady) { return }
    $ollama = Get-Command ollama.exe -ErrorAction Stop
    $serveProcess = Get-CimInstance Win32_Process -Filter "Name = 'ollama.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match '(?:^|\s)serve(?:\s|$)' } |
        Select-Object -First 1
    if (-not $serveProcess) {
        $started = Start-Process -FilePath $ollama.Source -ArgumentList 'serve' -WindowStyle Hidden -PassThru
        Write-WatchdogLog "ollama->starting pid=$($started.Id)"
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds([math]::Max(5, $OllamaStartupTimeoutSeconds))
    do {
        Start-Sleep -Seconds 1
        if (Test-OllamaReady) {
            Write-WatchdogLog 'ollama->ready endpoint=http://127.0.0.1:11434'
            return
        }
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Ollama did not become ready within $OllamaStartupTimeoutSeconds seconds"
}

Write-WatchdogLog "started poll=${PollSeconds}s cooldown=${ReenableCooldownSeconds}s"
do {
    if (Test-OllamaReady) {
        Remove-Item -LiteralPath $cooldownPath -Force -ErrorAction SilentlyContinue
    }
    else {
        $cooldown = Get-CooldownRemaining
        if ($cooldown -le 0) {
            try {
                Ensure-OllamaRunning
                Remove-Item -LiteralPath $cooldownPath -Force -ErrorAction SilentlyContinue
            }
            catch {
                Start-Cooldown
                Write-WatchdogLog "Ollama watchdog failed closed; retry after ${ReenableCooldownSeconds}s: $($_.Exception.Message)" -ErrorRecord
                if ($Once) { exit 1 }
            }
        }
        elseif ($Once) {
            Write-WatchdogLog "ollama unavailable; startup cooldown=${cooldown}s" -ErrorRecord
            exit 1
        }
    }
    if (-not $Once) { Start-Sleep -Seconds ([math]::Max(10, $PollSeconds)) }
} while (-not $Once)
