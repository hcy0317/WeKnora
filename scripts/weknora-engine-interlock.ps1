Set-StrictMode -Version Latest

function Get-WeKnoraEngineOwner {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "engine Docker owner state is missing: $Path"
    }
    $file = Get-Item -LiteralPath $Path
    if ($file.Length -gt 64) { throw 'engine Docker owner state is too large' }
    $owner = (Get-Content -LiteralPath $Path -Raw).Trim()
    if ($owner -notin @('legacy', 'controller')) {
        throw "invalid engine Docker owner state: $owner"
    }
    return $owner
}

function Set-WeKnoraAtomicText {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Value
    )

    $directory = Split-Path -Parent ([IO.Path]::GetFullPath($Path))
    if (-not (Test-Path -LiteralPath $directory -PathType Container)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }
    $temporary = Join-Path $directory ('.weknora-engine-{0}.tmp' -f [Guid]::NewGuid().ToString('N'))
    $backup = "$Path.previous"
    try {
        $bytes = [Text.Encoding]::ASCII.GetBytes("$Value`n")
        $stream = [IO.FileStream]::new(
            $temporary,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None,
            4096,
            [IO.FileOptions]::WriteThrough
        )
        try {
            $stream.Write($bytes, 0, $bytes.Length)
            $stream.Flush($true)
        }
        finally {
            $stream.Dispose()
        }
        if (Test-Path -LiteralPath $Path -PathType Leaf) {
            [IO.File]::Replace($temporary, $Path, $backup, $true)
        }
        else {
            [IO.File]::Move($temporary, $Path)
        }
    }
    finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

function Set-WeKnoraEngineOwner {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][ValidateSet('legacy', 'controller')][string]$Owner
    )

    Set-WeKnoraAtomicText -Path $Path -Value $Owner
}

function Invoke-WeKnoraEngineActuation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$OwnerPath,
        [Parameter(Mandatory)][string]$OwnerMutex,
        [Parameter(Mandatory)][ValidateSet('legacy', 'controller')][string]$ExpectedOwner,
        [Parameter(Mandatory)][scriptblock]$Action,
        [ValidateRange(1, 60)][int]$LockTimeoutSeconds = 5
    )

    $mutex = [Threading.Mutex]::new($false, $OwnerMutex)
    $acquired = $false
    try {
        try {
            $acquired = $mutex.WaitOne([TimeSpan]::FromSeconds($LockTimeoutSeconds))
        }
        catch [Threading.AbandonedMutexException] {
            $acquired = $true
        }
        if (-not $acquired) {
            throw "engine Docker owner interlock is busy: $OwnerMutex"
        }
        $actualOwner = Get-WeKnoraEngineOwner -Path $OwnerPath
        if ($actualOwner -ne $ExpectedOwner) {
            throw "engine Docker owner mismatch: expected=$ExpectedOwner actual=$actualOwner"
        }
        & $Action
    }
    finally {
        if ($acquired) { $mutex.ReleaseMutex() }
        $mutex.Dispose()
    }
}

function Test-WeKnoraEngineProcessMatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object]$Process,
        [Parameter(Mandatory)][string[]]$CommandLineNeedles
    )

    if ($Process.Name -notin @('pwsh.exe', 'powershell.exe', 'wscript.exe', 'cscript.exe')) {
        return $false
    }
    $commandLine = [string]$Process.CommandLine
    if ([string]::IsNullOrWhiteSpace($commandLine)) { return $false }
    foreach ($needle in $CommandLineNeedles) {
        if (-not [string]::IsNullOrWhiteSpace($needle) -and
            $commandLine.IndexOf($needle, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

function Wait-WeKnoraEngineProcessesExit {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]]$CommandLineNeedles,
        [ValidateRange(1, 120)][int]$TimeoutSeconds = 15
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $matching = @(Get-CimInstance Win32_Process | Where-Object {
            Test-WeKnoraEngineProcessMatch -Process $_ -CommandLineNeedles $CommandLineNeedles
        })
        if ($matching.Count -eq 0) { return }
        Start-Sleep -Milliseconds 250
    } while ([DateTimeOffset]::UtcNow -lt $deadline)

    $details = $matching | ForEach-Object { '{0}:{1}' -f $_.Name, $_.ProcessId }
    throw "engine guard processes did not exit: $($details -join ', ')"
}
