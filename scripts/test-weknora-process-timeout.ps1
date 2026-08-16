$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'weknora-process.ps1')

$startedAt = [DateTimeOffset]::UtcNow
$timedOut = $false
try {
    Invoke-GuardProcess `
        -FilePath $env:ComSpec `
        -ArgumentList @('/d', '/c', 'ping', '-n', '10', '127.0.0.1') `
        -TimeoutSeconds 1 | Out-Null
}
catch {
    $timedOut = $_.Exception.Message -match 'timed out after 1 second'
    if (-not $timedOut) {
        throw "unexpected timeout error: $($_.Exception.GetType().FullName): $($_.Exception.Message)"
    }
}
$elapsed = ([DateTimeOffset]::UtcNow - $startedAt).TotalSeconds

if (-not $timedOut) { throw 'expected the child process to time out' }
if ($elapsed -gt 5) { throw "timeout enforcement took too long: $([math]::Round($elapsed, 2))s" }

Write-Output "PASS process timeout enforced in $([math]::Round($elapsed, 2))s"
