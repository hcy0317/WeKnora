[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

$interlockPath = Join-Path $PSScriptRoot 'weknora-engine-interlock.ps1'
$guardPath = Join-Path $PSScriptRoot 'weknora-gpu-guard.ps1'
$watchdogPath = Join-Path $PSScriptRoot 'weknora-ollama-watchdog.ps1'
$handoffPath = Join-Path $PSScriptRoot 'weknora-engine-handoff.ps1'
$guardInstallerPath = Join-Path $PSScriptRoot 'install-weknora-gpu-guard.ps1'

Assert-True (Test-Path -LiteralPath $interlockPath -PathType Leaf) 'shared engine interlock is missing'
Assert-True (Test-Path -LiteralPath $watchdogPath -PathType Leaf) 'standalone Ollama watchdog is missing'
Assert-True (Test-Path -LiteralPath $handoffPath -PathType Leaf) 'atomic owner handoff tool is missing'
Assert-True (Test-Path -LiteralPath $guardInstallerPath -PathType Leaf) 'GPU Guard installer is missing'

. $interlockPath
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ("weknora-owner-test-{0}" -f [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
try {
    $ownerPath = Join-Path $testRoot 'owner.txt'
    Set-WeKnoraEngineOwner -Path $ownerPath -Owner legacy
    Assert-True ((Get-WeKnoraEngineOwner -Path $ownerPath) -eq 'legacy') 'legacy owner was not persisted'

    $called = $false
    Invoke-WeKnoraEngineActuation `
        -OwnerPath $ownerPath `
        -OwnerMutex ("Local\WeKnoraOwnerContract-{0}" -f $PID) `
        -ExpectedOwner legacy `
        -Action { $script:called = $true }
    Assert-True $called 'legacy owner could not actuate under the interlock'

    Set-WeKnoraEngineOwner -Path $ownerPath -Owner controller
    $rejected = $false
    try {
        Invoke-WeKnoraEngineActuation `
            -OwnerPath $ownerPath `
            -OwnerMutex ("Local\WeKnoraOwnerContract-{0}" -f $PID) `
            -ExpectedOwner legacy `
            -Action { throw 'must not run' }
    }
    catch {
        $rejected = $_.Exception.Message -match 'owner mismatch'
    }
    Assert-True $rejected 'owner mismatch did not fail closed'

    $legacyGuardProcess = [pscustomobject]@{
        Name = 'pwsh.exe'
        CommandLine = 'pwsh.exe -File C:\WeKnora\scripts\weknora-gpu-guard.ps1'
    }
    $unrelatedProcess = [pscustomobject]@{
        Name = 'pwsh.exe'
        CommandLine = 'pwsh.exe -File C:\WeKnora\scripts\weknora-ollama-watchdog.ps1'
    }
    Assert-True (Test-WeKnoraEngineProcessMatch `
        -Process $legacyGuardProcess `
        -CommandLineNeedles @('\weknora-gpu-guard.ps1')) 'legacy Guard process was not identified'
    Assert-True (-not (Test-WeKnoraEngineProcessMatch `
        -Process $unrelatedProcess `
        -CommandLineNeedles @('\weknora-gpu-guard.ps1'))) 'unrelated watchdog process was matched'

    $guardSource = Get-Content -LiteralPath $guardPath -Raw
    $watchdogSource = Get-Content -LiteralPath $watchdogPath -Raw
    $guardInstallerSource = Get-Content -LiteralPath $guardInstallerPath -Raw
    Assert-True ($guardSource -notmatch 'Ensure-OllamaRunning|Maintain-Ollama') 'GPU Guard still owns Ollama'
    Assert-True ($guardSource -match "legacy.+observe.+disabled") 'GPU Guard modes are missing'
    Assert-True ($watchdogSource -match 'Ensure-OllamaRunning') 'Ollama watchdog behavior was not preserved'
    Assert-True ($guardInstallerSource -match 'DeferGpuGuardStart') 'GPU Guard installer cannot defer orphan migration'
    Assert-True ($guardInstallerSource -match 'GPUGuardStartDeferred') 'deferred Guard state is not reported'
    Assert-True ($guardInstallerSource -match '\$gpuTriggers\s*=\s*if\s*\(\$DeferGpuGuardStart\)') 'deferred Guard is not isolated to a future trigger'
}
finally {
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'engine host ownership contract passed'
