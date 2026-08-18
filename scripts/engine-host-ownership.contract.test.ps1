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
$clientBundlePath = Join-Path $PSScriptRoot 'weknora-engine-client-bundle.ps1'
$controllerInstallerPath = Join-Path $PSScriptRoot 'install-engine-host-controller.ps1'
$composeOverridePath = Join-Path (Split-Path -Parent $PSScriptRoot) 'docker-compose.override.yml'

Assert-True (Test-Path -LiteralPath $interlockPath -PathType Leaf) 'shared engine interlock is missing'
Assert-True (Test-Path -LiteralPath $watchdogPath -PathType Leaf) 'standalone Ollama watchdog is missing'
Assert-True (Test-Path -LiteralPath $handoffPath -PathType Leaf) 'atomic owner handoff tool is missing'
Assert-True (Test-Path -LiteralPath $guardInstallerPath -PathType Leaf) 'GPU Guard installer is missing'
Assert-True (Test-Path -LiteralPath $clientBundlePath -PathType Leaf) 'container client TLS bundle exporter is missing'

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
    Assert-True ($guardInstallerSource -match 'Enable-ScheduledTask\s+-TaskName\s+\$TaskName') 'replacement tasks can inherit a disabled state'

    . $clientBundlePath
    $sourceTLSRoot = Join-Path $testRoot 'controller-tls'
    $clientTLSRoot = Join-Path $testRoot 'client-tls'
    foreach ($relativePath in @(
            'ca.crt',
            'server.crt',
            'server.key',
            'gateway\client.crt',
            'gateway\client.key',
            'backend\client.crt',
            'backend\client.key',
            'bootstrap\client.crt',
            'bootstrap\client.key'
        )) {
        $fixturePath = Join-Path $sourceTLSRoot $relativePath
        New-Item -ItemType Directory -Path (Split-Path -Parent $fixturePath) -Force | Out-Null
        Set-Content -LiteralPath $fixturePath -Value $relativePath -NoNewline
    }

    $currentUserSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    New-Item -ItemType Directory -Path $clientTLSRoot | Out-Null
    $preexistingAcl = Get-Acl -LiteralPath $clientTLSRoot
    $preexistingAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            [Security.Principal.SecurityIdentifier]::new('S-1-1-0'),
            [Security.AccessControl.FileSystemRights]::ReadAndExecute,
            [Security.AccessControl.AccessControlType]::Allow
        ))
    Set-Acl -LiteralPath $clientTLSRoot -AclObject $preexistingAcl
    Export-WeKnoraEngineClientBundle `
        -SourceTLSRoot $sourceTLSRoot `
        -DestinationRoot $clientTLSRoot `
        -ContainerRuntimeUserSid $currentUserSid | Out-Null

    $exportedFiles = @(Get-ChildItem -LiteralPath $clientTLSRoot -File -Recurse | ForEach-Object {
            [IO.Path]::GetRelativePath($clientTLSRoot, $_.FullName)
        } | Sort-Object)
    $expectedExportedFiles = @(
        'backend\client.crt',
        'backend\client.key',
        'ca.crt',
        'gateway\client.crt',
        'gateway\client.key'
    ) | Sort-Object
    Assert-True (($exportedFiles -join '|') -eq ($expectedExportedFiles -join '|')) 'client TLS export contains protected or missing files'

    $clientRootAcl = Get-Acl -LiteralPath $clientTLSRoot
    $runtimeUserRule = @($clientRootAcl.Access | Where-Object {
            $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -eq $currentUserSid -and
            $_.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and
            ($_.FileSystemRights -band [Security.AccessControl.FileSystemRights]::ReadAndExecute)
        })
    Assert-True ($runtimeUserRule.Count -gt 0) 'container runtime user cannot read the client TLS export'
    $allowedClientRootSids = @('S-1-5-18', 'S-1-5-32-544', $currentUserSid)
    $unexpectedAclRules = @($clientRootAcl.Access | Where-Object {
            $_.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and
            $allowedClientRootSids -notcontains $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
        })
    Assert-True ($unexpectedAclRules.Count -eq 0) 'client TLS export retained an allow rule for an unrelated principal'

    $controllerInstallerSource = Get-Content -LiteralPath $controllerInstallerPath -Raw
    $composeOverrideSource = Get-Content -LiteralPath $composeOverridePath -Raw
    Assert-True ($controllerInstallerSource -match 'ClientTLSRoot') 'controller installer does not publish the client TLS export path'
    Assert-True ($composeOverrideSource -match 'C:/ProgramData/WeKnora/engine-client-tls') 'Compose still defaults to the protected controller TLS directory'
}
finally {
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'engine host ownership contract passed'
