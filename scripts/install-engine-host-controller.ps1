[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$RepositoryRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$InstallRoot = (Join-Path $env:ProgramData 'WeKnora\engine-controller'),
    [string]$ServiceName = 'WeKnoraEngineHostController',
    [switch]$SkipStart
)

$ErrorActionPreference = 'Stop'

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'install-engine-host-controller.ps1 must run from an elevated PowerShell session'
}

$repository = (Resolve-Path -LiteralPath $RepositoryRoot).Path
$expectedModule = Join-Path $repository 'go.mod'
$exampleConfig = Join-Path $repository 'config\engine-controller.example.yaml'
$interlockSource = Join-Path $repository 'scripts\weknora-engine-interlock.ps1'
$handoffSource = Join-Path $repository 'scripts\weknora-engine-handoff.ps1'
if (-not (Test-Path -LiteralPath $expectedModule -PathType Leaf) -or
        -not (Test-Path -LiteralPath $exampleConfig -PathType Leaf) -or
        -not (Test-Path -LiteralPath $interlockSource -PathType Leaf) -or
        -not (Test-Path -LiteralPath $handoffSource -PathType Leaf)) {
    throw "RepositoryRoot is not a WeKnora checkout: $repository"
}

$installDirectory = [IO.Path]::GetFullPath($InstallRoot)
if (-not $installDirectory.StartsWith([IO.Path]::GetFullPath($env:ProgramData), [StringComparison]::OrdinalIgnoreCase)) {
    throw "InstallRoot must stay under ProgramData: $installDirectory"
}

$binaryPath = Join-Path $installDirectory 'engine-host-controller.exe'
$configPath = Join-Path $installDirectory 'config.yaml'
$tlsRoot = Join-Path $installDirectory 'tls'
$ownershipDirectory = Join-Path $env:ProgramData 'WeKnora\engine-ownership'
$ownerPath = Join-Path $ownershipDirectory 'owner.txt'
$temporaryBinary = Join-Path $env:TEMP ("weknora-engine-host-controller-{0}.exe" -f [Guid]::NewGuid().ToString('N'))
$configCreated = $false

try {
    Push-Location $repository
    try {
        & go build -buildvcs=false -o $temporaryBinary ./cmd/engine-host-controller
        if ($LASTEXITCODE -ne 0) { throw 'failed to build engine-host-controller.exe' }
    }
    finally {
        Pop-Location
    }

    $existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($existingService -and $existingService.Status -ne 'Stopped') {
        if ($PSCmdlet.ShouldProcess($ServiceName, 'Stop service for binary replacement')) {
            Stop-Service -Name $ServiceName -ErrorAction Stop
            (Get-Service -Name $ServiceName).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
        }
    }

    if ($PSCmdlet.ShouldProcess($installDirectory, 'Install engine host controller')) {
        New-Item -ItemType Directory -Path $installDirectory -Force | Out-Null
        Copy-Item -LiteralPath $temporaryBinary -Destination $binaryPath -Force
        if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
            Copy-Item -LiteralPath $exampleConfig -Destination $configPath
            $configCreated = $true
        }
        Copy-Item -LiteralPath $interlockSource -Destination (Join-Path $installDirectory 'weknora-engine-interlock.ps1') -Force
        Copy-Item -LiteralPath $handoffSource -Destination (Join-Path $installDirectory 'weknora-engine-handoff.ps1') -Force

        New-Item -ItemType Directory -Path $ownershipDirectory -Force | Out-Null
        . $interlockSource
        if (-not (Test-Path -LiteralPath $ownerPath -PathType Leaf)) {
            Set-WeKnoraEngineOwner -Path $ownerPath -Owner legacy
        }

        $requiredTLSFiles = @(
            (Join-Path $tlsRoot 'ca.crt'),
            (Join-Path $tlsRoot 'server.crt'),
            (Join-Path $tlsRoot 'server.key'),
            (Join-Path $tlsRoot 'gateway\client.crt'),
            (Join-Path $tlsRoot 'gateway\client.key'),
            (Join-Path $tlsRoot 'backend\client.crt'),
            (Join-Path $tlsRoot 'backend\client.key'),
            (Join-Path $tlsRoot 'bootstrap\client.crt'),
            (Join-Path $tlsRoot 'bootstrap\client.key')
        )
        $presentTLSFiles = @($requiredTLSFiles | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })
        if ($presentTLSFiles.Count -eq 0) {
            & $binaryPath -init-certs $tlsRoot
            if ($LASTEXITCODE -ne 0) { throw 'failed to initialize controller certificate bundle' }
        }
        elseif ($presentTLSFiles.Count -ne $requiredTLSFiles.Count) {
            throw 'controller TLS directory is incomplete; preserve it and repair the missing files explicitly'
        }

        # Language-independent SIDs: LocalSystem and builtin Administrators.
        & icacls.exe $installDirectory /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'failed to restrict controller directory ACLs' }
        & icacls.exe $ownershipDirectory /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' '*S-1-5-32-545:(OI)(CI)RX' | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'failed to restrict engine ownership directory ACLs' }
    }

    $binaryCommand = ('"{0}" -config "{1}"' -f $binaryPath, $configPath)
    if (-not $existingService) {
        if ($PSCmdlet.ShouldProcess($ServiceName, 'Register LocalSystem automatic service')) {
            New-Service `
                -Name $ServiceName `
                -BinaryPathName $binaryCommand `
                -DisplayName 'WeKnora Engine Host Controller' `
                -Description 'mTLS host authority for allowlisted WeKnora Docker engine lifecycle actions.' `
                -StartupType Automatic | Out-Null
        }
    }
    elseif ($PSCmdlet.ShouldProcess($ServiceName, 'Update service command')) {
        & sc.exe config $ServiceName binPath= $binaryCommand start= auto obj= LocalSystem | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'failed to update the controller Windows Service' }
    }
    if ($PSCmdlet.ShouldProcess($ServiceName, 'Configure service recovery actions')) {
        & sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/15000/none/0 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'failed to configure controller recovery actions' }
    }

    $firewallName = 'WeKnora Engine Host Controller mTLS'
    $firewallRule = Get-NetFirewallRule -DisplayName $firewallName -ErrorAction SilentlyContinue
    if (-not $firewallRule -and $PSCmdlet.ShouldProcess($firewallName, 'Create inbound mTLS firewall rule')) {
        New-NetFirewallRule `
            -DisplayName $firewallName `
            -Direction Inbound `
            -Action Allow `
            -Protocol TCP `
            -LocalPort 18443 `
            -RemoteAddress @('172.16.0.0/12', '192.168.0.0/16') `
            -Profile Any | Out-Null
    }
    elseif ($firewallRule -and $PSCmdlet.ShouldProcess($firewallName, 'Enable inbound mTLS firewall rule')) {
        Enable-NetFirewallRule -DisplayName $firewallName | Out-Null
    }

    if (-not $SkipStart -and $PSCmdlet.ShouldProcess($ServiceName, 'Start observe-only controller')) {
        Start-Service -Name $ServiceName
        (Get-Service -Name $ServiceName).WaitForStatus('Running', [TimeSpan]::FromSeconds(30))
    }

    [pscustomobject]@{
        Service = $ServiceName
        Binary = $binaryPath
        Config = $configPath
        TLSRoot = $tlsRoot
        Owner = (Get-Content -LiteralPath $ownerPath -Raw).Trim()
        OwnerPath = $ownerPath
        ConfigMode = if ($configCreated) { 'observe-only initialized' } else { 'preserved existing config' }
        LegacyGpuGuardChanged = $false
    }
}
finally {
    if (Test-Path -LiteralPath $temporaryBinary -PathType Leaf) {
        Remove-Item -LiteralPath $temporaryBinary -Force
    }
}
