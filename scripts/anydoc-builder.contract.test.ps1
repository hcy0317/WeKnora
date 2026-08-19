[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$localBuilderPath = Join-Path $repositoryRoot 'docker/Dockerfile.app-builder-local'

Assert-True (Test-Path -LiteralPath $localBuilderPath -PathType Leaf) 'local app builder Dockerfile is missing'

$source = Get-Content -LiteralPath $localBuilderPath -Raw

Assert-True ($source -match '(?m)^ARG WITH_ANYDOC=1\s*$') 'local app builder must enable AnyDoc by default'
Assert-True ($source -match '(?m)^ENV PATH=/usr/local/cargo/bin:/usr/local/go/bin:\$\{PATH\}\s*$') 'local app builder does not expose the Rust toolchain on PATH'
Assert-True ($source -match 'https://sh\.rustup\.rs') 'local app builder does not install the Rust toolchain'
Assert-True ($source -match '\./scripts/build-anydoc-lib\.sh') 'local app builder does not build the AnyDoc static archive'
Assert-True ($source -match 'make build-prod GO_BUILD_TAGS=anydoc') 'local app builder does not link the AnyDoc build tag'
Assert-True ($source -match '(?s)if \[ "\$WITH_ANYDOC" = "1" \].+else\s*\\?\s*make build-prod') 'local app builder does not preserve an explicit AnyDoc opt-out'

Write-Host 'AnyDoc local builder contract passed.'
