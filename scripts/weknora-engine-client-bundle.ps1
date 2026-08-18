Set-StrictMode -Version Latest

function Export-WeKnoraEngineClientBundle {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$SourceTLSRoot,

        [Parameter(Mandatory)]
        [string]$DestinationRoot,

        [Parameter(Mandatory)]
        [ValidatePattern('^S-\d+(?:-\d+)+$')]
        [string]$ContainerRuntimeUserSid
    )

    $sourceRoot = [IO.Path]::GetFullPath($SourceTLSRoot)
    $destinationDirectory = [IO.Path]::GetFullPath($DestinationRoot)
    if ($sourceRoot.Equals($destinationDirectory, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'client TLS export must use a directory separate from the protected controller TLS root'
    }

    $clientFiles = @(
        'ca.crt',
        'gateway\client.crt',
        'gateway\client.key',
        'backend\client.crt',
        'backend\client.key'
    )
    foreach ($relativePath in $clientFiles) {
        $sourcePath = Join-Path $sourceRoot $relativePath
        if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
            throw "controller client TLS source is incomplete: $relativePath"
        }
    }

    New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
    $unexpectedFiles = @(Get-ChildItem -LiteralPath $destinationDirectory -File -Recurse | Where-Object {
            $relativePath = [IO.Path]::GetRelativePath($destinationDirectory, $_.FullName)
            $clientFiles -notcontains $relativePath
        })
    if ($unexpectedFiles.Count -gt 0) {
        throw 'client TLS export contains unexpected files; preserve it and repair the directory explicitly'
    }

    foreach ($relativePath in $clientFiles) {
        $sourcePath = Join-Path $sourceRoot $relativePath
        $destinationPath = Join-Path $destinationDirectory $relativePath
        New-Item -ItemType Directory -Path (Split-Path -Parent $destinationPath) -Force | Out-Null
        $temporaryPath = "$destinationPath.tmp-$([Guid]::NewGuid().ToString('N'))"
        try {
            Copy-Item -LiteralPath $sourcePath -Destination $temporaryPath
            Move-Item -LiteralPath $temporaryPath -Destination $destinationPath -Force
        }
        finally {
            if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
                Remove-Item -LiteralPath $temporaryPath -Force
            }
        }
    }

    # Replace the DACL instead of merely appending grants: an existing broad
    # allow rule must not survive on a directory that contains client keys.
    $clientRootAcl = [Security.AccessControl.DirectorySecurity]::new()
    $clientRootAcl.SetAccessRuleProtection($true, $false)
    $inheritanceFlags = [Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [Security.AccessControl.InheritanceFlags]::ObjectInherit
    $propagationFlags = [Security.AccessControl.PropagationFlags]::None
    $allow = [Security.AccessControl.AccessControlType]::Allow
    foreach ($ruleSpec in @(
            @('S-1-5-18', [Security.AccessControl.FileSystemRights]::FullControl),
            @('S-1-5-32-544', [Security.AccessControl.FileSystemRights]::FullControl),
            @($ContainerRuntimeUserSid, [Security.AccessControl.FileSystemRights]::ReadAndExecute)
        )) {
        $principal = [Security.Principal.SecurityIdentifier]::new($ruleSpec[0])
        $rule = [Security.AccessControl.FileSystemAccessRule]::new(
            $principal,
            $ruleSpec[1],
            $inheritanceFlags,
            $propagationFlags,
            $allow
        )
        [void]$clientRootAcl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $destinationDirectory -AclObject $clientRootAcl

    Get-Item -LiteralPath $destinationDirectory
}
