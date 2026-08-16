function Invoke-GuardProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory)][ValidateRange(1, 3600)][int]$TimeoutSeconds
    )

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    if ($startInfo.PSObject.Properties.Name -contains 'ArgumentList') {
        foreach ($argument in $ArgumentList) {
            [void]$startInfo.ArgumentList.Add($argument)
        }
    }
    else {
        $startInfo.Arguments = ($ArgumentList | ForEach-Object {
            if ($_ -notmatch '[\s"]') { $_ }
            else { '"' + ($_ -replace '"', '\"') + '"' }
        }) -join ' '
    }

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw "failed to start process '$FilePath'" }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            $taskkill = Join-Path $env:SystemRoot 'System32/taskkill.exe'
            & $taskkill /PID $process.Id /T /F 2>&1 | Out-Null
            [void]$process.WaitForExit(2000)
            throw "process '$FilePath' timed out after $TimeoutSeconds second"
        }
        [pscustomobject]@{
            ExitCode = $process.ExitCode
            Output = @($stdout.Result -split "`r?`n" | Where-Object { $_ -ne '' })
            ErrorOutput = @($stderr.Result -split "`r?`n" | Where-Object { $_ -ne '' })
        }
    }
    finally {
        $process.Dispose()
    }
}
