Option Explicit

Dim shell, fileSystem, scriptDirectory, watchdogPath, pwshPath, command, exitCode
Set shell = CreateObject("WScript.Shell")
Set fileSystem = CreateObject("Scripting.FileSystemObject")

scriptDirectory = fileSystem.GetParentFolderName(WScript.ScriptFullName)
watchdogPath = fileSystem.BuildPath(scriptDirectory, "weknora-ollama-watchdog.ps1")
pwshPath = shell.ExpandEnvironmentStrings("%ProgramFiles%") & "\PowerShell\7\pwsh.exe"

If Not fileSystem.FileExists(watchdogPath) Then WScript.Quit 2
If Not fileSystem.FileExists(pwshPath) Then WScript.Quit 3

command = Quote(pwshPath) & _
    " -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File " & _
    Quote(watchdogPath)
If WScript.Arguments.Named.Exists("Once") Then command = command & " -Once"

exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode

Function Quote(value)
    Quote = Chr(34) & Replace(value, Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function
