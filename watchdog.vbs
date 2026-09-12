' watchdog.vbs — restarts server.js if it ever exits/crashes.
' Runs hidden (no console window). Resolves its own folder, so it works
' regardless of where this repo is checked out.
'
' To auto-start at login: press Win+R, type shell:startup, press Enter,
' then copy this file into the folder that opens.
' To start it right now without logging out: just double-click this file.

Set fso = CreateObject("Scripting.FileSystemObject")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
Set WshShell = CreateObject("WScript.Shell")
Do While True
    WshShell.Run "node """ & scriptDir & "\server.js""", 0, True
    WScript.Sleep 3000
Loop
