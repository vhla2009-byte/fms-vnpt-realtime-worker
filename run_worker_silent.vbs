Set WshShell = CreateObject("WScript.Shell")
WshShell.Run Chr(34) & "d:\QLNT\fms_realtime_cloud\cloud_worker\run_worker_daemon.bat" & Chr(34), 0, False
Set WshShell = Nothing
