@echo off
cd /d "d:\QLNT\fms_realtime_cloud\cloud_worker"
title FMS_VNPT_Cloud_Worker_247

:loop
echo [%date% %time%] Khoi dong FMS Cloud Worker... >> worker_service.log
"C:\Users\ngotu\node\node-v22.14.0-win-x64\node.exe" index.js >> worker_service.log 2>&1
echo [%date% %time%] Tien trinh dung dot ngot! Tu dong khoi dong lai sau 5 giay... >> worker_service.log
timeout /t 5 /nobreak >nul
goto loop
