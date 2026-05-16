@echo off
title SwiftLogistics - Stop Services

echo.
echo  ============================================
echo     SwiftLogistics - Stopping Services
echo  ============================================
echo.

cd /d "%~dp0"

echo [INFO] Stopping all services...
docker-compose down

echo.
echo [INFO] All services stopped.
echo.

pause
