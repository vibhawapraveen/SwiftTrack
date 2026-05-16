@echo off
title SwiftLogistics - SwiftTrack Platform Launcher

echo.
echo  ============================================
echo     SwiftLogistics - SwiftTrack Platform
echo         Middleware Architecture Demo
echo  ============================================
echo.

:: Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running. Please start Docker Desktop first.
    echo.
    pause
    exit /b 1
)

echo [INFO] Docker is running.
echo.

:: Navigate to project directory
cd /d "%~dp0"

echo [STEP 1/4] Stopping any existing containers...
docker-compose down >nul 2>&1
echo           Done.
echo.

echo [STEP 2/4] Building all services...
echo           This may take a few minutes on first run...
echo.
docker-compose build --parallel
if errorlevel 1 (
    echo [ERROR] Build failed. Please check the error messages above.
    pause
    exit /b 1
)
echo.
echo           Build completed successfully.
echo.

echo [STEP 3/4] Starting all services...
docker-compose up -d
if errorlevel 1 (
    echo [ERROR] Failed to start services. Please check the error messages above.
    pause
    exit /b 1
)
echo.
echo           All services started.
echo.

echo [STEP 4/4] Waiting for services to be ready...
echo           (This may take up to 60 seconds)
echo.

:: Wait for services to be healthy
set /a counter=0
:wait_loop
set /a counter+=1
if %counter% gtr 60 (
    echo.
    echo [WARNING] Timeout waiting for services. They may still be starting.
    goto :continue
)

:: Check if API Gateway is responding
curl -s http://localhost:3000/health >nul 2>&1
if errorlevel 1 (
    echo           Waiting... (%counter%s)
    timeout /t 1 /nobreak >nul
    goto :wait_loop
)

:continue
echo.
echo  ============================================
echo              SERVICES STARTED
echo  ============================================
echo.
echo  The following services are now running:
echo.
echo  [INFRASTRUCTURE]
echo    - RabbitMQ Management:  http://localhost:15672
echo      (User: swift, Pass: logistics123)
echo    - PostgreSQL Database:  localhost:5432
echo.
echo  [BACKEND SERVICES]
echo    - API Gateway:          http://localhost:3000
echo    - CMS Service (SOAP):   http://localhost:8001
echo    - ROS Service (REST):   http://localhost:8002
echo    - WMS Service (TCP):    http://localhost:8003
echo.
echo  [FRONTEND PORTALS]
echo    - Client Portal:        http://localhost:8080
echo    - Driver App:           http://localhost:8081
echo.
echo  ============================================
echo.

:: Wait a moment before opening browsers
timeout /t 3 /nobreak >nul

echo [INFO] Opening portals in your default browser...
echo.

:: Open Client Portal
start "" http://localhost:8080

:: Brief delay between opening tabs
timeout /t 2 /nobreak >nul

:: Open Driver App
start "" http://localhost:8081

:: Brief delay
timeout /t 2 /nobreak >nul

:: Open RabbitMQ Management
start "" http://localhost:15672

echo.
echo  ============================================
echo          DEMO CREDENTIALS
echo  ============================================
echo.
echo  CLIENT PORTAL:
echo    Email: techmart@example.com
echo    Password: password123
echo.
echo  DRIVER APP:
echo    Email: kasun@swiftlogistics.lk
echo    Password: password123
echo.
echo  RABBITMQ:
echo    Username: swift
echo    Password: logistics123
echo.
echo  ============================================
echo.
echo  Press any key to view logs (Ctrl+C to exit)...
pause >nul

:: Show logs
echo.
echo [INFO] Showing service logs (Ctrl+C to exit)...
echo.
docker-compose logs -f
