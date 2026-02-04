@echo off
REM Windows环境设置脚本 - 创建虚拟环境并安装依赖

echo ==========================================
echo Binance Quant Trading System
echo Windows Environment Setup
echo ==========================================
echo.

REM 检查Python
echo [1/4] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)
python --version
echo [OK] Python found
echo.

REM 创建虚拟环境
echo [2/4] Creating virtual environment...
if exist quant (
    echo WARNING: Virtual environment 'quant' already exists.
    set /p REMOVE="Remove and recreate? (y/N): "
    if /i "%REMOVE%"=="y" (
        echo Removing existing virtual environment...
        rmdir /s /q quant
    ) else (
        echo Keeping existing virtual environment.
        goto :install
    )
)

python -m venv quant
if errorlevel 1 (
    echo ERROR: Failed to create virtual environment
    pause
    exit /b 1
)
echo [OK] Virtual environment created: quant\
echo.

:install
REM 激活虚拟环境并升级pip
echo [3/4] Activating virtual environment and upgrading pip...
call quant\Scripts\activate.bat
python -m pip install --upgrade pip setuptools wheel
echo [OK] pip upgraded
echo.

REM 安装依赖
echo [4/4] Installing dependencies...
if not exist requirements.txt (
    echo ERROR: requirements.txt not found!
    pause
    exit /b 1
)

python -m pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)
echo [OK] Dependencies installed
echo.

REM 验证安装
echo Verifying installation...
python -c "import pandas, numpy, aiohttp, websockets, binance, pytest; print('[OK] All required packages installed')" 2>nul
if errorlevel 1 (
    echo WARNING: Some packages may not be installed correctly
)

echo.
echo ==========================================
echo Setup completed!
echo ==========================================
echo.
echo To activate the virtual environment, run:
echo   quant\Scripts\activate.bat
echo.
echo To run tests:
echo   python -m pytest tests\ -v
echo.
pause
