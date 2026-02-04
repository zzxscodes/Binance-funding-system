#!/bin/bash
# Linux环境设置脚本 - 创建虚拟环境并安装依赖

set -e

echo "=========================================="
echo "Binance Quant Trading System"
echo "Linux Environment Setup"
echo "=========================================="
echo ""

# 检查Python
echo "[1/5] Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed"
    exit 1
fi
python3 --version
echo "[OK] Python found"
echo ""

# 检查python3-venv
echo "[2/5] Checking python3-venv..."
if ! python3 -m venv --help &> /dev/null; then
    echo "ERROR: python3-venv is not installed"
    echo "Please install it using:"
    echo "  sudo apt update && sudo apt install -y python3-venv"
    exit 1
fi
echo "[OK] python3-venv available"
echo ""

# 创建虚拟环境
echo "[3/5] Creating virtual environment..."
if [ -d "quant" ]; then
    echo "WARNING: Virtual environment 'quant' already exists."
    read -p "Remove and recreate? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf quant
    else
        echo "Keeping existing virtual environment."
        SKIP_VENV=1
    fi
fi

if [ -z "$SKIP_VENV" ]; then
    python3 -m venv quant
    echo "[OK] Virtual environment created: quant/"
fi
echo ""

# 激活虚拟环境并升级pip
echo "[4/5] Activating virtual environment and upgrading pip..."
source quant/bin/activate
pip install --upgrade pip setuptools wheel
echo "[OK] pip upgraded"
echo ""

# 安装依赖
echo "[5/5] Installing dependencies..."
if [ ! -f "requirements.txt" ]; then
    echo "ERROR: requirements.txt not found!"
    exit 1
fi

pip install -r requirements.txt
echo "[OK] Dependencies installed"
echo ""

# 验证安装
echo "Verifying installation..."
python3 -c "import pandas, numpy, aiohttp, websockets, binance, pytest; print('[OK] All required packages installed')" 2>/dev/null || echo "WARNING: Some packages may not be installed correctly"

echo ""
echo "=========================================="
echo "Setup completed!"
echo "=========================================="
echo ""
echo "To activate the virtual environment, run:"
echo "  source quant/bin/activate"
echo ""
echo "To run tests:"
echo "  python3 -m pytest tests/ -v"
echo ""
