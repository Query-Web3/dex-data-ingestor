#!/bin/bash

# 获取当前工作目录
CURRENT_WORKPLACE=$(pwd)

# # 获取脚本所在目录
# # CURRENT_DIR=$(readlink -f "$0")
# CURRENT_DIR=$(dirname "$0")
# echo "Current directory: $CURRENT_DIR"

# # 设置脚本执行位置
# cd "$CURRENT_DIR"s

# 激活虚拟环境（如果使用）
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Venv activated"
fi

# 设置环境变量（如果需要）
export PYTHONPATH=$PYTHONPATH:$CURRENT_WORKPLACE
echo "Environment variables set: PYTHONPATH=$PYTHONPATH"

# 检查并创建logs目录
LOGS_DIR="$CURRENT_WORKPLACE/logs"
if [ ! -d "$LOGS_DIR" ]; then
    mkdir -p "$LOGS_DIR"
    echo "Logs directory created: $LOGS_DIR"
fi

# 启动应用
# python3 src/main.py
# echo "Starting application..."

# 如果需要后台运行，可以使用以下命令
nohup python3 $CURRENT_WORKPLACE/src/main.py > $CURRENT_WORKPLACE/logs/app.log 2>&1 &
echo "ingestor application started in background"