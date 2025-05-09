#!/bin/bash

# 获取脚本所在目录
CURRENT_DIR=$(dirname "$0")
echo "Script directory: $CURRENT_DIR"

# 获取当前工作目录
CURRENT_WORKPLACE=$(pwd)

# 获取进程ID，使用与start.sh中相同的命令匹配
PID=$(ps aux | grep "python3 $CURRENT_WORKPLACE/src/main.py" | grep -v grep | awk '{print $2}')
if [ -z "$PID" ]; then
    echo "No running process found"
    exit 0
fi

# 停止进程
echo "Stopping process with PID: $PID"
kill -9 $PID

# 等待进程完全停止
sleep 2

# 确认进程是否已停止
if ps -p $PID > /dev/null; then
    echo "Failed to stop process"
    exit 1
else
    echo "Process stopped successfully"
    exit 0
fi