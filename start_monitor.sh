#!/bin/bash

# 1. 定义时间戳 (格式: 20260327_0720)
LOG_TIME=$(date +"%Y%m%d_%H%M")

# 2. 定义完整的日志路径
LOG_FILE="/home/yangping/my_log/raw_inv_${LOG_TIME}.log"

# 3. 启动节点并过滤日志
# --line-buffered 确保 grep 实时写入文件，不产生缓存延迟
echo "Starting TRON node... Logging to $LOG_FILE"

nohup java -Xmx16g -XX:+UseG1GC -jar framework/build/libs/FullNode.jar \
  -c config.conf 2>&1 | \
  grep --line-buffered "\[RAW_INV\]" > "$LOG_FILE" &

echo "Node started in background with PID $!"
