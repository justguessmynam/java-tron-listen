#!/bin/bash

# ==============================================================================
# 脚本名称: deploy_tron_root.sh
# 功能描述: 纯 root 环境下的一键自动化部署波场（Tron）监听节点及 Python 环境
# ==============================================================================

# 确保脚本如果遇到错误立即停止执行
set -e

echo "========================================="
echo "开始部署波场节点环境 (Root 专用版)..."
echo "========================================="

# 1. 严格检查 root 权限
if [ "$EUID" -ne 0 ]; then
  echo "错误: 此脚本必须以 root 用户身份运行！"
  exit 1
fi

# 2. 安装系统依赖与环境
# [apt] Advanced Package Tool (高级包管理工具)
# [-y] Yes (自动确认所有提示)
echo "正在更新系统源并安装依赖 (JDK8, Python3-venv, git, aria2)..."
apt update
apt install openjdk-8-jdk git python3-venv aria2 -y
echo "系统依赖安装完成。"
echo "-----------------------------------------"

# 3. 配置 UFW 防火墙
# [ufw] Uncomplicated Firewall (简单防火墙)
# [tcp/udp] Transmission Control Protocol / User Datagram Protocol (传输控制/用户数据报协议)
echo "正在配置防火墙端口 (18888, 8090)..."
ufw allow 18888/tcp
ufw allow 18888/udp
ufw allow 8090/tcp
ufw allow 8090/udp
ufw reload
echo "防火墙配置完成。"
echo "-----------------------------------------"

# 4. 克隆代码仓库并切换分支
echo "正在前往 /root 目录..."
cd /root

echo "正在克隆 java-tron-listen 仓库..."
if [ ! -d "java-tron-listen" ]; then
    git clone https://github.com/justguessmynam/java-tron-listen
fi
cd java-tron-listen
git checkout listener-task

cd /root
echo "正在克隆 tron_pyscripts 仓库..."
if [ ! -d "tron_pyscripts" ]; then
    git clone https://github.com/justguessmynam/tron_pyscripts
fi
echo "代码仓库准备就绪。"
echo "-----------------------------------------"

# 5. 配置 Python 虚拟环境及依赖
# [venv] Virtual Environment (Python 虚拟环境)
# [pip] Pip Installs Packages (Python 包管理器)
echo "正在配置 Python 虚拟环境并安装 requests..."
cd /root/tron_pyscripts

python3 -m venv listenenv
./listenenv/bin/pip install --upgrade pip
./listenenv/bin/pip install requests
echo "Python 虚拟环境配置完成。"
echo "-----------------------------------------"

# 6. 后台启动 Aria2 快照下载 (下载至 /root/java-tron-listen 目录)
# 使用 nohup 将下载任务丢入后台，避免阻塞脚本
echo "正在进入 /root/java-tron-listen 目录并后台启动快照文件下载..."
cd /root/java-tron-listen

# [nohup] No Hang Up (不挂断地运行命令，忽略挂起信号)
# [-s 16 -x 16] 16个连接，单个服务器最大16线程
# [-c] Continue (断点续传)
# [-o] Output (指定输出文件名)
nohup aria2c -s 16 -x 16 -c -o snapshot.tgz "http://34.86.86.229/backup20260715/LiteFullNode_output-directory.tgz" > download.log 2>&1 &

echo "========================================="
echo "🎉 一键部署脚本执行完毕！"
echo "========================================="
echo "💡 快照下载已在后台安全运行。"
echo "📂 快照文件位置: /root/java-tron-listen/snapshot.tgz"
echo "📂 下载日志和进度记录在: /root/java-tron-listen/download.log"
echo "🔍 您可以运行以下命令来【实时查看】下载进度："
echo "   tail -f /root/java-tron-listen/download.log"
echo "========================================="
