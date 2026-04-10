import requests
import time
import os
import csv
from datetime import datetime

# --- 配置区 ---
CSV_FILE = "result_with_ip.csv"
REFRESH_RATE = 2  # 提高刷新频率，因为只看活跃节点，性能开销更小

# 你的 4 个监控节点
NODES = {
    "Node_1": "http://127.0.0.1:8090",
    "Node_2": "http://IP_2:8090",
    "Node_3": "http://IP_3:8090",
    "Node_4": "http://IP_4:8090"
}

GREEN = '\033[32m'
YELLOW = '\033[33m'
CYAN = '\033[36m'
RESET = '\033[0m'

def get_target_ips(csv_path):
    targets = set()
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ip = row.get('ipv4', '').strip()
                if ip and ip.lower() != "not found":
                    targets.add(ip)
        return targets
    except:
        return set()

def fetch_peers(url):
    try:
        # wallet/getnodeinfo 是波场官方查询 P2P 状态的标准接口
        r = requests.post(f"{url}/wallet/getnodeinfo", timeout=1.5).json()
        # 仅保留握手成功的或正在连接的
        return {p.get("host").strip("/"): p for p in r.get("peerList", [])}
    except:
        return None

def run_monitor():
    targets = get_target_ips(CSV_FILE)
    
    while True:
        try:
            # 1. 批量抓取快照
            snapshots = {name: fetch_peers(url) for name, url in NODES.items()}
            
            # 2. 计算“活跃并属于目标列表”的并集（过滤掉不关心的杂乱节点）
            active_in_view = set()
            for snap in snapshots.values():
                if snap:
                    # 只有在 result_with_ip.csv 里的 IP 才显示
                    active_in_view.update([ip for ip in snap.keys() if ip in targets])

            # 3. 渲染界面
            os.system('clear' if os.name == 'posix' else 'cls')
            print(f"{CYAN}TRON 活跃链路动态监控 (筛选模式){RESET} | {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 135)
            
            # 动态表头
            header = f"{'Target IP (Active)':<18} | " + " | ".join([f"{n:<25}" for n in NODES.keys()])
            print(header)
            print("-" * 135)

            # 4. 只按“当前有连接”的 IP 循环
            for tip in sorted(list(active_in_view)):
                row_cells = []
                for name in NODES.keys():
                    snap = snapshots[name]
                    if snap and tip in snap:
                        p = snap[tip]
                        # 只区分 Active(握手完) 和 Inactive(尝试中)
                        is_act = p.get("active", False)
                        color = GREEN if is_act else YELLOW
                        label = "★A" if is_act else "○P"
                        
                        # 简化显示：模式 + 延迟 + 高度缩写
                        status = f"{color}{label} {p.get('avgLatency',0):>3}ms|{p.get('headBlockNum',0)//1000:>5}k{RESET}"
                        # 补齐宽度：ANSI 长度修正
                        row_cells.append(f"{status}{' ' * (25 - 13)}")
                    else:
                        # 没连接的列直接留空，保持视觉清爽
                        row_cells.append(f"{' ':<25}")
                
                print(f"{tip:<18} | " + " | ".join(row_cells))

            if not active_in_view:
                print(f"\n{'[ 当前无活跃目标连接 ]':^135}")

            print("=" * 135)
            print(f"说明: {GREEN}★A=活跃连接{RESET}, {YELLOW}○P=被动/握手中{RESET}, 仅显示在 CSV 列表中的实时连接")
            
            time.sleep(REFRESH_RATE)

        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    run_monitor()
