import time
import csv
import os
from datetime import datetime

CSV_FILE = "result_with_ip.csv"
LOG_FILE = "./logs/tron.log"
OUTPUT_LOG = "node_connection_history.csv"

def get_target_ips(csv_path):
    targets = set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ip = row.get('ipv4', '').strip()
            if ip and ip.lower() != "not found":
                targets.add(ip)
    return targets

def monitor_with_rolling(target_ips):
    last_inode = None
    f = None
    
    print(f"监控启动，目标数: {len(target_ips)}")

    while True:
        try:
            # 检查文件是否存在
            if not os.path.exists(LOG_FILE):
                time.sleep(1)
                continue

            # 检查 Inode 是否变化（判断是否发生了日志回滚）
            current_inode = os.stat(LOG_FILE).st_ino
            if current_inode != last_inode:
                if f: f.close()
                f = open(LOG_FILE, 'r')
                last_inode = current_inode
                # 如果是新打开的文件，可以选从头读或从末尾读
                # f.seek(0, 2) 
                print(f"检测到日志回滚/重新打开: {datetime.now()}")

            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue

            # 处理逻辑
            found_ip = next((ip for ip in target_ips if ip in line), None)
            if found_ip:
                event = ""
                if "Handshake finished" in line or "Channel activated" in line:
                    event = "CONNECTED"
                elif "Close channel" in line or "disconnected" in line:
                    event = "DISCONNECTED"
                
                if event:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    with open(OUTPUT_LOG, 'a', newline='') as csv_out:
                        writer = csv.writer(csv_out)
                        writer.writerow([now, found_ip, event])
                    print(f"[{now}] {event} | {found_ip}")

        except KeyboardInterrupt:
            if f: f.close()
            break
        except Exception as e:
            print(f"发生错误: {e}")
            time.sleep(2)

if __name__ == "__main__":
    ips = get_target_ips(CSV_FILE)
    monitor_with_rolling(ips)
