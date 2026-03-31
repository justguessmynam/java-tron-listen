import pandas as pd
import numpy as np
import json
import os
import time
import sys
import shutil
import glob
import gc
from datetime import datetime, timedelta
import multiprocessing
from multiprocessing import Pool
import matplotlib.pyplot as plt

# ================= 核心配置 (绝对路径) =================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CSV_FILE = os.path.join(BASE_DIR, "tx100.csv")
LOG_FILE = os.path.join(BASE_DIR, "tx100.log")
TEMP_CACHE_DIR = os.path.join(BASE_DIR, "temp_chunks")
FINAL_CACHE = os.path.join(BASE_DIR, "tx_full_14g_cache.parquet")
OUTPUT_DIR = os.path.join(BASE_DIR, "analysis_results_full")

MAX_PROCESSES = 3  # 16G 内存建议保留核心给系统
LOG_CHUNK_SIZE = 128 * 1024 * 1024  # 128MB
# =====================================================

def parse_log_chunk_to_disk(args):
    """子进程：解析日志 + 跨天自动校正时间"""
    file_path, start, size, chunk_id, target_dir, base_date_str = args
    results = []
    current_date = datetime.strptime(base_date_str, "%Y-%m-%d")
    last_hour = -1
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(start)
            if start != 0: f.readline() 
            chunk_content = f.read(size)
            lines = chunk_content.splitlines()
            
            for line in lines:
                if "[RAW_INV] " not in line: continue
                try:
                    time_part = line[:12] # HH:MM:SS.mmm
                    hour = int(time_part[:2])
                    
                    # 跨天校正 (23:59 -> 00:01)
                    if last_hour != -1 and hour < last_hour:
                        current_date += timedelta(days=1)
                    last_hour = hour
                    
                    json_part = line.split("[RAW_INV] ")[1]
                    data = json.loads(json_part)
                    
                    log_time_obj = datetime.strptime(time_part, "%H:%M:%S.%f").time()
                    log_dt = datetime.combine(current_date.date(), log_time_obj)
                    
                    for tx_hash in data.get('ids', []):
                        results.append((tx_hash, log_dt, data.get('ip'), data.get('sz'), data.get('lat')))
                except: continue
        
        if results:
            df = pd.DataFrame(results, columns=['hash', 'log_dt', 'ip', 'sz', 'lat'])
            df.to_parquet(os.path.join(target_dir, f"chunk_{chunk_id}.parquet"), engine='pyarrow')
            return len(results)
        return 0
    except Exception: return -1

def main():
    start_total = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(TEMP_CACHE_DIR): os.makedirs(TEMP_CACHE_DIR)

    # --- [0/4] 初始化基准日期 ---
    print(f"⏳ [0/4] 正在解析 CSV 初始化基准日期...")
    df_csv = pd.read_csv(CSV_FILE, names=['exec_time', 'nid', 'ip', 'hash'], header=0)
    df_csv['dt'] = pd.to_datetime(df_csv['exec_time'], errors='coerce')
    df_csv = df_csv.dropna(subset=['dt', 'hash'])
    base_date_str = df_csv['dt'].iloc[0].strftime("%Y-%m-%d")

    # --- [1/4] 并行解析 ---
    if not os.path.exists(FINAL_CACHE):
        file_size = os.path.getsize(LOG_FILE)
        offsets = range(0, file_size, LOG_CHUNK_SIZE)
        all_tasks = [(LOG_FILE, o, LOG_CHUNK_SIZE, i, TEMP_CACHE_DIR, base_date_str) for i, o in enumerate(offsets)]
        tasks_to_run = [t for t in all_tasks if not os.path.exists(os.path.join(TEMP_CACHE_DIR, f"chunk_{t[3]}.parquet"))]
        
        print(f"🚀 [1/4] 解析 14G 日志 | 总块数: {len(all_tasks)} | 待处理: {len(tasks_to_run)}")
        if tasks_to_run:
            p_start = time.time()
            with Pool(processes=MAX_PROCESSES) as pool:
                for i, _ in enumerate(pool.imap_unordered(parse_log_chunk_to_disk, tasks_to_run), 1):
                    elapsed = time.time() - p_start
                    speed = (i * LOG_CHUNK_SIZE / 1024 / 1024) / elapsed if elapsed > 0 else 0
                    sys.stdout.write(f"\r   进度: {i/len(tasks_to_run)*100:6.2f}% | 速率: {speed:6.2f} MB/s")
                    sys.stdout.flush()
            print(f"\n   ✅ 解析阶段完成。")

        # --- [2/4] 合并分块 ---
        print(f"📦 [2/4] 合并数据并持久化缓存...")
        all_files = sorted(glob.glob(os.path.join(TEMP_CACHE_DIR, "*.parquet")))
        df_log = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
        df_log.to_parquet(FINAL_CACHE, compression='snappy')
        shutil.rmtree(TEMP_CACHE_DIR)
        gc.collect()
    else:
        print(f"✅ [1&2] 加载现有缓存 {FINAL_CACHE}")
        df_log = pd.read_parquet(FINAL_CACHE)

    # --- [3/4] 索引优化 ---
    print(f"⚡ [3/4] 建立 Hash 索引...")
    df_log = df_log.set_index('hash').sort_index()
    gc.collect()

    # --- [4/4] 关联分析 ---
    print(f"🔗 [4/4] 执行关联匹配 (CSV 行数: {len(df_csv)})...")
    analysis_results, source_hits = [], []
    c_start = time.time()
    
    for idx, (i, row) in enumerate(df_csv.iterrows(), 1):
        try:
            matches = df_log.loc[[row['hash']]]
            # 1. 始发节点匹配 (用于 delta_t)
            s_match = matches[matches['ip'] == row['ip']]
            for _, s_row in s_match.iterrows():
                source_hits.append({
                    'hash': row['hash'], 'ip': row['ip'], 
                    'delta_t': (s_row['log_dt'] - row['dt']).total_seconds(),
                    'log_lat': s_row['lat']
                })
            # 2. 误报率计算 (用于 FP 分析)
            forwarding = matches[matches['ip'] != row['ip']]
            if not forwarding.empty:
                f_i = len(forwarding[forwarding['sz'] == 1]) / len(forwarding)
                analysis_results.append({'hash': row['hash'], 'time': row['dt'], 'f_i': f_i})
        except KeyError: continue
        
        if idx % 100 == 0 or idx == len(df_csv):
            sys.stdout.write(f"\r   🎯 匹配进度: {idx}/{len(df_csv)} | 速度: {idx/(time.time()-c_start):6.1f} tx/s")
            sys.stdout.flush()

    # --- 保存数据表 ---
    print(f"\n\n💾 正在导出 CSV 数据表至 {OUTPUT_DIR}...")
    df_hits = pd.DataFrame(source_hits)
    df_hits.to_csv(os.path.join(OUTPUT_DIR, "source_node_hits.csv"), index=False)
    
    df_final = pd.DataFrame(analysis_results).dropna(subset=['f_i'])
    df_final.to_csv(os.path.join(OUTPUT_DIR, "analysis_results.csv"), index=False)

    # --- 可视化生成 ---
    print(f"📊 正在生成分析图表...")

    # 图 1: FP Distribution (3% 步长分桶统计 CSV 交易数量)
    max_f = df_final['f_i'].max() if not df_final.empty else 0.1
    step = 0.03
    bin_edges = np.arange(0, max_f + step, step)
    counts, _ = np.histogram(df_final['f_i'], bins=bin_edges)
    x_labels = [f"{int(bin_edges[i]*100)}-{int(bin_edges[i+1]*100)}%" for i in range(len(bin_edges)-1)]

    fig, ax1 = plt.subplots(figsize=(12, 7))
    bars = ax1.bar(x_labels, counts, color='skyblue', edgecolor='black', alpha=0.7)
    ax1.set_ylabel('Transaction Count (CSV Samples)', fontsize=12)
    ax1.set_xlabel('False Positive Rate Interval (%)', fontsize=12)
    
    # 柱状图标注具体交易笔数
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5, f'{int(height)}', 
                     ha='center', va='bottom', fontsize=10, fontweight='bold')

    # CDF 曲线
    ax2 = ax1.twinx()
    cdf = np.cumsum(counts) / len(df_final) if len(df_final) > 0 else []
    ax2.plot(x_labels, cdf, 'r-o', linewidth=2, markersize=8, label='CDF')
    ax2.set_ylabel('Cumulative Probability (CDF)', color='red', fontsize=12)
    ax2.set_ylim(0, 1.1)
    for i, val in enumerate(cdf):
        ax2.annotate(f'{val*100:.1f}%', xy=(i, val), xytext=(0, 10), 
                     textcoords='offset points', ha='center', color='darkred', fontsize=9)

    plt.title("FP Rate Distribution (3% Intervals) & CDF", pad=20, fontsize=14)
    plt.xticks(rotation=30)
    fig.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "1_fp_distribution_3pct.png"), dpi=300)

    # ====== 图 2: 完整版（三曲线：first-seen TPS + FP + Avg Spread） ======
    print(f" 正在生成图2（first-seen TPS + FP + Avg Spread）...")

    # 1. 监测时间区间
    t_min = df_final['time'].min()
    t_max = df_final['time'].max()
    print(f"   📅 监测区间确认: {t_min} 至 {t_max}")

    # 2. 恢复 hash 列（之前 set_index 过）
    df_log_reset = df_log.reset_index()

    # 3. 过滤时间窗口
    df_log_filtered = df_log_reset[
        (df_log_reset['log_dt'] >= t_min) &
        (df_log_reset['log_dt'] <= t_max)
    ].copy()

    # 4. First-seen 交易
    df_first = (
        df_log_filtered
        .sort_values('log_dt')
        .drop_duplicates(subset='hash', keep='first')
        .copy()
    )

    # 5. First-seen TPS（30分钟分桶）
    df_first['time_bin'] = df_first['log_dt'].dt.floor('30min')
    df_load = df_first.groupby('time_bin').size().rename('first_seen_count')
    df_load = df_load.to_frame()
    df_load['tps'] = df_load['first_seen_count'] / 1800  # TPS = count / 30*60s

    # 6. 采样 FP
    df_final['time_bin'] = df_final['time'].dt.floor('30min')
    df_sample = df_final.groupby('time_bin')['f_i'].mean().rename('avg_f_i')

    # 7. 平均传播强度计算
    # 每笔交易的传播次数
    df_spread = df_log_filtered.groupby('hash').size().rename('spread_count')
    df_first_spread = df_first[['hash', 'log_dt']].merge(
        df_spread, on='hash', how='left'
    )
    df_first_spread['time_bin'] = df_first_spread['log_dt'].dt.floor('30min')
    df_spread_ts = df_first_spread.groupby('time_bin')['spread_count'].mean().rename('avg_spread')

    # 8. 合并时间序列
    df_ts = pd.concat([df_load[['tps']], df_sample, df_spread_ts], axis=1)
    df_ts = df_ts.dropna(subset=['tps'])
    df_ts = df_ts.sort_index()

    # 9. 绘图
    fig2, ax3 = plt.subplots(figsize=(14, 7))
    indices = np.arange(len(df_ts))
    x_labels = df_ts.index.strftime('%m-%d %H:%M')

    # 9a. TPS
    line1 = ax3.plot(indices, df_ts['tps'], 'g-o', markersize=6, linewidth=1.5, label='First-seen TPS')
    ax3.set_ylabel('Transaction Arrival Rate (TPS)', color='green', fontweight='bold', fontsize=12)
    ax3.tick_params(axis='y', labelcolor='green')

    # 9b. FP
    ax4 = ax3.twinx()
    line2 = ax4.plot(indices, df_ts['avg_f_i'], color='orange', marker='s', markersize=6, linewidth=1.5, label='Avg FP Rate')
    ax4.set_ylabel('Average False Positive Rate (f_i)', color='orange', fontweight='bold', fontsize=12)
    ax4.tick_params(axis='y', labelcolor='orange')

    # 9c. 传播强度
    ax5 = ax3.twinx()
    ax5.spines["right"].set_position(("outward", 60))
    line3 = ax5.plot(indices, df_ts['avg_spread'], color='blue', marker='^', markersize=5, linewidth=1.5, label='Avg Propagation Count')
    ax5.set_ylabel('Propagation Intensity', color='blue', fontsize=12)
    ax5.tick_params(axis='y', labelcolor='blue')

    # 10. X轴
    ax3.set_xticks(indices)
    ax3.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)

    # 11. 图例
    lns = line1 + line2 + line3
    labs = [str(l.get_label()) for l in lns]
    ax3.legend(lns, labs, loc='upper left')

    # 12. 标题 & 网格
    plt.title(f"Network Analysis: First-seen TPS vs FP Rate vs Propagation\nPeriod: {t_min} to {t_max}", fontsize=14, pad=15)
    ax3.grid(True, alpha=0.3, axis='y', linestyle='--')

    fig2.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "2_load_analysis_full.png"), dpi=300)

    print(f"✅ 图2已保存（first-seen TPS + FP + Avg Spread）。起点：{x_labels[0]}，终点：{x_labels[-1]}")
    
    print(f"\n✨ 全部流程成功完成！")
    print(f"⏰ 总耗时: {timedelta(seconds=int(time.time()-start_total))}")
    print(f"📁 结果存至: {OUTPUT_DIR}/")

if __name__ == "__main__":
    if multiprocessing.get_start_method() != 'spawn':
        try: multiprocessing.set_start_method('spawn', force=True)
        except: pass
    main()