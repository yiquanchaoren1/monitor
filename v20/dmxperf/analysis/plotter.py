# -*- coding: utf-8 -*-
import os
import glob
import re
import pandas as pd
import matplotlib
matplotlib.use('Agg') # 后台绘图
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
from collections import defaultdict

class Plotter:
    def __init__(self, run_root):
        self.run_root = run_root
        self.metrics_root = os.path.join(run_root, "metrics")

    def plot_job(self, job_name, visualize_keys):
        """
        job_name: 任务名
        visualize_keys: 需要绘制的指标列表，例如 ["cpu", "memory", "gpu_util", "power"]
        """
        if not visualize_keys:
            return

        #print(f"      [Plotter] 正在为 {job_name} 生成聚合对比图...")

        job_dir = os.path.join(self.metrics_root, job_name)
        timeseries_dir = os.path.join(job_dir, "TimeSeries")
        plots_dir = os.path.join(job_dir, "Plots")
        os.makedirs(plots_dir, exist_ok=True)

        # 1. 扫描所有 CSV 并按节点分组
        node_files_map = defaultdict(list)
        csv_files = glob.glob(os.path.join(timeseries_dir, "**", "*_metrics.csv"), recursive=True)
        
        if not csv_files:
            print(f"      [Plotter] 未找到任何数据文件 (在 {timeseries_dir})")
            return

        for f in csv_files:
            filename = os.path.basename(f)
            # 解析 NodeName
            node_name = "UnknownNode"
            parts = re.split(r'[-_]PID', filename)
            if len(parts) > 1:
                node_name = parts[0]
            else:
                parent = os.path.basename(os.path.dirname(f))
                parts_p = re.split(r'[-_]PID', parent)
                if len(parts_p) > 1:
                    node_name = parts_p[0]
            node_files_map[node_name].append(f)

        # 2. 对每个节点进行绘图
        for node, files in node_files_map.items():
            for key in visualize_keys:
                self._plot_node_metric(node, files, key, plots_dir)

    def _plot_node_metric(self, node_name, files, key, output_dir):
        """
        绘制单个节点、单个指标的聚合图 (支持系统级指标叠加)
        """
        key_lower = key.lower()
        
        # 1. 预读取所有 PID 数据，找到全局最早开始时间 (Global T0)
        data_frames = [] # list of (label, df)
        min_timestamp = None
        
        for csv_file in files:
            try:
                df = pd.read_csv(csv_file)
                if 'Timestamp' in df.columns and not df.empty:
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                    start_ts = df['Timestamp'].min()
                    
                    if min_timestamp is None or start_ts < min_timestamp:
                        min_timestamp = start_ts
                    
                    # 提取 PID Label
                    pid_label = "Unknown"
                    pid_match = re.search(r'PID(\d+)', os.path.basename(csv_file))
                    if pid_match: pid_label = f"PID{pid_match.group(1)}"
                    
                    data_frames.append({'type': 'process', 'label': pid_label, 'df': df})
            except: pass

        if not data_frames or min_timestamp is None:
            return

        # 2. 如果是 memory 指标，尝试加载 System Memory
        if key_lower == "memory":
            try:
                first_file = files[0]
                pid_dir = os.path.dirname(first_file)
                node_dir = os.path.dirname(pid_dir)
                sys_mem_path = os.path.join(node_dir, "system_memory.csv")
                
                if os.path.exists(sys_mem_path):
                    df_sys = pd.read_csv(sys_mem_path)
                    if 'Timestamp' in df_sys.columns and not df_sys.empty:
                        df_sys['Timestamp'] = pd.to_datetime(df_sys['Timestamp'])
                        df_sys = df_sys[df_sys['Timestamp'] >= min_timestamp]
                        if not df_sys.empty:
                            data_frames.append({'type': 'system', 'label': 'System Total', 'df': df_sys})
            except Exception as e:
                print(f"⚠️ 加载系统内存失败: {e}")

        # 3. 开始绘图
        plt.figure(figsize=(12, 6))
        has_data = False
        y_label = key
        title = f"{node_name} - {key}"
        
        # === [核心修复] 使用 c.lower() 进行大小写不敏感匹配 ===
        target_cols_filter = lambda c: False
        
        if key_lower == "cpu":
            target_cols_filter = lambda c: "proc_cpu_util" in c.lower()
            y_label = "CPU Utilization (%)"
        elif key_lower == "memory":
            target_cols_filter = lambda c: "proc_mem_rss" in c.lower() # 仅针对进程文件
            y_label = "Memory (MB)"
        elif key_lower == "gpu_util":
            target_cols_filter = lambda c: "gpu" in c.lower() and "util" in c.lower()
            y_label = "GPU Utilization (%)"
        elif key_lower == "gpu_mem":
            target_cols_filter = lambda c: "gpu" in c.lower() and "mem" in c.lower()
            y_label = "VRAM Usage (MiB)"
        elif key_lower == "power" or key_lower == "gpu_power":
            target_cols_filter = lambda c: "gpu" in c.lower() and "power" in c.lower()
            y_label = "Power (W)"

        for item in data_frames:
            df = item['df']
            label_prefix = item['label']
            
            # 计算相对时间
            df['RelativeTime'] = (df['Timestamp'] - min_timestamp).dt.total_seconds()
            
            # 特殊处理 System Memory
            if item['type'] == 'system':
                val_cols = [c for c in df.columns if c != 'Timestamp' and c != 'RelativeTime']
                if val_cols:
                    col = val_cols[0]
                    has_data = True
                    plt.plot(df['RelativeTime'], df[col], label=label_prefix, linewidth=2.5, color='black', linestyle='--')
                continue

            # 处理 Process Metrics
            cols = [c for c in df.columns if target_cols_filter(c)]
            for col in cols:
                has_data = True
                final_label = label_prefix
                if "gpu" in col.lower():
                    gpu_match = re.search(r'(gpu\d+)', col, re.IGNORECASE)
                    if gpu_match: final_label = f"{label_prefix}-{gpu_match.group(1)}"
                
                plot_data = df[col]
                # 内存单位转换 KB -> MB
                if "proc_mem_rss" in col.lower():
                    plot_data = df[col] / 1024.0
                
                plt.plot(df['RelativeTime'], plot_data, label=final_label, linewidth=1.5, alpha=0.8)

        if has_data:
            plt.xlabel("Time (seconds)", fontsize=12)
            plt.ylabel(y_label, fontsize=12)
            plt.title(title, fontsize=14, fontweight='bold')
            plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, fontsize=9)
            plt.grid(True, linestyle='--', alpha=0.5)
            
            ax = plt.gca()
            try: ax.ticklabel_format(style='plain', axis='y', useOffset=False)
            except: pass
            y_formatter = ScalarFormatter(useOffset=False)
            y_formatter.set_scientific(False)
            ax.yaxis.set_major_formatter(y_formatter)

            plt.tight_layout()
            out_filename = f"{node_name}_{key_lower}.png"
            plt.savefig(os.path.join(output_dir, out_filename), dpi=150)
            #print(f"      [Plotter] 生成图表: {out_filename}")
        else:
            # 可选：打印警告，方便调试
            # print(f"      [Plotter] 警告: 在数据中未找到指标 '{key}' 的对应列")
            pass
        
        plt.close()