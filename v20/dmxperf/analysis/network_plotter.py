# dmxperf/analysis/network_plotter.py
# -*- coding: utf-8 -*-
import os
import glob
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg') # 后台绘图
import matplotlib.pyplot as plt

class NetworkPlotter:
    def __init__(self, run_root):
        self.run_root = run_root
        self.metrics_root = os.path.join(run_root, "metrics")

    def plot_cluster_network(self, job_name):
        """
        入口函数：绘制集群维度的网络分析图
        """
        job_dir = os.path.join(self.metrics_root, job_name)
        ts_dir = os.path.join(job_dir, "TimeSeries")
        events_dir = os.path.join(job_dir, "Events")
        plots_dir = os.path.join(job_dir, "Plots", "Network")
        os.makedirs(plots_dir, exist_ok=True)

        print(f"      [NetPlot] 正在生成集群网络图表 (IB+ENS+IBP | 同步Summary时间): {job_name}")

        # 1. 加载数据
        node_dfs, global_min_time = self._load_all_nodes(ts_dir)
        if not node_dfs:
            print("      [NetPlot] ⚠️ 未找到网络数据")
            return

        # 2. 对齐时间 (计算 RelativeTime)
        valid_dfs = {}
        for node, df in node_dfs.items():
            df['RelativeTime'] = (df['Timestamp'] - global_min_time).dt.total_seconds()
            valid_dfs[node] = df

        # 3. 加载事件时间 (严格对齐 reporter.py 逻辑)
        events = self._load_events(events_dir, global_min_time)

        # === 4. 生成图表 ===
        # [核心图] 分节点堆叠子图 (6条线: IB/ENS/IBP 的 Rx/Tx)
        self._plot_per_node_subplots(valid_dfs, events, plots_dir)

    def _load_all_nodes(self, ts_dir):
        """扫描并加载 CSV"""
        node_map = {}
        min_ts = None
        files = glob.glob(os.path.join(ts_dir, "**", "network_metrics.csv"), recursive=True)
        
        for f in files:
            try:
                # 提取节点名
                parent = os.path.basename(os.path.dirname(f))
                if parent == "TimeSeries": node_name = "localhost"
                else: node_name = parent.split('-PID')[0]
                
                df = pd.read_csv(f)
                if 'Timestamp' not in df.columns: continue
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                
                local_min = df['Timestamp'].min()
                if min_ts is None or local_min < min_ts:
                    min_ts = local_min
                
                node_map[node_name] = df
            except: pass
        return node_map, min_ts

    def _load_events(self, events_dir, global_t0):
        """
        解析 walltime.csv 获取阶段时间点。
        [修正] 逻辑严格对齐 time.py (TimeAnalyzer)
        """
        events = {}
        walltime_csv = os.path.join(events_dir, "walltime.csv")
        if not os.path.exists(walltime_csv):
            return events
            
        try:
            df = pd.read_csv(walltime_csv)
            
            # === 定义关键事件标记 (与 time.py 保持一致) ===
            marker_init_end = "Initial field file reading completed" # T1
            marker_solve_start = "Create new monitor layer"          # T2
            marker_solve_end = "stopping the solver"                 # T3
            
            # 1. 确定 Solver Start (优先使用 T2，若无则回退到 T1)
            # T2: Global Solve Start
            rows_t2 = df[df['Event'].str.contains(marker_solve_start, case=False, na=False)]
            start_time = None
            
            if not rows_t2.empty:
                start_time = rows_t2['WallTime_s'].min()
            else:
                # Fallback T1: Global Init End
                rows_t1 = df[df['Event'].str.contains(marker_init_end, case=False, na=False)]
                if not rows_t1.empty:
                    start_time = rows_t1['WallTime_s'].min()
            
            if start_time is not None:
                events['Solver Start'] = start_time

            # 2. 确定 Solver End (使用 T3 的最大值，即所有 Rank 都结束)
            rows_t3 = df[df['Event'].str.contains(marker_solve_end, case=False, na=False)]
            if not rows_t3.empty:
                events['Solver End'] = rows_t3['WallTime_s'].max()
                
        except Exception as e:
            # print(f"Event parsing error: {e}")
            pass
            
        return events

    # =========================================================
    #  图表逻辑：多子图垂直堆叠 (含 IB, ENS, IBP)
    # =========================================================
    def _plot_per_node_subplots(self, node_dfs, events, out_dir):
        nodes = sorted(node_dfs.keys())
        n_nodes = len(nodes)
        
        if n_nodes == 0: return

        # 动态计算画布高度
        fig_height = max(6, 3 * n_nodes)
        
        # 创建子图
        fig, axes = plt.subplots(nrows=n_nodes, ncols=1, figsize=(16, fig_height), sharex=True)
        if n_nodes == 1: axes = [axes]
        
        colors_evt = {'Solver Start': 'red', 'Solver End': 'green'}
        
        # 定义颜色 (Matplotlib tab10)
        c_ib  = '#1f77b4' # Blue
        c_ens = '#ff7f0e' # Orange
        c_ibp = '#2ca02c' # Green

        for i, node in enumerate(nodes):
            ax = axes[i]
            df = node_dfs[node]
            has_data = False
            
            try:
                # 重采样
                d = df.set_index('RelativeTime')
                d.index = pd.to_timedelta(d.index, unit='s')
                resampled = d.resample('1S').mean(numeric_only=True)
                
                x_axis = resampled.index.total_seconds()

                # --- 1. IB 数据 (IB_*) ---
                # Rx
                ib_rx_cols = [c for c in df.columns if 'IB_' in c and '_Rx' in c]
                if ib_rx_cols: 
                    s = resampled[ib_rx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='IB Rx', color=c_ib, linestyle='-')
                        has_data = True
                # Tx
                ib_tx_cols = [c for c in df.columns if 'IB_' in c and '_Tx' in c]
                if ib_tx_cols:
                    s = resampled[ib_tx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='IB Tx', color=c_ib, linestyle='--')
                        has_data = True

                # --- 2. ETH_ens 数据 (ETH_ens*) ---
                # Rx
                ens_rx_cols = [c for c in df.columns if 'ETH_ens' in c and '_Rx' in c]
                if ens_rx_cols:
                    s = resampled[ens_rx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='ENS Rx', color=c_ens, linestyle='-')
                        has_data = True
                # Tx
                ens_tx_cols = [c for c in df.columns if 'ETH_ens' in c and '_Tx' in c]
                if ens_tx_cols:
                    s = resampled[ens_tx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='ENS Tx', color=c_ens, linestyle='--')
                        has_data = True

                # --- 3. ETH_ibp34s0 数据 (ETH_ibp34s0*) ---
                # Rx
                ibp_rx_cols = [c for c in df.columns if 'ETH_ibp34s0' in c and '_Rx' in c]
                if ibp_rx_cols:
                    s = resampled[ibp_rx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='IBP Rx', color=c_ibp, linestyle='-')
                        has_data = True
                # Tx
                ibp_tx_cols = [c for c in df.columns if 'ETH_ibp34s0' in c and '_Tx' in c]
                if ibp_tx_cols:
                    s = resampled[ibp_tx_cols].sum(axis=1)
                    if s.max() > 0:
                        ax.plot(x_axis, s, label='IBP Tx', color=c_ibp, linestyle='--')
                        has_data = True

            except Exception as e:
                pass
            
            # 子图样式
            ax.set_title(f"Node: {node}", loc='left', fontsize=12, fontweight='bold', pad=10)
            ax.set_ylabel("MB/s", fontsize=10)
            ax.grid(True, linestyle='--', alpha=0.5)
            
            # 阶段竖线
            for name, time_sec in events.items():
                c = colors_evt.get(name, 'black')
                ax.axvline(x=time_sec, color=c, linestyle=':', linewidth=1.5, alpha=0.8)
                if i == 0:
                    # 只在第一个图显示标签，避免拥挤
                    ax.text(time_sec + 0.5, ax.get_ylim()[1] * 0.9, name, color=c, fontweight='bold')

            # 图例
            if has_data:
                ax.legend(loc='upper right', fontsize=8, ncol=3)

        # 底部 X 轴
        axes[-1].set_xlabel("Time (seconds)", fontsize=12)
        
        fig.suptitle("Network Traffic Breakdown (IB vs ENS vs IBP)", fontsize=16, fontweight='bold', y=1.005)
        
        plt.tight_layout()
        out_file = os.path.join(out_dir, "cluster_subplots_stacked.png")
        plt.savefig(out_file, dpi=150, bbox_inches='tight')
        plt.close()