# -*- coding: utf-8 -*-
import os
import csv
import glob
import re
import pandas as pd

class Reporter:
    def __init__(self, run_root, config):
        self.run_root = run_root
        self.metrics_root = os.path.join(run_root, "metrics")
        self.report_dir = os.path.join(run_root, "report")
        self.config = config

    def _find_col(self, df, candidates):
        """查找匹配的列名（不区分大小写）"""
        for cand in candidates:
            # 精确匹配
            if cand in df.columns: return cand
            # 遍历查找（忽略大小写）
            for col in df.columns:
                if cand.lower() == col.lower(): return col
                # 部分匹配兼容 (例如 'proc_mem' 匹配 'proc_mem_rss')
                if cand.lower() in col.lower() and 'gpu' not in col.lower():
                    return col
        return None

    def generate_summary(self, job_meta_map):
        #print(f"      [Reporter] 正在生成汇总报告 -> {self.report_dir}")
        os.makedirs(self.report_dir, exist_ok=True)
        
        summary_rows = []
        
        # 1. 获取全局 Loop 设置 (默认 1)
        global_cfg = self.config.get('global', {})
        global_loop = int(global_cfg.get('loop', 1))
        
        for job in self.config.get('jobs', []):
            base_case_name = job.get('case_name')
            
            # 2. 计算当前 Job 的 Loop 次数
            job_loop = int(job.get('loop', global_loop))
            
            # 3. 生成需要扫描的实际 Case Name 列表
            target_case_names = []
            if job_loop > 1:
                for k in range(job_loop):
                    target_case_names.append(f"{base_case_name}_{k+1}")
            else:
                target_case_names.append(base_case_name)
            
            # 4. 遍历实际执行生成的 Case
            for case_name in target_case_names:
                # 获取该 Job 的元数据
                meta = job_meta_map.get(case_name, {})
                rank_pid_map = meta.get('rank_pid_map', {})
                
                # === [修改点] 获取详细的时间统计字典 (按 Rank 区分) ===
                # 这里的数据由 TimeAnalyzer.analyze() 生成并存入 meta
                time_stats = meta.get('time_stats', {})
                ranks_time_data = time_stats.get('ranks', {})
                
                pid_to_rank_map = {v: int(k) for k, v in rank_pid_map.items()}

                timeseries_dir = os.path.join(self.metrics_root, case_name, "TimeSeries")
                
                # 检查目录是否存在
                if not os.path.exists(timeseries_dir):
                    # print(f"      [Reporter] 跳过缺失目录: {timeseries_dir}")
                    continue

                # 搜索所有 metrics CSV
                csv_files = glob.glob(os.path.join(timeseries_dir, "**", "*_metrics.csv"), recursive=True)
                
                if not csv_files: continue

                for f in csv_files:
                    try:
                        df = pd.read_csv(f)
                        
                        # === PID / Node 补全逻辑 ===
                        if 'pid' not in df.columns or 'node' not in df.columns:
                            filename = os.path.basename(f)
                            
                            # 尝试从文件名提取 PID
                            pid_val = "Unknown"
                            pid_match = re.search(r'PID(\d+)', filename) or re.search(r'PID(\d+)', os.path.basename(os.path.dirname(f)))
                            if pid_match: 
                                pid_val = pid_match.group(1)
                            
                            # 如果找不到 PID，说明这是系统级文件(如 network_metrics.csv)，直接跳过
                            if pid_val == "Unknown" and 'pid' not in df.columns:
                                continue 
                            
                            node_val = "Unknown"
                            split_match = re.split(r'[-_]PID', filename)
                            if len(split_match) > 1: node_val = split_match[0]
                            
                            if 'pid' not in df.columns: df['pid'] = pid_val
                            if 'node' not in df.columns: df['node'] = node_val

                        df['pid'] = df['pid'].astype(str)
                        
                        # === 计算 Rank ===
                        if pid_to_rank_map:
                            df['rank'] = df['pid'].map(pid_to_rank_map).fillna(-1).astype(int)
                        else:
                            unique_pids = sorted(df['pid'].unique())
                            pid_rank_dict = {p: i for i, p in enumerate(unique_pids)}
                            df['rank'] = df['pid'].map(pid_rank_dict)

                        # === 按 PID 分组统计 ===
                        for pid, group in df.groupby('pid'):
                            if str(pid) == "Unknown": continue

                            rank = group['rank'].iloc[0]
                            node = group['node'].iloc[0]
                            
                            # === [修改点] 根据 Rank 查找独立的时间统计 ===
                            rank_str = str(int(rank)) if rank != -1 else "0"
                            p_init = 0.0
                            p_solve = 0.0
                            
                            if rank_str in ranks_time_data:
                                # 如果找到了该 Rank 的特定时间
                                p_data = ranks_time_data[rank_str]
                                p_init = p_data.get('init', 0.0)
                                p_solve = p_data.get('solve', 0.0)
                            else:
                                # 如果没找到（例如 Rank 0 才有日志），尝试回退到 Rank 0 的数据
                                if "0" in ranks_time_data:
                                    p_init = ranks_time_data["0"].get('init', 0.0)
                                    p_solve = ranks_time_data["0"].get('solve', 0.0)
                            
                            # 动态计算 GPU 数量
                            detected_gpu_ids = set()
                            for col in group.columns:
                                m = re.search(r'gpu(\d+)', col, re.IGNORECASE)
                                if m: detected_gpu_ids.add(m.group(1))
                            
                            real_gpu_count = len(detected_gpu_ids)
                            if real_gpu_count == 0:
                                 real_gpu_count = int(job.get('gpus_per_proc', 0))

                            # (1) 内存指标
                            mem_col = self._find_col(group, ['proc_mem_rss', 'rss_mem', 'rss', 'Memory'])
                            peak_mem = 0
                            if mem_col:
                                try:
                                    raw_val = group[mem_col].max()
                                    if 'proc_mem' in mem_col:
                                        peak_mem = int(raw_val / 1024.0)
                                    else:
                                        peak_mem = int(raw_val)
                                except: pass
                            
                            # (2) CPU 指标
                            cpu_col = self._find_col(group, ['proc_cpu_util', 'cpu_util', 'cpu', 'CPU'])
                            avg_cpu = round(group[cpu_col].mean(), 2) if cpu_col else 0
                            
                            # (3) GPU 指标
                            gpu_mem_cols = [c for c in group.columns if 'gpu' in c.lower() and 'mem' in c.lower()]
                            gpu_util_cols = [c for c in group.columns if 'gpu' in c.lower() and 'util' in c.lower()]
                            
                            def get_gpu_id(col_name):
                                m = re.search(r'gpu(\d+)', col_name, re.IGNORECASE)
                                return int(m.group(1)) if m else 999
                                
                            gpu_mem_cols.sort(key=get_gpu_id)
                            gpu_util_cols.sort(key=get_gpu_id)
                            
                            peak_gpu_mem = str(list(group[gpu_mem_cols].max(axis=0).fillna(0).astype(int))) if gpu_mem_cols else "[]"
                            
                            avg_gpu_util = "[]"
                            if gpu_util_cols:
                                final_means = []
                                for col in gpu_util_cols:
                                    if not group[col].empty:
                                        val = group[col].mean()
                                    else:
                                        val = 0.0
                                    final_means.append(round(val, 1))
                                avg_gpu_util = str(final_means)

                            row = {
                                "pid": f"PID{pid}",
                                "node": node,
                                "job": case_name, # 使用实际 case_name
                                "gpus": real_gpu_count,
                                "description": "empty",
                                "rank": rank, 
                                "peak_memory(MB)": peak_mem,
                                "average_cpu(%)": avg_cpu,
                                "peak_gpu_mem(MB)": peak_gpu_mem,
                                "average_gpu_use(%)": avg_gpu_util,
                                # === [修改点] 填入独立的 init 和 solve 时间 ===
                                "init_duration(s)": f"{p_init:.4f}",
                                "solve_duration(s)": f"{p_solve:.4f}"
                            }
                            summary_rows.append(row)

                    except Exception as e:
                        print(f"❌ 处理文件 {f} 出错: {e}")

        if summary_rows:
            # 排序: Job -> Rank
            summary_rows.sort(key=lambda x: (x['job'], x['rank']))
            keys = summary_rows[0].keys()
            summary_file = os.path.join(self.report_dir, "summary.csv")
            with open(summary_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(summary_rows)
            print(f"✅ [Reporter] 汇总报告已生成: {summary_file}")