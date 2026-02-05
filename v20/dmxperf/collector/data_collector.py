# -*- coding: utf-8 -*-
import os
import glob
import pandas as pd
import re

class DataCollector:
    def __init__(self):
        pass

    def aggregate(self, timeseries_root):
        if not os.path.exists(timeseries_root):
            print(f"⚠️ [Collector] 根目录不存在: {timeseries_root}")
            return

        #print(f"🔍 [Collector] 开始扫描目录: {timeseries_root}")
        node_dirs = glob.glob(os.path.join(timeseries_root, "*"))
        
        found_pid = False
        for node_dir in node_dirs:
            if not os.path.isdir(node_dir): continue
            
            pid_dirs = glob.glob(os.path.join(node_dir, "*PID*"))
            for pid_dir in pid_dirs:
                found_pid = True
                #print(f"  -> 处理 PID 目录: {os.path.basename(pid_dir)}")
                self._process_single_pid(pid_dir)
        
        if not found_pid:
            print(f"⚠️ [Collector] 未在 {timeseries_root} 下发现任何 PID 目录！")

    def _process_single_pid(self, pid_dir):
        # 1. 查找所有 csv
        csv_files = glob.glob(os.path.join(pid_dir, "*.csv"))
        # 排除已存在的聚合文件（防止重复处理或误删结果）
        csv_files = [f for f in csv_files if not f.endswith("_metrics.csv")]
        
        if not csv_files:
            return

        dfs = []
        for f in csv_files:
            try:
                file_stem = os.path.splitext(os.path.basename(f))[0]
                
                # 尝试读取
                try:
                    df = pd.read_csv(f)
                except pd.errors.EmptyDataError:
                    # print(f"     ⚠️ 跳过空文件: {os.path.basename(f)}")
                    continue
                except Exception as e:
                    print(f"     ❌ 读取出错 {os.path.basename(f)}: {e}")
                    continue
                
                # 检查 Timestamp
                if 'Timestamp' in df.columns:
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                    df.set_index('Timestamp', inplace=True)
                    df.sort_index(inplace=True) # merge_asof 必须排序
                    
                    # 重命名列
                    new_columns = {}
                    for col in df.columns:
                        clean_col = col.strip()
                        if clean_col.lower() == "value":
                            new_columns[col] = file_stem
                        else:
                            new_columns[col] = f"{file_stem}_{clean_col}"
                    
                    df = df.rename(columns=new_columns)
                    
                    # 强制转数值
                    for col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    dfs.append(df)
                else:
                    print(f"     ⚠️ 跳过无 Timestamp 列的文件: {os.path.basename(f)}")
            except Exception as e:
                print(f"     ❌ 处理文件失败 {os.path.basename(f)}: {e}")

        if not dfs:
            # print(f"     ⚠️ {pid_dir} 中没有加载到任何有效的 DataFrame，跳过聚合。")
            return

        try:
            # === 智能容错合并 ===
            base_df = None
            gpu_dfs = []

            for df in dfs:
                # 简单判断是否 GPU 数据
                is_gpu = any("gpu" in c.lower() for c in df.columns)
                
                if not is_gpu:
                    # CPU/Mem 数据严格合并
                    if base_df is None:
                        base_df = df
                    else:
                        base_df = pd.merge(base_df, df, left_index=True, right_index=True, how='inner')
                else:
                    gpu_dfs.append(df)

            # 兜底：如果没有 CPU 数据，用第一个 GPU 数据做基准
            if base_df is None and gpu_dfs:
                # print("     ℹ️ 无 CPU 数据，使用 GPU 数据作为基准。")
                base_df = gpu_dfs[0]
                gpu_dfs = gpu_dfs[1:]
            
            if base_df is None:
                print("     ❌ 无法确定基准数据 (Base DF is None)，无法合并。")
                return

            final_df = base_df
            
            # 将 GPU 数据挂载 (容忍 2s 误差)
            for gdf in gpu_dfs:
                final_df = pd.merge_asof(
                    final_df, 
                    gdf, 
                    left_index=True, 
                    right_index=True, 
                    tolerance=pd.Timedelta('1s'), 
                    direction='nearest'
                )

            # 清洗空值
            final_df.dropna(inplace=True)
            
            # 排序列
            cols = list(final_df.columns)
            def sort_key(col_name):
                if col_name.startswith("proc_"): return (0, -1, col_name)
                gpu_match = re.match(r"gpu(\d+)[_]", col_name)
                if gpu_match: return (1, int(gpu_match.group(1)), col_name)
                return (2, -1, col_name)

            sorted_cols = sorted(cols, key=sort_key)
            final_df = final_df[sorted_cols]
            
            # 生成结果文件路径
            dirname = os.path.basename(pid_dir) 
            output_name = dirname.replace("-", "_") + "_metrics.csv"
            output_path = os.path.join(pid_dir, output_name)
            
            # 写入聚合文件
            final_df.to_csv(output_path)
            # print(f"     ✅ 已生成: {output_name} ({len(final_df)} 行)")
            
            # === [新增逻辑] 清理冗余的原始 CSV 文件 ===
            # 只有在上面 to_csv 成功后才会执行到这里
            deleted_count = 0
            for f in csv_files:
                try:
                    # 再次检查不是结果文件（双重保险）
                    if os.path.abspath(f) != os.path.abspath(output_path):
                        os.remove(f)
                        deleted_count += 1
                except Exception as e:
                    print(f"     ⚠️ 删除冗余文件失败 {os.path.basename(f)}: {e}")
            
            # if deleted_count > 0:
            #     print(f"     🧹 已清理 {deleted_count} 个原始数据文件")
            
        except Exception as e:
            print(f"     ❌ 合并写入过程发生异常: {e}")
            import traceback
            traceback.print_exc()