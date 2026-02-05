# -*- coding: utf-8 -*-
import os
import pandas as pd

class TimeAnalyzer:
    def __init__(self):
        # 1. 局部事件 (每个 Rank 都有)
        self.marker_program_start = "This is the MPI rank" # T0
        self.marker_solve_end = "stopping the solver"      # T3

        # 2. 全局同步事件 (通常只有 Rank 0 打印，或者是全局路障)
        self.marker_init_end = "Initial field file reading completed" # T1
        self.marker_solve_start = "Create new monitor layer"          # T2

    def analyze(self, walltime_csv):
        """
        返回一个字典:
        {
            "global_init_end": 10.5,
            "global_solve_start": 12.0,
            "ranks": {
                "0": {"init": 1.2, "solve": 50.1},
                "1": {"init": 1.3, "solve": 50.0},
                ...
            }
        }
        """
        result = {"ranks": {}}

        if not os.path.exists(walltime_csv):
            return result

        try:
            df = pd.read_csv(walltime_csv)
            if 'WallTime_s' not in df.columns or 'Event' not in df.columns:
                return result

            # 统一转字符串防止类型问题
            df['Rank'] = df['Rank'].astype(str)

            # === 第一步：计算全局同步点 (T1, T2) ===
            # 逻辑：取整个日志中该事件出现的最早时间作为全局锚点
            
            # T1: Init End
            rows_t1 = df[df['Event'].str.contains(self.marker_init_end, case=False, na=False)]
            global_t1 = float(rows_t1['WallTime_s'].min()) if not rows_t1.empty else None

            # T2: Solve Start
            rows_t2 = df[df['Event'].str.contains(self.marker_solve_start, case=False, na=False)]
            global_t2 = float(rows_t2['WallTime_s'].min()) if not rows_t2.empty else None
            
            # 兜底：如果没找到 T2，用 T1 代替；如果没 T1，用 0
            if global_t1 is None: global_t1 = 0.0
            if global_t2 is None: global_t2 = global_t1

            # === 第二步：按 Rank 计算各自的时长 ===
            # 分组遍历每个 Rank
            for rank, group in df.groupby('Rank'):
                if rank == "nan" or rank == "N/A": continue
                
                # T0: Local Start
                # 该 Rank 最早出现 marker_program_start 的时间
                rows_t0 = group[group['Event'].str.contains(self.marker_program_start, case=False, na=False)]
                local_t0 = float(rows_t0['WallTime_s'].min()) if not rows_t0.empty else 0.0

                # T3: Local End
                # 该 Rank 最晚出现 marker_solve_end 的时间
                rows_t3 = group[group['Event'].str.contains(self.marker_solve_end, case=False, na=False)]
                local_t3 = float(rows_t3['WallTime_s'].max()) if not rows_t3.empty else None
                
                # === 计算逻辑 ===
                # Init = Global_T1 - Local_T0
                # (每个进程启动时间不同，所以 Init 不同)
                init_val = 0.0
                if global_t1 > local_t0:
                    init_val = global_t1 - local_t0

                # Solve = Local_T3 - Global_T2
                # (每个进程结束时间不同，所以 Solve 不同)
                solve_val = 0.0
                if local_t3 is not None and global_t2 is not None:
                    if local_t3 > global_t2:
                        solve_val = local_t3 - global_t2
                
                result["ranks"][rank] = {
                    "init": round(init_val, 4),
                    "solve": round(solve_val, 4)
                }

        except Exception as e:
            print(f"⚠️ [TimeAnalyzer] 分析详细时间失败: {e}")

        return result
        