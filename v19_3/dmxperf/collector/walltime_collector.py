# -*- coding: utf-8 -*-
import os
import re
import csv

class WalltimeParser:
    def __init__(self):
        # 匹配元数据: "This is the MPI rank 0 ... PID: 12345"
        self.meta_pattern = re.compile(r'MPI rank\s*(\d+).*?PID:\s*(\d+)', re.IGNORECASE)
        
        # 匹配带 Wall time 的行
        self.time_pattern = re.compile(r'(.*?)\[Wall time:\s*([0-9.]+)(?:,\s*Rank:\s*(\d+))?\]')
        
        # 辅助正则：行内找 PID (备用手段)
        self.pid_search_pattern = re.compile(r'PID[:\s=]*(\d+)', re.IGNORECASE)

    def parse(self, log_file, output_csv=None):
        rank_pid_map = {} 
        pid_rank_map = {} 
        rows = []
        
        if not os.path.exists(log_file):
            return {}, 0.0

        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line: continue

                    # 1. 建立 Rank <-> PID 映射表
                    if "PID:" in line and "MPI rank" in line:
                        meta_match = self.meta_pattern.search(line)
                        if meta_match:
                            r = meta_match.group(1)
                            p = meta_match.group(2)
                            rank_pid_map[r] = p
                            pid_rank_map[p] = r

                    # 2. 解析时间行
                    match = self.time_pattern.search(line)
                    if match:
                        inline_text = match.group(1).strip()
                        wall_time_val = float(match.group(2))
                        explicit_rank = match.group(3) # 可能为 None

                        final_rank = None

                        # [策略 1] 优先使用日志中显式的 Rank
                        if explicit_rank:
                            final_rank = explicit_rank
                        
                        # [策略 2] 如果没有显式 Rank，尝试通过 PID 反查
                        if not final_rank:
                            pid_match = self.pid_search_pattern.search(inline_text)
                            if pid_match:
                                found_pid = pid_match.group(1)
                                if found_pid in pid_rank_map:
                                    final_rank = pid_rank_map[found_pid]

                        # [策略 3] 强制兜底：如果还是未知 (NAN)，默认全为 0
                        if not final_rank:
                            final_rank = "0"

                        event_desc = inline_text if inline_text else "Event"
                        rows.append([wall_time_val, final_rank, event_desc])

            # 3. 写入 CSV
            if output_csv and rows:
                os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["WallTime_s", "Rank", "Event"])
                    writer.writerows(rows)

            return {"rank_pid_map": rank_pid_map}

        except Exception as e:
            print(f"❌ [Walltime] Parse error: {e}")
            return {"rank_pid_map": {}}        