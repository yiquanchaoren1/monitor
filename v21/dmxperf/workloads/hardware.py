# dmxperf/workloads/hardware.py
# -*- coding: utf-8 -*-
import os
import sys
import socket
from .base import BaseWorkload, WorkloadContext

class HardwareWorkload(BaseWorkload):
    """
    硬件验收专用 Workload
    [修改后逻辑]
    解析 args 字段，第一个词作为工具名，后续作为参数。
    例如 args="gemm 1638 10" -> cmd="./dmxperf --gemm 1638 10"
    """
    def prepare(self) -> WorkloadContext:
        ctx = WorkloadContext()
        ctx.case_name = self.job.get('case_name')
        
        # 1. 准备日志目录
        exp_root = os.path.join(self.run_root, "metrics", ctx.case_name)
        log_dir = os.path.join(exp_root, "logs") 
        
        ctx.paths = { "root": exp_root, "logs": log_dir }
        
        if not self.dry_run:
            os.makedirs(log_dir, exist_ok=True)

        # 2. 解析节点列表 (保持不变)
        target_nodes = []
        raw_layout = self.job.get("node_layout", [])
        if not raw_layout:
            target_nodes = ["localhost"]
        else:
            for item in raw_layout:
                if isinstance(item, str): target_nodes.append(item)
                elif isinstance(item, dict):
                    h = item.get("hostname")
                    if h: target_nodes.append(h)

        ctx.effective_nodes = target_nodes
        ctx.result_dir = None 

        # 3. 确定 dmxperf 可执行路径 (保持不变)
        if getattr(sys, 'frozen', False):
            executable = os.path.abspath(sys.executable)
            base_cmd = executable
        else:
            current_file = os.path.abspath(__file__)
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
            run_py = os.path.join(base_dir, "run.py")
            base_cmd = f"python3 {run_py}"

        # === [核心修改] 解析 args 构造命令 ===
        # 获取用户配置的完整参数字符串，例如 "gemm 16384 10"
        full_args_str = self.job.get('args', '').strip()
        
        if not full_args_str:
            raise ValueError(f"❌ Job '{ctx.case_name}' 缺少 'args' 参数 (例如: 'gemm 1638 10')")

        # 拆分字符串
        # "gemm 16384 10" -> parts=["gemm", "16384", "10"]
        parts = full_args_str.split()
        tool_name = parts[0]   # "gemm"
        tool_args = " ".join(parts[1:]) # "16384 10"
        
        # 构造 CLI flag: "--gemm"
        cli_flag = f"--{tool_name}"
        
        # 最终命令: ./dmxperf --gemm 16384 10
        final_tool_cmd = f"{base_cmd} {cli_flag} {tool_args}"

        # 5. 构建并行 SSH 命令组 (保持不变)
        ssh_cmds = []
        local_hostname = socket.gethostname()
        
        for node in target_nodes:
            node_log = os.path.join(log_dir, f"{node}.log")
            
            if node in ["localhost", "127.0.0.1", local_hostname]:
                cmd = f"{final_tool_cmd} > {node_log} 2>&1"
            else:
                # 使用 bash -l -c 确保加载环境变量
                remote_cmd = f"bash -l -c '{final_tool_cmd}'"
                cmd = f"ssh -o StrictHostKeyChecking=no {node} \"{remote_cmd}\" > {node_log} 2>&1"
            
            ssh_cmds.append(cmd)

        if not ssh_cmds:
            ctx.cmd_string = "echo 'No nodes specified'"
        elif len(ssh_cmds) == 1:
            ctx.cmd_string = ssh_cmds[0]
        else:
            ctx.cmd_string = "(" + " & ".join(ssh_cmds) + " & wait)"
            
        ctx.log_file = os.path.join(self.run_root, "logs", f"{ctx.case_name}_dispatch.log")
        ctx.env = os.environ.copy()

        return ctx