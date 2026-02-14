# dmxperf/monitor/manager.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import socket
from dmxperf.infra.executor import ExecutorFactory

class MonitorManager:
    def __init__(self, run_root, dry_run=False):
        self.run_root = run_root
        self.dry_run = dry_run
        self.host_agent_script, self.device_agent_script = self._locate_agents()
        self.is_binary_mode = getattr(sys, 'frozen', False)

    def _locate_agents(self):
        """自动定位 Agent 脚本或二进制位置"""
        if getattr(sys, 'frozen', False):
            # Binary Mode (PyInstaller 打包后)
            base_dir = os.path.dirname(os.path.abspath(sys.executable))
            host = os.path.join(base_dir, "dmx_host_agent")
            dev = os.path.join(base_dir, "dmx_device_agent")
        else:
            # Source Mode (源码运行)
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            host = os.path.join(base_dir, "agents", "host_agent.py")
            dev = os.path.join(base_dir, "agents", "device_agent.py")
        
        return host, dev

    def start(self, node_list, target_name, interval, gpus_per_proc=1, timeseries_root=None, silent=False):
        """
        在指定节点列表启动 Agent
        :param silent: 是否静默启动 (不打印日志，由 Controller 统一打印)
        """
        if not timeseries_root:
            timeseries_root = os.path.join(self.run_root, "metrics", target_name, "TimeSeries")
            
        unique_nodes = list(set(node_list))
        
        # [支持静默] 配合 Controller 的树状日志
        if not silent:
            print(f"      [Monitor] 启动监控 -> Nodes: {unique_nodes}")

        # 先清理可能的残留 (静默清理)
        self.stop(unique_nodes, silent=True)

        # 构建启动命令前缀
        if self.is_binary_mode:
            cmd_host_prefix = f"nohup {self.host_agent_script}"
            cmd_dev_prefix  = f"nohup {self.device_agent_script}"
        else:
            cmd_host_prefix = f"nohup python3 {self.host_agent_script}"
            cmd_dev_prefix  = f"nohup python3 {self.device_agent_script}"

        for node in unique_nodes:
            executor = ExecutorFactory.create(node, self.dry_run)
            
            # 1. 启动 Host Agent
            log_host = os.path.join(self.run_root, "logs", f"agent_host_{node}.log")
            cmd_host = (f"{cmd_host_prefix} "
                        f"--timeseries_root {timeseries_root} "
                        f"--target_name {target_name} "
                        f"--interval {interval} ")
            executor.exec_background(cmd_host, log_file=log_host)

            # 2. 启动 Device Agent
            log_dev = os.path.join(self.run_root, "logs", f"agent_device_{node}.log")
            cmd_dev = (f"{cmd_dev_prefix} "
                       f"--timeseries_root {timeseries_root} "
                       f"--target_name {target_name} "
                       f"--interval {interval} "
                       f"--gpus_per_proc {gpus_per_proc} ")
            executor.exec_background(cmd_dev, log_file=log_dev)

    def stop(self, node_list, silent=False):
        """
        停止指定节点上的 Agent (包含本地防误杀逻辑)
        :param silent: 是否静默停止
        """
        unique_nodes = list(set(node_list))
        if not silent:
            print(f"      [Monitor] 停止监控 -> Nodes: {unique_nodes}")
            
        # 获取当前进程 PID (用于本地命令排除自己)
        my_pid = os.getpid()

        # === 1. 远程命令: 直接强杀 ===
        # 先尝试 SIGINT 让 Agent 优雅落盘，再 SIGKILL
        remote_cmd = (
            "pkill -INT -f dmx_host_agent 2>/dev/null; "
            "pkill -INT -f dmx_device_agent 2>/dev/null; "
            "sleep 1; " 
            "pkill -9 -f dmxperf/agents 2>/dev/null; "
            "pkill -9 -f dmx_host_agent 2>/dev/null; "
            "pkill -9 -f dmx_device_agent 2>/dev/null; "
        )

        # === 2. 本地命令: 排除自己 ===
        # 使用 pgrep 过滤掉当前进程 PID，防止误杀 Controller 自身
        local_cmd = (
            f"pgrep -f dmx_host_agent | grep -v {my_pid} | xargs -r kill -2 2>/dev/null; "
            f"pgrep -f dmx_device_agent | grep -v {my_pid} | xargs -r kill -2 2>/dev/null; "
            "sleep 1; "
            f"pgrep -f dmxperf/agents | grep -v {my_pid} | xargs -r kill -9 2>/dev/null; "
            f"pgrep -f dmx_host_agent | grep -v {my_pid} | xargs -r kill -9 2>/dev/null; "
            f"pgrep -f dmx_device_agent | grep -v {my_pid} | xargs -r kill -9 2>/dev/null; "
        )

        for node in unique_nodes:
            executor = ExecutorFactory.create(node, self.dry_run)
            
            # 根据 executor 判断是否为本地节点，选择对应的命令
            target_cmd = local_cmd if executor.is_local else remote_cmd
            
            # 执行命令，忽略 -9 (SIGKILL), 137 (Kill via -9), 255 (SSH error sometimes), 1 (No process found)
            executor.run(target_cmd, ignore_errors=[-9, 137, 255, 1])