# dmxperf/agents/host_agent.py
# -*- coding: utf-8 -*-
import argparse
import time
import datetime
import os
import signal
import socket
import threading
import queue
import sys

running = True
def handle_signal(s, f):
    global running
    try:
        print(f"[HostAgent] 收到信号 {s}，准备退出...")
    except: pass
    running = False

class AsyncWriter:
    def __init__(self, root, node):
        self.q = queue.Queue()
        self.node_dir = os.path.join(root, node)
        self.node_name = node
        try:
            os.makedirs(self.node_dir, exist_ok=True)
            print(f"[AsyncWriter] 数据输出目录: {self.node_dir}")
        except Exception as e:
            print(f"❌ 目录创建失败: {e}")
            raise e
        self.t = threading.Thread(target=self._loop, daemon=True)
        self.t.start()
        self.files = {}

    def write(self, pid, filename, ts, val, header=None):
        if pid == "" or pid is None:
            path = os.path.join(self.node_dir, filename)
        else:
            path = os.path.join(self.node_dir, f"{self.node_name}-PID{pid}", filename)
        self.q.put((path, f"{ts},{val}\n", header))

    def _loop(self):
        while True:
            item = self.q.get()
            if item is None: break
            path, line, header_def = item
            try:
                if path not in self.files:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    f = open(path, 'a', encoding='utf-8', buffering=1)
                    self.files[path] = f
                    if os.path.getsize(path) == 0:
                        hdr = header_def if header_def else "Timestamp,Value"
                        if not hdr.endswith('\n'): hdr += '\n'
                        f.write(hdr)
                        f.flush()
                self.files[path].write(line)
            except Exception as e: pass

    def close(self):
        self.q.put(None)
        self.t.join()
        for f in self.files.values():
            try: f.close()
            except: pass

class NetworkMonitor:
    def __init__(self):
        self.ib_phys = self._scan_ib_physical()
        self.ib_names = [x[0] for x in self.ib_phys]
        self.eth_phys = self._scan_eth_physical()
        self.eth_names = [x[0] for x in self.eth_phys]
        
        self.last_stats = self._read_counters()
        self.last_time = time.time()

    def _scan_ib_physical(self):
        base = "/sys/class/infiniband"
        devices = []
        if not os.path.exists(base): return []
        
        candidates = [("port_rcv_data_64", "port_xmit_data_64"), 
                      ("port_rcv_data", "port_xmit_data"), 
                      ("rx_bytes", "tx_bytes")]
        try:
            for dev in sorted(os.listdir(base)): 
                ports_dir = os.path.join(base, dev, "ports")
                if not os.path.exists(ports_dir): continue
                
                for port in sorted(os.listdir(ports_dir)):
                    cnt_path = os.path.join(ports_dir, port, "counters")
                    if not os.path.isdir(cnt_path): continue
                    
                    for rx, tx in candidates:
                        if os.path.exists(os.path.join(cnt_path, tx)):
                            name = f"{dev}_port{port}"
                            devices.append((name, cnt_path, (rx, tx)))
                            break
        except: pass
        return devices

    def _scan_eth_physical(self):
        base = "/sys/class/net"
        devices = []
        if not os.path.exists(base): return []
        
        try:
            for iface in sorted(os.listdir(base)):
                if iface == "lo": continue
                iface_path = os.path.join(base, iface)
                if not os.path.exists(os.path.join(iface_path, "device")):
                    continue
                
                stat_path = os.path.join(iface_path, "statistics")
                if os.path.exists(stat_path):
                    devices.append((iface, stat_path))
        except: pass
        return devices

    def _read_counters(self):
        stats = {}
        for name, path, (rx_n, tx_n) in self.ib_phys:
            try:
                r = 0; t = 0
                with open(os.path.join(path, rx_n), 'r') as f:
                    v = int(f.read().strip())
                    if "packet" not in rx_n and "bytes" not in rx_n: v *= 4
                    r = v
                with open(os.path.join(path, tx_n), 'r') as f:
                    v = int(f.read().strip())
                    if "packet" not in tx_n and "bytes" not in tx_n: v *= 4
                    t = v
                stats[f"IB_{name}"] = {'rx': r, 'tx': t}
            except: pass

        for name, path in self.eth_phys:
            try:
                r = 0; t = 0
                with open(os.path.join(path, "rx_bytes"), 'r') as f:
                    r = int(f.read().strip())
                with open(os.path.join(path, "tx_bytes"), 'r') as f:
                    t = int(f.read().strip())
                stats[f"ETH_{name}"] = {'rx': r, 'tx': t}
            except: pass
        return stats

    def collect(self):
        curr = self._read_counters()
        res = {}
        for key, curr_val in curr.items():
            last_val = self.last_stats.get(key, {'rx': 0, 'tx': 0})
            dr = max(0, curr_val['rx'] - last_val['rx'])
            dt = max(0, curr_val['tx'] - last_val['tx'])
            res[key] = {'rx_mb': dr / 1048576.0, 'tx_mb': dt / 1048576.0}
        self.last_stats = curr
        return res

class HostAgent:
    def __init__(self, root, target_name, interval):
        try: os.nice(19)
        except: pass
        self.target_name = target_name
        self.interval = max(0.1, interval)
        self.node = socket.gethostname()
        self.writer = AsyncWriter(root, self.node)
        self.prev_cpu = {}
        try: self.clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        except: self.clk_tck = 100
        
        self.net_mon = NetworkMonitor()
        
        self.col_keys = []
        for name in self.net_mon.ib_names: self.col_keys.append(f"IB_{name}")
        for name in self.net_mon.eth_names: self.col_keys.append(f"ETH_{name}")
            
        self.net_header = "Timestamp"
        for key in self.col_keys: self.net_header += f",{key}_Rx_MB,{key}_Tx_MB"

        print(f"[HostAgent] 启动成功: Node={self.node}")
        print(f"[HostAgent] 物理层监控列: {self.col_keys}")

    # === [关键修复] 重新实现 _get_pids 以精准过滤 ===
    def _get_pids(self):
        pids = []
        target_token = f"/{self.target_name}/"
        my_pid = os.getpid()
        
        # 黑名单: 排除启动器、Shell、以及 Agent 自身
        # 在这里显式加入 dmx_host_agent 和 dmx_device_agent
        BLACKLIST = {
            "mpirun", "mpiexec", "orterun", "hydra_pmi_proxy", "srun", 
            "bash", "sh", "zsh", "csh", "tcsh",                        
            "ssh", "sshd", "sudo", "su",
            "dmxperf", "dmx_host_agent", "dmx_device_agent" # <--- 核心修复
        }

        try:
            for pid_str in os.listdir('/proc'):
                if not pid_str.isdigit(): continue
                pid = int(pid_str)
                if pid == my_pid: continue
                
                try:
                    with open(f"/proc/{pid}/cmdline", 'rb') as f:
                        content = f.read()
                        if not content: continue
                        args = content.split(b'\0')
                        
                        exe_path = args[0].decode(errors='ignore')
                        exe_name = os.path.basename(exe_path)
                        full_cmd = b" ".join(args).decode(errors='ignore')

                    # 1. 黑名单过滤 (现在包含了 dmx_host_agent)
                    if exe_name in BLACKLIST: continue
                    
                    # 2. 额外防护: 命令行包含自身名字的也不要
                    if "dmxperf" in full_cmd or "dmx_host_agent" in full_cmd or "dmx_device_agent" in full_cmd: 
                        continue

                    # 3. 目标匹配逻辑
                    is_target = False
                    if target_token in exe_path:
                        is_target = True
                    elif exe_path.startswith(f"./{self.target_name}"):
                        is_target = True
                    elif ("python" in exe_name or "python3" in exe_name) and target_token in full_cmd:
                        is_target = True
                    
                    if is_target:
                        pids.append(pid)

                except: continue
        except: pass
        return pids

    def _collect_proc(self, pid):
        rss = 0; cpu = 0.0
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"): rss = int(line.split()[1]); break
            with open(f"/proc/{pid}/stat") as f:
                parts = f.read().split(')')[-1].split()
                ticks = int(parts[11]) + int(parts[12])
            now = time.time()
            if pid in self.prev_cpu:
                pt, ptm = self.prev_cpu[pid]
                dt = now - ptm
                if dt > 0: cpu = ((ticks - pt) / self.clk_tck) / dt * 100
            self.prev_cpu[pid] = (ticks, now)
        except: pass
        return round(cpu, 1), rss

    def run(self):
        try:
            print("[HostAgent] 进入监控循环...")
            time.sleep(self.interval)
            
            while running:
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                try:
                    with open('/proc/meminfo') as f: mem = {l.split(':')[0]: int(l.split()[1]) for l in f}
                    sys_mem = (mem['MemTotal'] - mem.get('MemAvailable', mem['MemFree'])) / 1024
                    self.writer.write("", "system_memory.csv", ts, f"{sys_mem:.1f}")
                except: pass

                try:
                    net_data = self.net_mon.collect()
                    val_list = []
                    for key in self.col_keys:
                        d = net_data.get(key, {'rx_mb':0.0, 'tx_mb':0.0})
                        val_list.append(f"{d['rx_mb']:.4f}")
                        val_list.append(f"{d['tx_mb']:.4f}")
                    self.writer.write("", "network_metrics.csv", ts, ",".join(val_list), header=self.net_header)
                except: pass

                for pid in self._get_pids():
                    c, r = self._collect_proc(pid)
                    self.writer.write(pid, "proc_cpu_util.csv", ts, c)
                    self.writer.write(pid, "proc_mem_rss.csv", ts, r)

                time.sleep(self.interval)
                
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            self.writer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeseries_root", required=True)
    parser.add_argument("--target_name", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    HostAgent(args.timeseries_root, args.target_name, args.interval).run()