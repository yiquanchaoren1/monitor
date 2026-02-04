# -*- coding: utf-8 -*-
import argparse
import time
import datetime
import os
import signal
import socket
import threading
import queue
import ctypes
from ctypes import *

running = True
def handle_signal(s, f): global running; running = False

# ==============================================================================
# 1. 手动封装 NVML (ctypes) - 替代 pynvml
# ==============================================================================
class NvmlStructs:
    class nvmlUtilization_t(Structure):
        _fields_ = [("gpu", c_uint), ("memory", c_uint)]

    class nvmlPciInfo_t(Structure):
        _fields_ = [("busId", c_char * 16), 
                    ("domain", c_uint), 
                    ("bus", c_uint), 
                    ("device", c_uint), 
                    ("pciDeviceId", c_uint), 
                    ("pciSubSystemId", c_uint), 
                    ("reserved0", c_uint), 
                    ("reserved1", c_uint), 
                    ("reserved2", c_uint), 
                    ("reserved3", c_uint)]

    class nvmlProcessInfo_t(Structure):
        _fields_ = [("pid", c_uint), ("usedGpuMemory", c_ulonglong)]

class NvmlNative:
    def __init__(self):
        self.lib = None
        self.NVML_SUCCESS = 0
        try:
            self.lib = CDLL("libnvidia-ml.so.1")
        except OSError:
            try:
                self.lib = CDLL("libnvidia-ml.so")
            except OSError:
                print("❌ 无法加载 libnvidia-ml.so，请确保安装了 NVIDIA 驱动。")
                return

    def check_error(self, ret):
        if ret != self.NVML_SUCCESS:
            raise RuntimeError(f"NVML Error code: {ret}")

    def init(self):
        if not self.lib: return
        self.check_error(self.lib.nvmlInit())

    def shutdown(self):
        if not self.lib: return
        try: self.lib.nvmlShutdown()
        except: pass

    def device_get_count(self):
        count = c_uint()
        self.check_error(self.lib.nvmlDeviceGetCount_v2(byref(count)))
        return count.value

    def device_get_handle_by_index(self, index):
        handle = c_void_p()
        self.check_error(self.lib.nvmlDeviceGetHandleByIndex_v2(index, byref(handle)))
        return handle

    def device_get_pci_info(self, handle):
        pci = NvmlStructs.nvmlPciInfo_t()
        self.check_error(self.lib.nvmlDeviceGetPciInfo_v3(handle, byref(pci)))
        return pci.busId.decode("utf-8")

    def device_get_utilization_rates(self, handle):
        util = NvmlStructs.nvmlUtilization_t()
        self.check_error(self.lib.nvmlDeviceGetUtilizationRates(handle, byref(util)))
        return util.gpu

    def device_get_power_usage(self, handle):
        power = c_uint() # mW
        try:
            ret = self.lib.nvmlDeviceGetPowerUsage(handle, byref(power))
            if ret == self.NVML_SUCCESS: return power.value
        except: pass
        return 0

    def device_get_compute_running_processes(self, handle):
        count = c_uint(0)
        ret = self.lib.nvmlDeviceGetComputeRunningProcesses(handle, byref(count), None)
        if count.value == 0: return []
        procs = (NvmlStructs.nvmlProcessInfo_t * count.value)()
        ret = self.lib.nvmlDeviceGetComputeRunningProcesses(handle, byref(count), procs)
        if ret != self.NVML_SUCCESS: return []
        result = []
        for i in range(count.value):
            result.append({'pid': procs[i].pid, 'usedGpuMemory': procs[i].usedGpuMemory})
        return result

# ==============================================================================
# 2. AsyncWriter (修复了 NFS 同步问题)
# ==============================================================================
class AsyncWriter:
    def __init__(self, root, node):
        self.q = queue.Queue()
        self.node_dir = os.path.join(root, node)
        os.makedirs(self.node_dir, exist_ok=True)
        self.t = threading.Thread(target=self._loop, daemon=True)
        self.t.start()
        self.files = {}
        self.node_name = node

    def write(self, pid, filename, ts, val, header="Timestamp,Value"):
        path = os.path.join(self.node_dir, f"{self.node_name}-PID{pid}", filename)
        self.q.put((path, header, f"{ts},{val}\n"))

    def _loop(self):
        while True:
            item = self.q.get()
            if item is None: break
            path, header, line = item
            if path not in self.files:
                try:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    f = open(path, 'a', buffering=1)
                    self.files[path] = f
                    if os.path.getsize(path) == 0: f.write(f"{header}\n")
                except Exception as e:
                    continue
            try:
                self.files[path].write(line)
            except: pass

    def close(self):
        # 1. 停止处理
        self.q.put(None)
        self.t.join()
        
        # 2. [关键修改] 强制刷盘，确保数据写入 NFS
        print(f"💾 [AsyncWriter] 正在强制同步 {len(self.files)} 个文件到磁盘...")
        for f in self.files.values():
            try:
                f.flush()
                os.fsync(f.fileno()) # 强制写入磁盘
            except: pass
            f.close()

# ==============================================================================
# 3. NativeNvmlManager - 业务逻辑层
# ==============================================================================
class NativeNvmlManager:
    def __init__(self):
        self.nvml = NvmlNative()
        self.available = False
        self.device_count = 0
        if self.nvml.lib:
            try:
                self.nvml.init()
                self.device_count = self.nvml.device_get_count()
                self.available = True
                print(f"[DeviceAgent] ✅ Native NVML (ctypes) 初始化成功，检测到 {self.device_count} 个 GPU。")
            except Exception as e:
                print(f"[DeviceAgent] ❌ NVML Init Failed: {e}")

    def get_gpu_states(self):
        if not self.available: return {}
        gpu_map = {}
        for i in range(self.device_count):
            try:
                handle = self.nvml.device_get_handle_by_index(i)
                bus_id = self.nvml.device_get_pci_info(handle)
                
                try: util = self.nvml.device_get_utilization_rates(handle)
                except: util = 0.0
                
                try: power = self.nvml.device_get_power_usage(handle) / 1000.0
                except: power = 0.0
                
                gpu_map[bus_id] = {'id': i, 'util': util, 'power': power}
            except: continue
        return gpu_map

    def get_active_processes(self):
        if not self.available: return []
        procs = []
        for i in range(self.device_count):
            try:
                handle = self.nvml.device_get_handle_by_index(i)
                bus_id = self.nvml.device_get_pci_info(handle)
                
                raw_procs = self.nvml.device_get_compute_running_processes(handle)
                for p in raw_procs:
                    mem_mib = p['usedGpuMemory'] / 1024.0 / 1024.0
                    procs.append({'pid': p['pid'], 'bus_id': bus_id, 'mem': mem_mib})
            except: continue
        return procs

    def shutdown(self):
        if self.available:
            self.nvml.shutdown()

# ==============================================================================
# 4. DeviceAgent 主逻辑
# ==============================================================================
class DeviceAgent:
    def __init__(self, args):
        self.timeseries_root = args.timeseries_root
        self.target_name = args.target_name
        self.interval = args.interval
        self.node_name = socket.gethostname()
        self.writer = AsyncWriter(self.timeseries_root, self.node_name)
        self.nv_manager = NativeNvmlManager()

    def _check_pid_name(self, pid, target_name):
        try:
            with open(f'/proc/{pid}/cmdline', 'rb') as f:
                cmd = f.read().decode(errors='ignore').replace('\0', ' ').strip()
                if "device_agent" in cmd: return False 
                if target_name.lower() in cmd.lower(): return True
        except: pass
        return False

    def run(self):
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        print(f"[DeviceAgent] 启动! 节点: {self.node_name}, 目标: '{self.target_name}' (No-Dep Version)")
        my_pid = os.getpid()

        try:
            while running:
                start = time.time()
                ts = datetime.datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
                gpu_states = self.nv_manager.get_gpu_states()
                active_procs = self.nv_manager.get_active_processes()

                if active_procs and gpu_states:
                    for p in active_procs:
                        pid = p['pid']
                        if pid == my_pid: continue
                        
                        if self._check_pid_name(pid, self.target_name):
                            bus_id = p['bus_id']
                            if bus_id in gpu_states:
                                gpu_info = gpu_states[bus_id]
                                gpu_idx = gpu_info['id']
                                
                                val = f"{p['mem']:.1f},{gpu_info['util']},{gpu_info['power']:.1f}"
                                self.writer.write(pid, f"gpu{gpu_idx}.csv", ts, val, "Timestamp,Memory(MiB),Util(%),Power(W)")

                elapsed = time.time() - start
                time.sleep(max(0.0, self.interval - elapsed))
        finally:
            self.nv_manager.shutdown()
            # 退出时会调用 AsyncWriter.close()，触发 fsync
            self.writer.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--timeseries_root", required=True)
    p.add_argument("--target_name", required=True)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--gpus_per_proc", type=int, default=1) 
    args = p.parse_args()

    agent = DeviceAgent(args)
    agent.run()
