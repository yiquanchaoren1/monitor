# dmxperf/agents/gpu_hw_agent.py
# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import shutil
import time

# === 资源路径获取 ===
def get_resource_root(res_type):
    """
    根据类型获取资源根目录
    """
    if hasattr(sys, '_MEIPASS'):
        base = os.path.join(sys._MEIPASS, 'src', res_type)
    else:
        base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', res_type)
    
    if not os.path.exists(base):
        raise FileNotFoundError(f"❌ 资源目录缺失: {base}")
    return base

def run_cmd(cmd_list, log_file=None, cwd=None):
    """通用执行器"""
    print(f"   ⚡ EXEC: {' '.join(cmd_list)}")
    try:
        # 使用 check_call 直接将输出打印到屏幕 (stdout)
        subprocess.check_call(cmd_list, stdout=sys.stdout, stderr=subprocess.STDOUT, cwd=cwd)
    except Exception as e:
        print(f"❌ 执行异常: {e}")
        if log_file:
            with open(log_file, 'a') as f: f.write(f"\n❌ FAILED: {e}\n")

# === [辅助] 自动探测显卡架构 ===
def get_gpu_arch():
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=compute_cap", "--format=csv,noheader,nounits"], 
            encoding='utf-8'
        )
        return out.strip().split('\n')[0].replace('.', '')
    except:
        return None

# === 核心逻辑: 现场编译并运行 ===
def compile_and_run(source_file, output_name, compile_flags=[], run_args=[], log_file=None):
    print(f"\n🚀 [Compile Mode] 准备构建: {output_name}")
    try:
        # 1. 检查 nvcc
        try:
            subprocess.check_output(["nvcc", "--version"], stderr=subprocess.STDOUT)
        except:
            print("❌ 错误: 未找到 'nvcc'，无法进行现场编译。")
            return

        # 2. 准备环境
        src_root = get_resource_root('cpp')
        work_dir = f"tmp_build_{output_name}_{int(time.time())}"
        
        if os.path.exists(work_dir): shutil.rmtree(work_dir)
        shutil.copytree(src_root, work_dir)
        
        # === 特殊处理: gpu_burn 机器码生成 ===
        if output_name == "gpu_burn":
            print("🔨 [Pre-build] 生成 Kernel (适配当前驱动)...")
            arch = get_gpu_arch()
            arch_flag = f"-arch=sm_{arch}" if arch else "-arch=compute_75"
            run_cmd(["nvcc", "-cubin", "compare.cu", "-o", "compare.ptx", arch_flag], log_file, cwd=work_dir)

        # 3. 编译主程序
        # -I common 确保 topologyQuery 和 deviceQuery 都能找到头文件
        cmd_build = ["nvcc", source_file, "-I", "common", "-o", output_name, "-Wno-deprecated-gpu-targets"] + compile_flags
        
        print(f"🔨 正在编译主程序...")
        run_cmd(cmd_build, log_file, cwd=work_dir)
        
        # 4. 运行
        bin_path = os.path.join(work_dir, output_name)
        if os.path.exists(bin_path):
            print(f"✅ 编译成功，开始运行...")
            run_cmd([f"./{output_name}"] + run_args, log_file, cwd=work_dir)
        else:
            print("❌ 编译失败，未生成二进制文件。")
            
        # 5. 清理
        shutil.rmtree(work_dir)
        
    except Exception as e:
        print(f"❌ 流程异常: {e}")

# === 功能入口 ===

def run_hw_check():
    """
    [拓扑模式] 依次执行:
    1. nvidia-smi topo (系统级)
    2. deviceQuery (CUDA 属性)
    3. topologyQuery (NUMA/Affinity 深度信息)
    """
    log = f"hw_check_{int(time.time())}.log"
    print(f"📝 Log: {log}")
    
    # Step 1
    print("\n--- [Step 1] System Topology Matrix (nvidia-smi) ---")
    run_cmd(["nvidia-smi", "topo", "-m"], log)
    
    # Step 2
    print("\n--- [Step 2] Device Query (Live Compile) ---")
    compile_and_run("deviceQuery.cu", "deviceQuery", [], log_file=log)

    # Step 3 (新增)
    print("\n--- [Step 3] Topology Query (Live Compile) ---")
    compile_and_run("topologyQuery.cu", "topologyQuery", [], log_file=log)

def run_bandwidth_test():
    log = f"bandwidth_{int(time.time())}.log"
    print(f"📝 Log: {log}")
    try:
        bin_path = os.path.join(get_resource_root('bin'), 'bandwidthTest')
        run_cmd([bin_path, "--memory=pinned", "--mode=quick"], log)
    except Exception as e: print(f"❌ {e}")

def run_bus_grind():
    log = f"bus_grind_{int(time.time())}.log"
    print(f"📝 Log: {log}")
    try:
        bin_path = os.path.join(get_resource_root('bin'), 'busGrind')
        run_cmd([bin_path], log)
    except Exception as e: print(f"❌ {e}")

def run_gpu_burn(duration):
    log = f"gpu_burn_{int(time.time())}.log"
    print(f"📝 Log: {log}")
    compile_and_run(
        "gpu_burn-drv.cpp", 
        "gpu_burn", 
        ["-lcuda", "-lcublas", "-lcudart"], 
        [str(duration)], 
        log
    )

def show_tool_help(tool_name):
    mapping = {'bandwidth': 'bandwidthTest', 'bus': 'busGrind'}
    target = mapping.get(tool_name)
    if target:
        try:
            bin_path = os.path.join(get_resource_root('bin'), target)
            subprocess.run([bin_path, "--help"])
        except: pass