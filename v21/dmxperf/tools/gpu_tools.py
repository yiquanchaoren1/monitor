# dmxperf/tools/gpu_tools.py
# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import shutil
import time
import re  # <--- 新增正则模块

# === ANSI 颜色去除正则 ===
ansi_escape = re.compile(r'\x1b\[[0-9;]*[mGK]')

# === 资源路径获取 ===
def get_resource_root(res_type):
    if hasattr(sys, '_MEIPASS'):
        base = os.path.join(sys._MEIPASS, 'src', res_type)
    else:
        base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', res_type)
    if not os.path.exists(base):
        raise FileNotFoundError(f"❌ 资源目录缺失: {base}")
    return base

# === [核心修改] 增强版 run_cmd: 实时读取输出并去除颜色代码 ===
def run_cmd(cmd_list, log_file=None, cwd=None):
    print(f"   ⚡ EXEC: {' '.join(cmd_list)}")
    sys.stdout.flush()
    
    try:
        # 使用 Popen 接管 stdout, 实现实时处理
        process = subprocess.Popen(
            cmd_list, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, # 将 stderr 合并到 stdout
            cwd=cwd,
            text=True,       # 以文本模式读取
            bufsize=1,       # 行缓冲
            encoding='utf-8', 
            errors='replace' # 防止编码报错
        )

        # 实时逐行读取
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            
            if line:
                # === 关键：用正则去除 ANSI 颜色码 ===
                clean_line = ansi_escape.sub('', line)
                
                # 打印到屏幕 (HardwareWorkload 会捕获这个输出到日志文件)
                print(clean_line, end='') 
                sys.stdout.flush()

        # 等待进程结束并检查返回码
        rc = process.poll()
        if rc != 0:
            print(f"❌ 进程异常退出，返回码: {rc}")
            # 不抛出异常，以免打断后续任务，只打印错误

    except Exception as e:
        print(f"❌ 执行异常: {e}")

# === 自动探测显卡架构 ===
def get_gpu_arch():
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=compute_cap", "--format=csv,noheader,nounits"], 
            encoding='utf-8'
        )
        return out.strip().split('\n')[0].replace('.', '')
    except:
        return "70" # Default V100

# === 现场编译并运行 ===
def compile_and_run(src_file, bin_name, compile_args, run_args, log_file=None):
    print(f"🚀 [Compile Mode] 准备构建: {bin_name}")
    
    src_path = os.path.join(get_resource_root('cpp'), src_file)
    include_path = os.path.join(get_resource_root('cpp'), 'common')
    
    # 检查 nvcc
    if shutil.which('nvcc') is None:
        print("❌ 错误: 未找到 'nvcc'，无法进行现场编译。")
        return

    # 1. 编译
    arch = get_gpu_arch()
    arch_flag = f"-arch=sm_{arch}"
    
    # 构建命令: nvcc source.cpp -I include -o bin ...
    build_cmd = ["nvcc", src_path, "-I", include_path, "-o", bin_name, arch_flag] + compile_args
    
    print("🔨 正在编译主程序...")
    run_cmd(build_cmd, log_file)
    
    if not os.path.exists(bin_name):
        print("❌ 编译失败，未生成二进制文件。")
        return

    # 2. 运行
    print("✅ 编译成功，开始运行...")
    
    exec_cmd = [f"./{bin_name}"] + run_args
    run_cmd(exec_cmd, log_file)
    
    # 3. 清理二进制 (可选)
    if os.path.exists(bin_name):
        os.remove(bin_name)

# ==============================================================================
# 各工具封装
# ==============================================================================

def run_hw_check():
    print("🛠️  System Topology & Hardware Attributes")
    compile_and_run("deviceQuery.cu", "deviceQuery", [], [])
    compile_and_run("topologyQuery.cu", "topologyQuery", [], [])
    try:
        subprocess.call(["nvidia-smi", "topo", "-m"])
    except: pass

def run_bandwidth_test(extra_args=None):
    try:
        bin_path = os.path.join(get_resource_root('bin'), 'bandwidthTest')
        cmd = [bin_path]
        if extra_args: cmd += extra_args
        run_cmd(cmd)
    except:
        print("⚠️ 未找到预编译 bandwidthTest，跳过。")

def show_tool_help(tool_name):
    print(f"ℹ️  Showing help for {tool_name}...")
    
    # 1. 定义映射关系: 参数名 -> (二进制文件名, 帮助参数)
    tool_map = {
        'bus': ('busGrind', '-h'),          # busGrind 只支持 -h
        'bandwidth': ('bandwidthTest', '--help') # bandwidthTest 支持 --help
    }
    
    # 获取配置
    config = tool_map.get(tool_name)
    
    if not config:
        print(f"❌ 未知工具名称: {tool_name}")
        return

    bin_name, help_flag = config

    try:
        # 2. 获取二进制文件的绝对路径
        bin_path = os.path.join(get_resource_root('bin'), bin_name)
        
        # 3. 检查文件是否存在
        if not os.path.exists(bin_path):
            print(f"❌ 未找到二进制文件: {bin_path}")
            return

        # 4. 执行帮助命令
        # run_cmd 已经封装了 stdout 的实时打印
        run_cmd([bin_path, help_flag])
        
    except Exception as e:
        print(f"❌ 获取帮助失败: {e}")

def run_bus_grind(extra_args=None):
    try:
        bin_path = os.path.join(get_resource_root('bin'), 'busGrind')
        cmd = [bin_path]
        if extra_args: cmd += extra_args
        run_cmd(cmd)
    except Exception as e: print(f"❌ {e}")

def run_gpu_burn(duration):
    print("🔥 [GPU Burn] 正在准备压测环境...")
    
    # 1. 准备路径
    src_root = get_resource_root('cpp')
    drv_src = "gpu_burn-drv.cpp"
    kernel_src = "compare.cu"
    ptx_name = "compare.ptx"
    bin_name = "gpu_burn"
    
    # 2. 检查 nvcc
    if shutil.which('nvcc') is None:
        print("❌ 错误: 未找到 'nvcc'，无法编译压测内核。")
        return

    # 3. 编译 CUDA 内核 (compare.cu -> compare.ptx)
    # gpu_burn 需要这个 .ptx 文件存在于运行目录下才能工作
    print("🔨 [1/2] 正在编译 CUDA 内核 (compare.ptx)...")
    
    arch = get_gpu_arch()
    arch_flag = f"-arch=compute_{arch}" # 注意:生成 PTX 通常用 compute_xx
    code_flag = f"-code=sm_{arch}"
    
    ptx_cmd = [
        "nvcc", 
        os.path.join(src_root, kernel_src),
        "-ptx", 
        "-o", ptx_name,
        arch_flag, code_flag
    ]
    run_cmd(ptx_cmd)
    
    if not os.path.exists(ptx_name):
        print("❌ 内核编译失败，找不到 compare.ptx")
        return

    # 4. 编译主程序 (gpu_burn-drv.cpp -> gpu_burn)
    print("🔨 [2/2] 正在编译主程序...")
    compile_flags = ["-lcuda", "-lcublas", "-lcudart", "-lstdc++"]
    
    compile_and_run(
        drv_src, 
        bin_name, 
        compile_flags, 
        [str(duration)],
        # 注意：这里实际上 compile_and_run 会再次编译一遍主程序并运行
        # 只要保证 compare.ptx 在当前目录下即可
    )
    
    # 5. 清理临时文件 (可选)
    # if os.path.exists(ptx_name): os.remove(ptx_name)

def run_gemm_test(extra_args=None):
    run_args = extra_args if extra_args else []
    compile_flags = ["-Wno-deprecated-gpu-targets", "-lcublas", "-lcudart", "-Xcompiler", "-fopenmp"]
    
    compile_and_run(
        "multi_gpu_gemm.cpp", 
        "multi_gpu_gemm", 
        compile_flags, 
        run_args
    )