# dmxperf/workloads/dmx_base.py
# -*- coding: utf-8 -*-
import os
import shutil
import socket
from .base import BaseWorkload, WorkloadContext

class DmxCommonWorkload(BaseWorkload):
    """
    DMX 求解器通用基类。
    增加特性：自动在所有计算节点上创建结果目录 (mkdir -p)。
    """
    def prepare(self) -> WorkloadContext:
        ctx = WorkloadContext()
        ctx.case_name = self.job.get('case_name')
        
        # 1. 本地日志目录准备
        exp_root = os.path.join(self.run_root, "metrics", ctx.case_name)
        ctx.log_file = os.path.join(self.run_root, "logs", f"{ctx.case_name}_solver.log")
        
        ctx.paths = {
            "root": exp_root,
            "timeseries": os.path.join(exp_root, "TimeSeries"),
            "events": os.path.join(exp_root, "Events")
        }

        if not self.dry_run:
            for p in ctx.paths.values():
                os.makedirs(p, exist_ok=True)

        # 2. 调用子类生成具体的 Run Config
        run_config_path = os.path.join(exp_root, f"run_config_{ctx.case_name}.json")
        effective_nodes, final_res_dir = self.generate_case_config(run_config_path)
        
        ctx.effective_nodes = effective_nodes
        ctx.result_dir = final_res_dir

        # === [核心逻辑] 远程创建目录 ===
        if not self.dry_run and final_res_dir:
            self._ensure_remote_dirs(effective_nodes, final_res_dir)

        # 3. 处理求解器二进制软链接
        solver_bin = self.job.get('solver_bin') or self.global_cfg.get('default_solver_bin')
        if not solver_bin:
             raise ValueError(f"❌ Job '{ctx.case_name}' 缺少 'solver_bin' 配置！")
        
        exe_cmd = self._setup_symlink(solver_bin, ctx.case_name)

        # 4. 构造启动命令
        ctx.cmd_string = f"unset CUDA_VISIBLE_DEVICES && {exe_cmd} {run_config_path} > {ctx.log_file} 2>&1"
        
        # 5. 环境变量
        ctx.env = os.environ.copy()
        if "CUDA_VISIBLE_DEVICES" in ctx.env:
            del ctx.env["CUDA_VISIBLE_DEVICES"]
        ctx.env["OMP_NUM_THREADS"] = "1"

        return ctx

    def cleanup(self):
        case_name = self.job.get('case_name')
        symlink_path = os.path.join(os.getcwd(), case_name)
        if os.path.islink(symlink_path):
            try: os.unlink(symlink_path)
            except: pass

    def generate_case_config(self, output_path):
        """[抽象方法] 子类必须实现具体的 JSON 生成逻辑"""
        raise NotImplementedError

    def _setup_symlink(self, solver_bin, case_name):
        abs_bin = os.path.abspath(solver_bin)
        solver_dir = os.path.dirname(abs_bin)
        exec_name = os.path.basename(abs_bin)
        symlink_path = os.path.join(os.getcwd(), case_name)
        
        if self.dry_run: return f"./{case_name}/{exec_name}"

        if os.path.exists(symlink_path) or os.path.islink(symlink_path):
            try:
                if os.path.islink(symlink_path): os.unlink(symlink_path)
                elif os.path.isdir(symlink_path): shutil.rmtree(symlink_path)
                else: os.remove(symlink_path)
            except: pass
            
        try:
            os.symlink(solver_dir, symlink_path)
            return f"./{case_name}/{exec_name}"
        except Exception as e:
            print(f"⚠️ 软链接创建失败，使用绝对路径: {e}")
            return abs_bin

    def _ensure_remote_dirs(self, nodes, path):
        """
        遍历所有计算节点，通过 SSH 执行 mkdir -p
        解决本地盘路径 (/data2/...) 在远程节点不存在的问题
        """
        if not nodes or not path: return
        
        # 去重，每个节点只执行一次
        unique_nodes = set(nodes)
        local_hostname = socket.gethostname()

        print(f"🌍 [Env Check] 正在检查并创建输出目录: {path}")

        for node in unique_nodes:
            # 1.如果是本机
            if node == local_hostname or node in ['localhost', '127.0.0.1']:
                try:
                    os.makedirs(path, exist_ok=True)
                except Exception as e:
                    print(f"⚠️  本机创建目录失败: {e}")
            # 2.如果是远程节点
            else:
                try:
                    # 使用 SSH 远程创建 (mkdir -p 保证如果已存在不会报错，父目录不存在会自动创建)
                    cmd = f"ssh {node} 'mkdir -p {path}'"
                    ret = os.system(cmd)
                    if ret != 0:
                        print(f"⚠️  节点 {node} 目录创建可能失败 (Exit Code: {ret})")
                except Exception as e:
                    print(f"❌ 远程连接节点 {node} 失败: {e}")