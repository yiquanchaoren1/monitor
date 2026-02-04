# dmxperf/workloads/dmx_solver.py
# -*- coding: utf-8 -*-
import os
import json
import socket
import datetime
import shutil
from .base import BaseWorkload, WorkloadContext

class DmxSolverWorkload(BaseWorkload):
    def prepare(self) -> WorkloadContext:
        ctx = WorkloadContext()
        ctx.case_name = self.job.get('case_name')
        
        # 1. 路径准备
        exp_root = os.path.join(self.run_root, "metrics", ctx.case_name)
        ctx.log_file = os.path.join(self.run_root, "logs", f"{ctx.case_name}_solver.log")
        
        # [修复] 填充 paths 字典
        ctx.paths = {
            "root": exp_root,
            "timeseries": os.path.join(exp_root, "TimeSeries"),
            "events": os.path.join(exp_root, "Events")
        }

        # 创建目录结构
        if not self.dry_run:
            # 遍历 paths 创建目录
            for p in ctx.paths.values():
                os.makedirs(p, exist_ok=True)

        # 2. 生成 dmxsol 专用配置文件 (run_config_xxx.json)
        run_config_path = os.path.join(exp_root, f"run_config_{ctx.case_name}.json")
        
        # 调用核心解析逻辑
        effective_nodes, _, final_res_dir = self._generate_json_config(run_config_path)
        
        ctx.effective_nodes = effective_nodes
        ctx.result_dir = final_res_dir
        
        # 3. 处理软链接 (Symlink)
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
        # 清理软链接
        case_name = self.job.get('case_name')
        symlink_path = os.path.join(os.getcwd(), case_name)
        if os.path.islink(symlink_path):
            try:
                os.unlink(symlink_path)
            except: pass

    def _setup_symlink(self, solver_bin, case_name):
        """处理求解器二进制的软链接"""
        abs_bin = os.path.abspath(solver_bin)
        solver_dir = os.path.dirname(abs_bin)
        exec_name = os.path.basename(abs_bin)
        symlink_path = os.path.join(os.getcwd(), case_name)
        
        if self.dry_run:
            return f"./{case_name}/{exec_name}"

        # 清理旧链接
        if os.path.exists(symlink_path) or os.path.islink(symlink_path):
            try:
                if os.path.islink(symlink_path): os.unlink(symlink_path)
                elif os.path.isdir(symlink_path): shutil.rmtree(symlink_path)
                else: os.remove(symlink_path)
            except: pass
            
        # 创建新链接
        try:
            os.symlink(solver_dir, symlink_path)
            return f"./{case_name}/{exec_name}"
        except Exception as e:
            print(f"⚠️ 软链接创建失败，使用绝对路径: {e}")
            return abs_bin

    def _generate_json_config(self, output_path):
        """从 input 模板读取 -> 修改节点/GPU绑定 -> 写入 run_config"""
        # 获取输入模板路径
        input_file = self.job.get('input')
        input_dir = self.global_cfg.get('input_dir', './')
        template_path = os.path.join(input_dir, input_file)
        
        default_ret = (["localhost"], 1, None)

        if not os.path.exists(template_path):
            if not self.dry_run: print(f"❌ 找不到模板: {template_path}")
            return default_ret

        # 读取 JSON (带 GBK 容错)
        data = {}
        try:
            with open(template_path, 'r', encoding='utf-8') as f: data = json.load(f)
        except:
            try:
                with open(template_path, 'r', encoding='gbk') as f: data = json.load(f)
            except:
                if not self.dry_run: return default_ret

        # --- 解析逻辑 ---
        orig_nodes = ["localhost"]
        orig_proc = 1
        orig_gpu = 1
        
        if "control" in data:
            c = data["control"]
            if c.get("mpirun_host_name_list"): orig_nodes = c["mpirun_host_name_list"]
            if c.get("mpirun_host_nproc_list"): orig_proc = int(c["mpirun_host_nproc_list"][0])
            if c.get("hardware_device_id_list"):
                ids = c["hardware_device_id_list"][0]
                if isinstance(ids, list): orig_gpu = len(ids)

        final_hosts = []
        final_nprocs = []
        final_dev_ids = []
        final_bundles = []
        summary_nodes = []

        if "node_layout" in self.job:
            # 异构配置
            for idx, item in enumerate(self.job["node_layout"]):
                h = item.get("hostname", "default")
                if str(h).lower() in ["default", ""]:
                    h = orig_nodes[idx] if idx < len(orig_nodes) else "localhost"
                
                if h in ["localhost", "127.0.0.1"]: h = socket.gethostname()
                final_hosts.append(h)
                summary_nodes.append(h)

                p = item.get("proc_per_node", "default")
                if str(p).lower() == "default": p = orig_proc
                else: p = int(p)
                final_nprocs.append(p)

                g = item.get("gpus_per_proc", "default")
                if str(g).lower() == "default": g = orig_gpu
                else: g = int(g)

                for i in range(p):
                    start = i * g
                    ids = list(range(start, start + g))
                    final_dev_ids.append(ids)
                    final_bundles.append([1] * len(ids))
        else:
            # 同构配置
            target_nodes = self.job.get('nodes', orig_nodes)
            if isinstance(target_nodes, str) and target_nodes.lower() != 'default':
                target_nodes = target_nodes.split(',')
            
            norm_nodes = []
            for n in target_nodes:
                if n.strip() in ["localhost", "127.0.0.1"]: norm_nodes.append(socket.gethostname())
                else: norm_nodes.append(n.strip())
            
            target_proc = int(self.job.get('proc_per_node', orig_proc))
            target_gpu = int(self.job.get('gpus_per_proc', orig_gpu))

            final_hosts = norm_nodes
            summary_nodes = norm_nodes
            final_nprocs = [target_proc] * len(norm_nodes)

            for _ in norm_nodes:
                for i in range(target_proc):
                    start = i * target_gpu
                    ids = list(range(start, start + target_gpu))
                    final_dev_ids.append(ids)
                    final_bundles.append([1] * len(ids))

        final_res_dir = None
        if "control" in data:
            c = data["control"]
            c["case_name"] = self.job.get('case_name')
            
            base_out = c.get("soln_output_dir", "./soln").rstrip('/')
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_dir = f"{base_out}_{ts}"
            if self.dry_run: unique_dir += "_DRYRUN"
            
            c["soln_output_dir"] = unique_dir
            final_res_dir = os.path.abspath(unique_dir)

            c["mpirun_host_name_list"] = final_hosts
            c["mpirun_host_nproc_list"] = final_nprocs
            c["hardware_device_id_list"] = final_dev_ids
            c["n_bundles_on_device"] = final_bundles

        if not self.dry_run and output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            if final_res_dir:
                try: os.makedirs(final_res_dir, exist_ok=True)
                except: pass

        return summary_nodes, 0, final_res_dir
