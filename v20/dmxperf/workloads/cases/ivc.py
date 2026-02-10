# dmxperf/workloads/cases/ivc.py
# -*- coding: utf-8 -*-
import json
import os
import socket
import datetime  # <--- [新增] 需要这个库来生成时间
from dmxperf.workloads.dmx_base import DmxCommonWorkload

class IvcWorkload(DmxCommonWorkload):
    """
    IVC 算例 - 时间戳增强版
    1. 自动在输出目录后追加时间戳 (保证每次运行目录唯一)。
    2. 基类会自动去远程节点创建这个新目录。
    """
    def generate_case_config(self, output_path):
        # 1. 读取模板
        input_file = self.job.get('input')
        input_dir = self.global_cfg.get('input_dir', './')
        template_path = os.path.join(input_dir, input_file)
        
        default_ret = (["localhost"], None)

        if not os.path.exists(template_path):
            if not self.dry_run: print(f"❌ 找不到模板: {template_path}")
            return default_ret

        data = {}
        try:
            with open(template_path, 'r', encoding='utf-8') as f: data = json.load(f)
        except:
            try:
                with open(template_path, 'r', encoding='gbk') as f: data = json.load(f)
            except:
                return default_ret

        # --- 解析节点布局 (保持不变) ---
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
                    final_dev_ids.append(list(range(0, g)))
                    final_bundles.append([1] * g)
        else:
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
                    final_dev_ids.append(list(range(0, target_gpu)))
                    final_bundles.append([1] * target_gpu)

        # --- [关键修改] 添加时间戳逻辑 ---
        final_res_dir = None
        if "control" in data:
            c = data["control"]
            c["case_name"] = self.job.get('case_name')
            
            # 1. 获取原始路径 (例如 /data2/hzb/soln)
            base_out = c.get("soln_output_dir", "./soln").rstrip('/')
            
            # 2. 生成时间戳 (例如 _20260209_143000)
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_dir = f"{base_out}_{ts}"
            
            if self.dry_run: unique_dir += "_DRYRUN"

            # 3. 更新 JSON 配置
            c["soln_output_dir"] = unique_dir
            final_res_dir = unique_dir  # 将这个新路径返回给基类，让它去 mkdir

            c["mpirun_host_name_list"] = final_hosts
            c["mpirun_host_nproc_list"] = final_nprocs
            c["hardware_device_id_list"] = final_dev_ids
            c["n_bundles_on_device"] = final_bundles

        # --- 写入文件 ---
        if not self.dry_run and output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        
        return summary_nodes, final_res_dir