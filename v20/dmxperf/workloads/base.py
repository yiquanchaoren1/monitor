# dmxperf/workloads/base.py
# -*- coding: utf-8 -*-

class WorkloadContext:
    """这是一个数据传输对象(DTO)，用于在 Prepare 阶段返回信息"""
    def __init__(self):
        self.cmd_string = ""        # 最终的启动命令
        self.env = {}               # 需要设置的环境变量
        self.effective_nodes = []   # 实际运行的节点列表 (用于监控)
        self.result_dir = None      # 结果输出目录 (用于清理)
        self.case_name = ""         # 任务名
        self.log_file = ""          # 日志路径
        self.paths = {}             # [修复] 新增 paths 字典，用于存储 path

class BaseWorkload:
    def __init__(self, job_config, global_config, run_root, dry_run=False):
        self.job = job_config
        self.global_cfg = global_config
        self.run_root = run_root
        self.dry_run = dry_run

    def prepare(self) -> WorkloadContext:
        """
        准备阶段：解析配置、生成文件、计算节点。
        返回 WorkloadContext 对象。
        """
        raise NotImplementedError

    def cleanup(self):
        """
        清理阶段：删除临时文件、软链接等。
        """
        pass
