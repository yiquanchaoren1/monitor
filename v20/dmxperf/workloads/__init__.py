# dmxperf/workloads/__init__.py
# -*- coding: utf-8 -*-
from .dmx_solver import DmxSolverWorkload
# from .gromacs import GromacsWorkload (未来可扩展)

class WorkloadFactory:
    @staticmethod
    def create(job_config, global_config, run_root, dry_run=False):
        """
        根据 Job 配置特征，自动决定使用哪个 Workload 类。
        """
        # 判定逻辑：如果包含 'input' 字段且文件名以 .json 结尾，默认为 DmxSolver
        if "input" in job_config:
            return DmxSolverWorkload(job_config, global_config, run_root, dry_run)
        
        # 默认回退 (或者抛出异常)
        print("⚠️ 未识别的任务类型，默认使用 DmxSolverWorkload")
        return DmxSolverWorkload(job_config, global_config, run_root, dry_run)
