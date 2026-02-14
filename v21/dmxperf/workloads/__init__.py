# dmxperf/workloads/__init__.py
# -*- coding: utf-8 -*-

from .cases.ivc import IvcWorkload
from .hardware import HardwareWorkload

class WorkloadFactory:
    @staticmethod
    def create(job_config, global_config, run_root, dry_run=False):
        # 获取算例类型
        case_type = job_config.get('case_type', 'ivc').lower()
        
        # 1. 求解器任务 (IVC)
        if case_type == 'ivc':
            return IvcWorkload(job_config, global_config, run_root, dry_run)
            
        # 2. [修改] 统一的硬件验收任务 (Hardware)
        # 只要 case_type 是 'hardware'，就交给 HardwareWorkload 处理
        # (为了兼容性，保留旧的枚举也可以，但建议统一)
        elif case_type == 'hardware':
            return HardwareWorkload(job_config, global_config, run_root, dry_run)
            
        else:
            raise ValueError(f"❌ 未知算例类型 (case_type): {case_type}")