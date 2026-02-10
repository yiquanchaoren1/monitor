# -*- coding: utf-8 -*-

from .cases.ivc import IvcWorkload

class WorkloadFactory:
    @staticmethod
    def create(job_config, global_config, run_root, dry_run=False):
        # 获取算例类型，默认为 'ivc' 以兼容旧配置
        case_type = job_config.get('case_type', 'ivc').lower()
        
        if case_type == 'ivc':
            return IvcWorkload(job_config, global_config, run_root, dry_run)
            
        # 未来如果加新算例，在这里扩展 elif case_type == 'xxx': ...
            
        else:
            raise ValueError(f"❌ 未知算例类型 (case_type): {case_type}")
