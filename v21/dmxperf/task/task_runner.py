# dmxperf/task/task_runner.py
# -*- coding: utf-8 -*-
import os
import time
import subprocess
import socket
from dmxperf.workloads import WorkloadFactory
from dmxperf.infra.executor import ExecutorFactory

class TaskRunner:
    def __init__(self, global_config, run_root, dry_run=False):
        self.global_config = global_config
        self.run_root = run_root
        self.dry_run = dry_run
        # 当前加载的 workload 实例
        self.current_workload = None 

    def prepare(self, job):
        """
        准备阶段：委托给具体的 Workload 处理
        """
        # 1. 工厂模式创建 workload
        self.current_workload = WorkloadFactory.create(
            job, self.global_config, self.run_root, self.dry_run
        )
        
        # 2. 执行 workload 特有的准备逻辑 (解析 JSON, 软链等)
        ctx = self.current_workload.prepare()
        
        return ctx

    def run(self, ctx):
        """
        运行阶段：执行命令 + 资源清理
        """
        print(f"      [Task] 启动计算进程 (Case: {ctx.case_name})...")
        
        # 1. 执行核心计算任务 (通常在当前节点/Head Node 启动)
        try:
            if not self.dry_run:
                # 使用 subprocess 直接启动 (保留对 IO 的控制)
                process = subprocess.Popen(
                    ctx.cmd_string, 
                    shell=True, 
                    executable='/bin/bash',
                    env=ctx.env,
                    cwd=os.getcwd() 
                )
                process.wait() # 阻塞等待任务完成
                
                if process.returncode != 0:
                    print(f"⚠️ [Task] 进程非正常退出，返回码: {process.returncode}")
            else:
                print(f"      [DryRun] CMD: {ctx.cmd_string}")
                time.sleep(1) # 模拟运行

        except Exception as e:
            print(f"❌ [Task] 启动失败: {e}")
            raise e
            
        finally:
            # 2. 任务后清理 (Workload 级)
            if self.current_workload:
                self.current_workload.cleanup()
            
            # 3. 结果目录清理 (如果配置了 clean_solver_results)
            self._handle_result_cleanup(ctx)

    def _handle_result_cleanup(self, ctx):
        # 检查是否开启清理
        should_clean = self.global_config.get('clean_solver_results', False)
        # Job 级配置覆盖 Global
        if 'clean_solver_results' in self.current_workload.job:
            should_clean = self.current_workload.job['clean_solver_results']
            
        if not should_clean or not ctx.result_dir:
            return

        if not self.dry_run:
            #print(f"      [Cleanup] 等待 2 秒释放文件锁...")
            time.sleep(2)
            
            # 遍历所有涉及的节点进行删除
            # 使用 Executor 屏蔽本地/远程差异
            unique_nodes = list(set(ctx.effective_nodes))
            #print(f"      [Cleanup] 清理结果目录: {ctx.result_dir}")
            
            rm_cmd = f"rm -rf {ctx.result_dir}"
            
            for node in unique_nodes:
                executor = ExecutorFactory.create(node, self.dry_run)
                executor.run(rm_cmd)