# dmxperf/infra/executor.py
# -*- coding: utf-8 -*-
import subprocess
import socket
import os

class BaseExecutor:
    def __init__(self, node, dry_run=False):
        self.node = node
        self.dry_run = dry_run
        self.is_local = self._check_is_local(node)

    def _check_is_local(self, node):
        if node in ["localhost", "127.0.0.1"]:
            return True
        try:
            return node == socket.gethostname()
        except:
            return False

    def run(self, cmd, env=None, timeout=None, ignore_errors=None):
        """
        阻塞执行命令
        :param ignore_errors: 一个包含忽略错误码的列表，例如 [-9, 137]
        """
        raise NotImplementedError

    def exec_background(self, cmd, log_file="/dev/null"):
        """后台非阻塞执行"""
        raise NotImplementedError

class SSHExecutor(BaseExecutor):
    def run(self, cmd, env=None, timeout=None, ignore_errors=None):
        """
        在远程或本地执行命令并等待结果。
        """
        # 如果是 Dry Run，只打印
        if self.dry_run:
            # print(f"      [DryRun] Exec on {self.node}: {cmd}")
            return 0

        final_cmd = cmd
        if not self.is_local:
            # 简单的 SSH 封装
            final_cmd = f"ssh -o StrictHostKeyChecking=no {self.node} '{cmd}'"

        try:
            # 使用 shell=True 允许使用管道符和逻辑符 (&&, |)
            res = subprocess.run(
                final_cmd, 
                shell=True, 
                env=env, 
                timeout=timeout,
                executable='/bin/bash',
                stdout=subprocess.DEVNULL, # 默认静默，除非报错
                stderr=subprocess.PIPE
            )
            
            # === [关键新增] 错误码处理逻辑 ===
            ret = res.returncode
            
            # 如果提供了忽略列表，且返回码在列表中，视为成功 (返回 0)
            if ignore_errors and ret in ignore_errors:
                return 0
            
            # 只有当非零且不在忽略列表中时，才打印警告
            if ret != 0:
                print(f"⚠️ [Executor] {self.node} 命令异常 (Code: {ret})")
                # print(f"   Err: {res.stderr.decode(errors='ignore').strip()}")
            
            return ret

        except Exception as e:
            print(f"❌ [Executor] {self.node} 执行异常: {e}")
            return -1

    def exec_background(self, cmd, log_file="/dev/null"):
        """
        启动后台进程 (nohup ... &)
        """
        bg_cmd = f"{cmd} > {log_file} 2>&1 &"
        
        if self.dry_run:
            return

        final_cmd = bg_cmd
        if not self.is_local:
            final_cmd = f"ssh -o StrictHostKeyChecking=no {self.node} '{bg_cmd}'"

        try:
            subprocess.Popen(final_cmd, shell=True, executable='/bin/bash')
        except Exception as e:
            print(f"❌ [Executor] {self.node} 后台启动失败: {e}")

class ExecutorFactory:
    @staticmethod
    def create(node, dry_run=False):
        return SSHExecutor(node, dry_run)