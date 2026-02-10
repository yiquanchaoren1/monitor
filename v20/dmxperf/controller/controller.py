# dmxperf/controller/controller.py
# -*- coding: utf-8 -*-
import time
import os
import datetime
import json
import sys
import traceback

from dmxperf.task.task_runner import TaskRunner
from dmxperf.monitor.manager import MonitorManager
from dmxperf.collector.data_collector import DataCollector 
from dmxperf.collector.walltime_collector import WalltimeParser
from dmxperf.analysis.reporter import Reporter 
from dmxperf.analysis.time import TimeAnalyzer
from dmxperf.analysis.plotter import Plotter
from dmxperf.analysis.network_plotter import NetworkPlotter # 新增


class PerfController:
    def __init__(self, config_input, dry_run=False):
        if isinstance(config_input, str):
            print(f"📂 [Config] Loaded: {config_input}")
            self.config = self._load_config_from_file(config_input)
        else:
            self.config = config_input

        self.dry_run = dry_run
        self.global_cfg = self.config.get('global', {})
        
        target_dir = self.global_cfg.get('output_dir', 'perf_runs').strip() or "perf_runs"
        self.run_root = self._create_run_root(target_dir)
        
        # === [修改点 1] 在这里打印全局配置 ===
        print(f"   > Global Output: {target_dir}")
        print(f"   > Loop: {self.global_cfg.get('loop', 1)}")
        print(f"   > Interval: {self.global_cfg.get('interval', 1)}s")
        print(f"   > Clean Output: {self.global_cfg.get('clean_solver_results', False)}")

        self.monitor_manager = MonitorManager(self.run_root, dry_run)
        
        self.collector = DataCollector()
        self.time_analyzer = TimeAnalyzer() 
        self.reporter = Reporter(self.run_root, self.config)
        self.walltime_parser = WalltimeParser()
        self.plotter = Plotter(self.run_root)
        self.network_plotter = NetworkPlotter(self.run_root)
        
        self.all_job_meta = {} 
        self.involved_nodes = set()

    def run(self):
        jobs = self.config.get('jobs', [])
        global_loop = int(self.global_cfg.get('loop', 1))
        total_runs = sum(int(job.get('loop', global_loop)) for job in jobs)
        
        print(f"   > Jobs: {len(jobs)} | Total Runs: {total_runs}\n")
        
        current_run_idx = 1
        
        try:
            for job in jobs:
                loop_count = int(job.get('loop', global_loop))
                base_case_name = job.get('case_name', 'Unknown')
                
                if 'node_layout' in job:
                    for layout in job['node_layout']:
                        if 'hostname' in layout: self.involved_nodes.add(layout['hostname'])

                for k in range(loop_count):
                    run_job = job.copy()
                    if loop_count > 1:
                        run_job['case_name'] = f"{base_case_name}_{k+1}"
                    
                    self._run_single_job(run_job, current_run_idx)
                    
                    if current_run_idx < total_runs:
                        time.sleep(1)
                    current_run_idx += 1

            if not self.dry_run: 
                print("\n------------------------------------------------------------")
                print("📊 [Summary] Final Report Generation")
                print("------------------------------------------------------------")
                self.reporter.generate_summary(self.all_job_meta)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  User Interrupted (Ctrl+C)!")
        
        except Exception as e:
            print(f"\n❌ Execution Crashed: {e}")
            traceback.print_exc()

        finally:
            self._perform_final_cleanup()

    def _run_single_job(self, job, index):
        case_name = job.get('case_name', 'Unknown')
        print(f"🚀 [Controller] Job: {case_name} (Run {index})")
        
        runner = TaskRunner(self.global_cfg, self.run_root, self.dry_run)
        nodes_list = []

        try:
            # === 1. Prepare ===
            print(f"├── ⚙️  [Prepare]")
            
            ctx = runner.prepare(job)
            nodes_list = ctx.effective_nodes
            self.involved_nodes.update(nodes_list)
            
            print(f"│   ├── Workload: DmxSolver")
            print(f"│   └── Nodes: {nodes_list}")

            # === 2. Monitor Start ===
            print(f"├── 👁️  [Monitor] Start")
            interval = job.get('interval', self.global_cfg.get('interval', 1.0))
            gpus_per_proc = job.get('gpus_per_proc', 1) 
            if 'node_layout' in job:
                 gpus_per_proc = job['node_layout'][0].get('gpus_per_proc', 1)

            ts_root = ctx.paths.get('timeseries') if ctx.paths else None

            print(f"│   ├── Deploying Agents... ", end="", flush=True)
            self.monitor_manager.start(
                node_list=nodes_list, 
                target_name=case_name, 
                interval=interval, 
                gpus_per_proc=gpus_per_proc,
                timeseries_root=ts_root,
                silent=True
            )
            print("OK")
            
            if not self.dry_run: time.sleep(1)
            
            # === 3. Task Run ===
            print(f"├── 🏃 [TaskRunner] Execution")
            print(f"│   ├── Context: {case_name}")
            print(f"│   ├── Log: {ctx.log_file}")
            
            try:
                display_cmd = ctx.cmd_string
                if "unset CUDA_VISIBLE_DEVICES &&" in display_cmd:
                    display_cmd = display_cmd.replace("unset CUDA_VISIBLE_DEVICES &&", "").strip()
                print(f"│   ├── CMD: {display_cmd}")
            except:
                print(f"│   ├── CMD: {ctx.cmd_string}")
            
            print(f"│   ├── Status: Running...", end="", flush=True)
            runner.run(ctx) 
            print(" Done (Exit 0)")
            
            # === 4. Monitor Stop ===
            print(f"├── 🛑 [Monitor] Stop")
            print(f"│   ├── Sending Signal... ", end="", flush=True)
            self.monitor_manager.stop(nodes_list, silent=True)
            print("OK")

            # === 5. Analysis ===
            print(f"└── 📊 [Analysis] Post-Process")
            
            if not self.dry_run:
                #print(f"    ├── NFS Sync: Waiting 3s...", end="", flush=True)
                time.sleep(3)
                #print(" OK")
                
                if ctx.paths and ctx.paths.get('timeseries'):
                    self.collector.aggregate(ctx.paths['timeseries'])
                    print(f"    ├── Data: Aggregation success")
                
                walltime_csv = os.path.join(self.run_root, "metrics", case_name, "Events", "walltime.csv")
                parse_res = self.walltime_parser.parse(ctx.log_file, walltime_csv)
                time_stats = self.time_analyzer.analyze(walltime_csv)
                parse_res.update({'time_stats': time_stats})
                
                self.all_job_meta[case_name] = parse_res

                try:
                    rank0 = time_stats.get('ranks', {}).get('0', {})
                    init_t = rank0.get('init', 0)
                    solve_t = rank0.get('solve', 0)
                    print(f"    ├── Time: Init={init_t}s, Solve={solve_t}s")
                except:
                    pass
                
                if job.get('visualize'): 
                    self.plotter.plot_job(case_name, job['visualize'])
                    
                    try:
                        plots_str = ", ".join(job['visualize'])
                        print(f"    └── Plots: {plots_str}")
                    except:
                        print(f"    └── Plots: Charts generated")
                
                # === 修复后的代码 ===
                # 注意：这里的 if 要和上面的 if job.get('visualize') 对齐
                if job.get('visualize') and "network" in job.get('visualize', []):
                    # 注意：这里必须缩进！
                    print(f"    ├── Network Plots: Generating cluster heatmaps...")
                    self.network_plotter.plot_cluster_network(case_name)
            print("") 
                  
        except Exception as e:
            print(f"\n❌ Job Error: {e}")
            self.monitor_manager.stop(nodes_list, silent=True)
            traceback.print_exc()

    def _perform_final_cleanup(self):
        if not self.involved_nodes: self.involved_nodes = {"localhost"}
        print(f"\n🧹 [Final Cleanup] Cleaning agents on {list(self.involved_nodes)}... ", end="")
        try:
            self.monitor_manager.stop(list(self.involved_nodes), silent=True)
            print("OK")
        except:
            print("Failed")
        print("👋 Controller Exited.")

    def _load_config_from_file(self, path):
        if not os.path.exists(path): sys.exit(f"❌ Config not found: {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f: return json.load(f)
        except UnicodeDecodeError:
            try:
                with open(path, 'r', encoding='gbk') as f: return json.load(f)
            except Exception as e:
                sys.exit(f"❌ 读取配置文件失败: {e}")
        except Exception as e:
            sys.exit(f"❌ 读取配置文件异常: {e}")

    def _create_run_root(self, base_dir):
        now = datetime.datetime.now()
        run_name = f"run_{now.strftime('%Y_%m_%d_%H%M%S')}"
        full_path = os.path.abspath(os.path.join(base_dir, run_name))
        for sub in ["metrics", "logs", "report"]:
            os.makedirs(os.path.join(full_path, sub), exist_ok=True)
        return full_path