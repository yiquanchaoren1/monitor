# dmxperf/cli/main.py
# -*- coding: utf-8 -*-
import argparse
import sys
from dmxperf.controller.controller import PerfController
from dmxperf.tools.gpu_tools import (
    run_hw_check, 
    run_bandwidth_test, 
    run_bus_grind, 
    run_gpu_burn, 
    run_gemm_test,   # <--- [新增] 导入 GEMM 测试函数
    show_tool_help
)

def main():
    banner_desc = """
🚀 DMXPerf - HPC Performance Monitoring & Acceptance Platform (v2.0)
==================================================================
专为大规模 GPU 集群设计的轻量级性能分析与硬件验收工具。
支持 "现场编译 (Live Compile)" 技术，自动适配当前驱动环境。

Usage: 
  ./dmxperf [Option]

--------------------------------------------------------------------------------
[1] Monitor Mode (性能监控模式)
--------------------------------------------------------------------------------
  -c, --config FILE     启动性能监控 Agent。
                        需要指定 JSON 配置文件路径 (例如: configs/task.json)。

--------------------------------------------------------------------------------
[2] Hardware Acceptance Mode (硬件验收模式)
--------------------------------------------------------------------------------
  --topo                [拓扑与属性] 运行 nvidia-smi topo, deviceQuery, topologyQuery。
  --bandwidth           [PCIe 带宽] 运行 bandwidthTest。
                        支持透传参数: --bandwidth --device=all
  --bus                 [PCIe 压测] 运行 busGrind (稳定性测试)。
  --burn SECONDS        [极致烤机] 运行 GPU Burn (矩阵乘法压力测试)。
  --gemm [ARGS]         [多卡验收] 运行多卡 GEMM 算力与吞吐测试 (cuBLASXt)。 [NEW]
                        参数: ./dmxperf --gemm [Size] [Duration]
                        例如: ./dmxperf --gemm 16384 10
                        设置卡数:CUDA_VISIBLE_DEVICES=0,1,2,3 ./release/bin/dmxperf --gemm

  --native              [原生帮助] 查看底层工具原始 Help。

"""

    parser = argparse.ArgumentParser(
        description=banner_desc,
        formatter_class=argparse.RawTextHelpFormatter,
        usage=argparse.SUPPRESS
    )
    
    group = parser.add_mutually_exclusive_group()

    group.add_argument('-c', '--config', metavar='FILE', help=argparse.SUPPRESS)
    group.add_argument('--topo', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--bandwidth', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--bus', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--burn', type=int, metavar='SECONDS', help=argparse.SUPPRESS)
    group.add_argument('--gemm', action='store_true', help=argparse.SUPPRESS) # <--- [新增]

    parser.add_argument('--dry-run', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--native', action='store_true', help=argparse.SUPPRESS)

    # 透传参数解析
    args, unknown = parser.parse_known_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    print("-" * 60)

    try:
        # 1. 监控模式
        if args.config:
            print(f"📂 [Mode] Performance Monitor")
            print(f"   Config: {args.config}")
            controller = PerfController(args.config, dry_run=args.dry_run)
            controller.run()

        # 2. 拓扑
        elif args.topo:
            print("🛠️  [Check] System Topology & Hardware Attributes")
            run_hw_check()

        # 3. 带宽
        elif args.bandwidth:
            if args.native: show_tool_help('bandwidth')
            else:
                msg = " PCIe Bandwidth Test"
                if unknown: msg += f" (Args: {' '.join(unknown)})"
                print(f"🛣️  [Check]{msg}")
                run_bandwidth_test(extra_args=unknown)

        # 4. 总线压测
        elif args.bus:
            if args.native: show_tool_help('bus')
            else:
                msg = " PCIe Bus Stability Test"
                if unknown: msg += f" (Args: {' '.join(unknown)})"
                print(f"⚙️  [Check]{msg}")
                run_bus_grind(extra_args=unknown)

        # 5. 烤机
        elif args.burn:
            print(f"🔥 [Stress] GPU Burn-in Test ({args.burn}s)...")
            run_gpu_burn(args.burn)
        
        # 6. 多卡 GEMM 验收 [新增]
        elif args.gemm:
            print(f"💎 [Check] Multi-GPU GEMM Performance Test")
            if unknown:
                print(f"   Args: {' '.join(unknown)}")
            else:
                print(f"   Args: (Default) N=16384, Duration=10s")
            run_gemm_test(extra_args=unknown)
            
        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断。")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()