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
    show_tool_help
)

def main():
    # 详细的帮助文案
    banner_desc = """
🚀 DMXPerf - HPC Performance Monitoring & Acceptance Platform (v2.0)
==================================================================
专为大规模 GPU 集群设计的轻量级性能分析与硬件验收工具。
支持 "现场编译 (Live Compile)" 技术，自动适配当前驱动环境，彻底解决兼容性问题。

Usage: 
  ./dmxperf [Option]

--------------------------------------------------------------------------------
[1] Monitor Mode (性能监控模式)
--------------------------------------------------------------------------------
  -c, --config FILE     启动性能监控 Agent。
                        需要指定 JSON 配置文件路径 (例如: configs/task.json)。
                        程序将采集 GPU/CPU/IB 指标并生成 timeline 数据。

  --dry-run             仅模拟运行逻辑，不执行实际采集命令 (用于测试配置)。

--------------------------------------------------------------------------------
[2] Hardware Acceptance Mode (硬件验收模式)
--------------------------------------------------------------------------------
  --topo                [拓扑与属性] 运行完整的硬件拓扑检查。包含 3 个步骤：
                          1. 运行 nvidia-smi topo -m (系统矩阵)
                          2. 现场编译并运行 deviceQuery (获取 SMs, Cache, Driver 等详情)
                          3. 现场编译并运行 topologyQuery (获取 NUMA/Affinity 深度信息)

  --bandwidth           [PCIe 带宽] 运行 bandwidthTest (Host <-> Device)。
                        测试 PCIe 总线的 H2D, D2H, D2D 极限吞吐量 (使用 Pinned Memory)。

  --bus                 [PCIe 压测] 运行 busGrind (总线饱和打击)。
                        通过高频小包通信占满 PCIe 通道，检测总线稳定性与掉卡风险。

  --burn SECONDS        [极致烤机] 运行 GPU Burn (矩阵乘法压力测试)。
                        现场生成适配当前架构的机器码，将所有 GPU 负载拉至 100%。
                        参数 SECONDS 指定烤机持续时长 (例如: --burn 60)。

  --native              [原生帮助] 配合上述工具开关使用，查看底层工具原始 Help。
                        例如: ./dmxperf --bandwidth --native

--------------------------------------------------------------------------------
Examples:
  1. 日常监控:       ./dmxperf -c configs/task.json
  2. 新机验收(拓扑): ./dmxperf --topo
  3. 显卡烤机(60秒): ./dmxperf --burn 60
  4. 查看原生帮助:   ./dmxperf --bus --native
"""

    parser = argparse.ArgumentParser(
        description=banner_desc,
        formatter_class=argparse.RawTextHelpFormatter,
        usage=argparse.SUPPRESS # 隐藏默认的一长串 usage 生成
    )
    
    # 核心功能组 (互斥: 一次只能做一个主要动作)
    group = parser.add_mutually_exclusive_group()

    # 1. 监控
    group.add_argument('-c', '--config', metavar='FILE', help=argparse.SUPPRESS)
    
    # 2. 硬件验收
    group.add_argument('--topo', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--bandwidth', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--bus', action='store_true', help=argparse.SUPPRESS)
    group.add_argument('--burn', type=int, metavar='SECONDS', help=argparse.SUPPRESS)

    # 辅助参数
    parser.add_argument('--dry-run', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--native', action='store_true', help=argparse.SUPPRESS)

    # 解析
    args = parser.parse_args()

    # 无参数时打印完整帮助
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    print("-" * 60)

    try:
        # === 路由逻辑 ===

        # 1. 监控模式
        if args.config:
            print(f"📂 [Mode] Performance Monitor")
            print(f"   Config: {args.config}")
            controller = PerfController(args.config, dry_run=args.dry_run)
            controller.run()

        # 2. 拓扑 (含 deviceQuery + topologyQuery)
        elif args.topo:
            print("🛠️  [Check] System Topology & Hardware Attributes")
            run_hw_check()

        # 3. 带宽
        elif args.bandwidth:
            if args.native:
                show_tool_help('bandwidth')
            else:
                print("🛣️  [Check] PCIe Bandwidth Test")
                run_bandwidth_test()

        # 4. 总线压测
        elif args.bus:
            if args.native:
                show_tool_help('bus')
            else:
                print("⚙️  [Check] PCIe Bus Stability Test")
                run_bus_grind()

        # 5. 烤机
        elif args.burn:
            print(f"🔥 [Stress] GPU Burn-in Test ({args.burn}s)...")
            run_gpu_burn(args.burn)
            
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