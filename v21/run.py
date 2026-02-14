# -*- coding: utf-8 -*-
import sys
import os

# 1. 获取项目根目录的绝对路径
project_root = os.path.dirname(os.path.abspath(__file__))

# 2. 将根目录加入 Python 搜索路径
sys.path.insert(0, project_root)

# 3. 导入 CLI 入口
try:
    from dmxperf.cli.main import main
except ImportError as e:
    print(f"❌ 环境错误: 无法加载 dmxperf 包。\n详细信息: {e}")
    sys.exit(1)

if __name__ == "__main__":
    # 4. 启动主程序
    main()
