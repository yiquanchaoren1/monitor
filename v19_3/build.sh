#!/bin/bash

# =================================================================
# DMXPerf Build Script (Flat CLI Mode)
# =================================================================

echo "🧹 Cleaning up old builds..."
rm -rf build/ dist/ release/ *.spec

# === 1. 环境自检 ===
echo "🔍 Checking source files..."

# 1.1 静态二进制
if [ ! -f "dmxperf/src/bin/bandwidthTest" ]; then
    echo "❌ Error: 'bandwidthTest' missing in dmxperf/src/bin!"
    exit 1
fi
if [ ! -f "dmxperf/src/bin/busGrind" ]; then
    echo "❌ Error: 'busGrind' missing in dmxperf/src/bin!"
    exit 1
fi

# 1.2 源码文件
if [ ! -d "dmxperf/src/cpp/common" ]; then
    echo "❌ Error: 'common' folder missing in dmxperf/src/cpp!"
    exit 1
fi
if [ ! -f "dmxperf/src/cpp/deviceQuery.cu" ]; then
    echo "❌ Error: 'deviceQuery.cu' missing in dmxperf/src/cpp!"
    exit 1
fi
# [新增] 检查 topologyQuery
if [ ! -f "dmxperf/src/cpp/topologyQuery.cu" ]; then
    echo "❌ Error: 'topologyQuery.cu' missing in dmxperf/src/cpp!"
    echo "   👉 请确保将官方 Samples 中的 topologyQuery.cu 放入该目录。"
    exit 1
fi
if [ ! -f "dmxperf/src/cpp/compare.cu" ]; then
    echo "❌ Error: 'compare.cu' missing in dmxperf/src/cpp!"
    exit 1
fi
if [ ! -f "dmxperf/src/cpp/gpu_burn-drv.cpp" ]; then
    echo "❌ Error: 'gpu_burn-drv.cpp' missing in dmxperf/src/cpp!"
    exit 1
fi

# === 2. 打包子 Agents ===
echo "📦 Building Host/Device Agents..."
pyinstaller -F --clean --name dmx_host_agent dmxperf/agents/host_agent.py
pyinstaller -F --clean --name dmx_device_agent dmxperf/agents/device_agent.py

# === 3. 打包 主控程序 ===
echo "📦 Building Main Controller (Flat CLI)..."
pyinstaller -F --clean \
    --name dmxperf \
    --hidden-import="pandas" \
    --hidden-import="matplotlib" \
    --hidden-import="matplotlib.backends.backend_agg" \
    --add-data "dmxperf/src/bin:src/bin" \
    --add-data "dmxperf/src/cpp:src/cpp" \
    run.py

if [ $? -ne 0 ]; then
    echo "❌ PyInstaller Build Failed!"
    exit 1
fi

# === 4. 整理发布目录 ===
echo "📂 Organizing release directory..."
mkdir -p release/bin
mkdir -p release/configs

mv dist/dmxperf release/bin/
mv dist/dmx_host_agent release/bin/
mv dist/dmx_device_agent release/bin/

if [ -d "configs" ]; then
    cp -r configs/* release/configs/ 2>/dev/null
fi
chmod +x release/bin/*

echo "✅ Build Complete!"
echo "-----------------------------------------------------"
echo "👉 Examples:"
echo "   ./release/bin/dmxperf --help         (查看详细帮助)"
echo "   ./release/bin/dmxperf --topo         (拓扑+硬件信息)"
echo "   ./release/bin/dmxperf --burn 60      (烤机)"
echo "   ./release/bin/dmxperf -c 1.json      (监控)"