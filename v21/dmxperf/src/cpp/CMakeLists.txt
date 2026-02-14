cmake_minimum_required(VERSION 3.18)
project(GpuTools LANGUAGES CXX CUDA)

# 【核心功能】
# 如果没有手动指定架构，这就告诉 CMake："去问问当前的显卡是啥架构，只编译那一种"
if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES "native")
endif()

# 告诉编译器头文件在同级的 common 目录下
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common)

# 定义要编译的目标
add_executable(deviceQuery deviceQuery.cu)
add_executable(topologyQuery topologyQuery.cu)

# 链接必要的库
target_link_libraries(deviceQuery PRIVATE cuda)
target_link_libraries(topologyQuery PRIVATE cuda)
