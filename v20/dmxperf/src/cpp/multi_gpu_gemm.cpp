#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <cuda_runtime.h>
#include <cublasXt.h>
#include <sys/time.h>
#include <omp.h>
#include <cmath>
#include <algorithm>
#include <numeric>

#define CHECK_CUDA(func) { \
    cudaError_t status = (func); \
    if (status != cudaSuccess) { \
        printf("❌ CUDA 错误: %s (%s:%d)\n", cudaGetErrorString(status), __FILE__, __LINE__); \
        exit(-1); \
    } \
}

#define CHECK_CUBLAS(func) { \
    cublasStatus_t status = (func); \
    if (status != CUBLAS_STATUS_SUCCESS) { \
        printf("❌ cuBLAS 错误: %d (%s:%d)\n", status, __FILE__, __LINE__); \
        exit(-1); \
    } \
}

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

int main(int argc, char** argv) {
    int N = 16384;
    int duration = 10;
    if (argc > 1) N = atoi(argv[1]);
    if (argc > 2) duration = atoi(argv[2]);

    omp_set_num_threads(8); 

    int num_devices = 0;
    CHECK_CUDA(cudaGetDeviceCount(&num_devices));
    std::vector<int> devices(num_devices);
    for (int i = 0; i < num_devices; i++) devices[i] = i;

    cublasXtHandle_t handle;
    CHECK_CUBLAS(cublasXtCreate(&handle));
    CHECK_CUBLAS(cublasXtDeviceSelect(handle, num_devices, devices.data()));
    CHECK_CUBLAS(cublasXtSetBlockDim(handle, 4096)); 

    size_t size_bytes = (size_t)N * N * sizeof(float);
    float *h_A, *h_B, *h_C;
    CHECK_CUDA(cudaMallocHost((void**)&h_A, size_bytes));
    CHECK_CUDA(cudaMallocHost((void**)&h_B, size_bytes));
    CHECK_CUDA(cudaMallocHost((void**)&h_C, size_bytes));

    #pragma omp parallel for
    for (size_t i = 0; i < (size_t)N * N; i++) {
        h_A[i] = 1.0f; h_B[i] = 0.5f;
    }

    float alpha = 1.0f, beta = 0.0f;
    printf("🚀 硬件预热中...\n");
    CHECK_CUBLAS(cublasXtSgemm(handle, CUBLAS_OP_N, CUBLAS_OP_N, N, N, N, &alpha, h_A, N, h_B, N, &beta, h_C, N));
    cudaDeviceSynchronize();

    std::vector<double> latencies;
    double start_test = get_time();
    while ((get_time() - start_test) < duration) {
        double t0 = get_time();
        CHECK_CUBLAS(cublasXtSgemm(handle, CUBLAS_OP_N, CUBLAS_OP_N, N, N, N, &alpha, h_A, N, h_B, N, &beta, h_C, N));
        cudaDeviceSynchronize();
        latencies.push_back(get_time() - t0);
    }

    int iters = latencies.size();
    double total_time = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double time_per_calc = total_time / iters;
    
    double data_moved_gb = (3.0 * N * N * sizeof(float)) / (1024.0 * 1024.0 * 1024.0);
    double throughput_gbs = data_moved_gb / time_per_calc;

    double ops_per_iter = 2.0 * std::pow((double)N, 3);
    double compute_tflops = (ops_per_iter / time_per_calc) / 1e12;

    double pcie_ref = 31.5;
    double utilization = (throughput_gbs / pcie_ref) * 100.0;

    printf("\n==================== 📊 节点性能验收报告 ====================\n");
    printf("任务规模 : %d x %d (单精度 FP32)\n", N, N);
    printf("------------------------------------------------------------\n");
    printf("GPU卡数 : %d GPUs\n", num_devices);    
    printf("------------------------------------------------------------\n");
    // 1. 时间指标
    printf("⏱️  单次计算耗时 : \033[1;33m%.4f s\033[0m\n", time_per_calc);
    
    // 2. 传输指标
    printf("📈 数据吞吐量   : \033[1;36m%.2f GB/s\033[0m\n", throughput_gbs);
    printf("🔗 总线利用率   : %.1f%% (相对于单路 PCIe 4.0 极限)\n", utilization);
    
    // 3. 算力指标
    printf("🔥 核心总算力   : \033[1;32m%.2f TFLOPS\033[0m\n", compute_tflops);
    
    printf("------------------------------------------------------------\n");
    // 4. 诊断逻辑
    printf("⚖️  瓶颈诊断     : ");
    if (utilization > 85.0) {
        printf("\033[1;31m总线受限 (I/O Bound)\033[0m\n");
    } else {
        printf("\033[1;34m计算受限 (Compute Bound)\033[0m\n");
    }
    printf("============================================================\n");

    cublasXtDestroy(handle);
    cudaFreeHost(h_A); cudaFreeHost(h_B); cudaFreeHost(h_C);
    return 0;
}
