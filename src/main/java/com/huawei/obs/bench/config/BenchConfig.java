package com.huawei.obs.bench.config;

/**
 * 全局压测配置模型 (Immutable Record)
 * 映射 config.dat 中的所有参数。
 * 采用 Record 保证高并发下的绝对线程安全。
 */
public record BenchConfig(
    // ==========================================
    // 1. 基础网络与认证配置
    // ==========================================
    String endpoint,
    boolean isSecure,          // true: HTTPS, false: HTTP
    boolean isTemporaryToken,  // 是否使用 STS 临时凭证

    // ==========================================
    // 2. Java 底层连接池调优
    // ==========================================
    int maxConnections,        // 底层 HTTP 最大并发连接数
    int socketTimeoutMs,       // Socket 读写超时
    int connectionTimeoutMs,   // 建连超时

    // ==========================================
    // 3. 压测并发模型与跳出策略
    // ==========================================
    int usersCount,            // 并发用户数
    int threadsPerUser,        // 单用户并发线程数
    long runSeconds,           // 压测总时长(秒)，0代表不限制
    long requestsPerThread,    // 单线程最大请求数，0代表不限制

    // ==========================================
    // 4. 测试用例路由
    // ==========================================
    int testCaseCode,          // 用例编码: 201, 202, 216 等

    // ==========================================
    // 5. 对象与数据属性
    // ==========================================
    String bucketName,         // 目标桶名
    String keyPrefix,          // 对象名前缀
    long objectSize,           // 单个对象大小(Bytes)
    long partSize,             // 分段大小(Bytes)

    // ==========================================
    // 6. 高级架构特性
    // ==========================================
    boolean objNamePatternHash,   // 是否开启一致性 Hash 打散
    boolean enableDataValidation, // 是否开启 LCG 零拷贝校验
    boolean enableDetailLog,      // 是否开启流水异步落盘
    boolean isMockMode            // 是否为离线自测 Mock 模式
) {
    /**
     * 紧凑型构造器 (Compact Constructor)
     * 在这里可以加入架构师级别的防御性校验，确保错误配置在启动时就被拦截，而不是在压测中途崩溃
     */
    public BenchConfig {
        // 1. 并发参数校验
        if (usersCount <= 0) {
            throw new IllegalArgumentException("UsersCount 必须大于 0");
        }
        if (threadsPerUser <= 0) {
            throw new IllegalArgumentException("ThreadsPerUser 必须大于 0");
        }

        // 2. 连接池安全校验：防止底层连接池饿死 Worker 线程
        int totalThreads = usersCount * threadsPerUser;
        if (maxConnections < totalThreads) {
            System.err.printf("[WARN] 配置风险: MaxConnections (%d) 小于总并发线程数 (%d)。" +
                    "这会导致大量线程阻塞等待连接。建议调大 MaxConnections！\n", maxConnections, totalThreads);
        }

        // 3. 跳出条件校验
        if (runSeconds <= 0 && requestsPerThread <= 0) {
            throw new IllegalArgumentException("RunSeconds 和 RequestsPerThread 不能同时为 0，否则压测将无法自动停止");
        }
    }

    /**
     * 获取全局总并发线程数
     */
    public int getTotalThreads() {
        return usersCount * threadsPerUser;
    }
}
