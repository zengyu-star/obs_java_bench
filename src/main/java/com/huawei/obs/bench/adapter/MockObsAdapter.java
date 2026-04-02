package com.huawei.obs.bench.adapter;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * OBS 模拟适配器 (Execution Plane - Mock Implementation)
 * 职责：在内存中模拟网络延迟和 HTTP 状态码，用于压测工具的离线自测和逻辑验证。
 */
public class MockObsAdapter implements IObsClientAdapter {

    private final long avgLatencyMs;
    private final int errorRateTenThousandths;

    /**
     * 构造 Mock 适配器
     * @param avgLatencyMs 模拟的平均网络时延 (毫秒)
     * @param errorRateTenThousandths 模拟的服务端错误率 (单位：万分比)
     */
    public MockObsAdapter(long avgLatencyMs, int errorRateTenThousandths) {
        this.avgLatencyMs = avgLatencyMs;
        this.errorRateTenThousandths = errorRateTenThousandths;
    }

    @Override
    public int putObject(String bucketName, String objectKey, ByteBuffer payload) {
        simulateNetworkLatency();
        return simulateStatusCode(200);
    }

    @Override
    public int getObject(String bucketName, String objectKey, String range) {
        simulateNetworkLatency();
        // 如果是 Range 下载，成功码通常是 206
        return simulateStatusCode(range == null ? 200 : 206);
    }

    @Override
    public int deleteObject(String bucketName, String objectKey) {
        simulateNetworkLatency();
        return simulateStatusCode(204); // Delete 成功通常返回 204 No Content
    }

    @Override
    public int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize) {
        // 模拟分段上传的多次交互耗时
        for (int i = 0; i < partCount; i++) {
            simulateNetworkLatency();
        }
        return simulateStatusCode(200);
    }

    @Override
    public int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint) {
        simulateNetworkLatency();
        return simulateStatusCode(200);
    }

    /**
     * 模拟网络延迟
     * 架构师优化：使用 LockSupport.parkNanos 代替 Thread.sleep。
     * 因为 parkNanos 不会抛出 InterruptedException，且精度更高，对系统调度更友好。
     */
    private void simulateNetworkLatency() {
        if (avgLatencyMs <= 0) return;

        // 生成 ±20% 的随机抖动 (Jitter)，模拟真实网络波动
        double jitterFactor = 0.8 + (ThreadLocalRandom.current().nextDouble() * 0.4);
        long sleepNanos = (long) (avgLatencyMs * jitterFactor * 1_000_000L);
        
        LockSupport.parkNanos(sleepNanos);
    }

    /**
     * 模拟 HTTP 状态码分布
     * 用于验证 MonitorService 是否能正确统计 403/404/5xx 等异常
     */
    private int simulateStatusCode(int successCode) {
        if (errorRateTenThousandths <= 0) return successCode;

        int rand = ThreadLocalRandom.current().nextInt(10000);
        if (rand >= errorRateTenThousandths) {
            return successCode;
        }

        // 随机分配错误类型
        int errorType = rand % 3;
        return switch (errorType) {
            case 0 -> 403; // AccessDenied
            case 1 -> 404; // NoSuchKey
            default -> 500; // InternalError
        };
    }
}
