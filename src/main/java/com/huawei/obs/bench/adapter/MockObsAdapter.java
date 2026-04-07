package com.huawei.obs.bench.adapter;

import com.huawei.obs.bench.config.BenchConfig;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * OBS Mock Adapter (Execution Plane - Mock Implementation)
 * Responsibility: Simulate network latency and HTTP status codes in memory for offline testing and logic verification.
 */
public class MockObsAdapter implements IObsClientAdapter {

    private final BenchConfig config;
    private final long avgLatencyMs;
    private final int errorRateTenThousandths;
    
    private final ThreadLocal<Long> lastRequestBytes = ThreadLocal.withInitial(() -> 0L);

    public MockObsAdapter(BenchConfig config) {
        this.config = config;
        this.avgLatencyMs = config.mockLatencyMs();
        this.errorRateTenThousandths = config.mockErrorRate();
    }

    @Override
    public int createBucket(String bucketName, String location) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L);
        return simulateStatusCode(200);
    }

    @Override
    public int deleteBucket(String bucketName) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L);
        return 204;
    }

    @Override
    public int putObject(String bucketName, String objectKey, ByteBuffer payload) {
        simulateNetworkLatency();
        lastRequestBytes.set(payload != null ? (long) payload.limit() : 0L);
        return simulateStatusCode(200);
    }

    @Override
    public int getObject(String bucketName, String objectKey, String range, ByteBuffer expectedPattern, ByteBuffer receiveBuffer) {
        simulateNetworkLatency();
        lastRequestBytes.set(receiveBuffer != null ? (long) receiveBuffer.capacity() : 0L);
        return simulateStatusCode(range == null ? 200 : 206);
    }

    @Override
    public int deleteObject(String bucketName, String objectKey) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L);
        return simulateStatusCode(204);
    }

    @Override
    public int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize) {
        // Multi-part simulation: Simulate latency for each part interaction
        for (int i = 0; i < partCount; i++) {
            simulateNetworkLatency();
        }
        lastRequestBytes.set((long) partCount * partSize);
        return simulateStatusCode(200);
    }

    @Override
    public int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint) {
        // Estimate part count for more realistic multi-part simulation
        long fileSize = config.objectSizeMax() > 0 ? config.objectSizeMax() : 100 * 1024 * 1024L; // Default 100MB if unknown
        int estimatedParts = (int) Math.ceil((double) fileSize / Math.max(1, partSize));
        
        // Parallel simulation: For resumable upload, tasks run in parallel.
        // We simulate the bottleneck latency for multiple tasks.
        int loops = (int) Math.ceil((double) estimatedParts / Math.max(1, taskNum));
        for (int i = 0; i < loops; i++) {
            simulateNetworkLatency();
        }
        
        lastRequestBytes.set(fileSize);
        return simulateStatusCode(200);
    }

    private void simulateNetworkLatency() {
        if (avgLatencyMs <= 0) return;
        double jitterFactor = 0.8 + (ThreadLocalRandom.current().nextDouble() * 0.4);
        long sleepNanos = (long) (avgLatencyMs * jitterFactor * 1_000_000L);
        LockSupport.parkNanos(sleepNanos);
    }

    private int simulateStatusCode(int successCode) {
        if (errorRateTenThousandths <= 0) return successCode;
        int rand = ThreadLocalRandom.current().nextInt(10000);
        if (rand >= errorRateTenThousandths) return successCode;

        int errorType = rand % 3;
        return switch (errorType) {
            case 0 -> 403;
            case 1 -> 404;
            default -> 500;
        };
    }

    @Override
    public String getLastRequestId() {
        return "MOCK_REQ_" + ThreadLocalRandom.current().nextInt(1000000);
    }

    @Override
    public long getLastRequestBytes() {
        return lastRequestBytes.get();
    }
}
