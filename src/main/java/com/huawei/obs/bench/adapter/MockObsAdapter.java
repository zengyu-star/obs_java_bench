package com.huawei.obs.bench.adapter;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * OBS Mock Adapter (Execution Plane - Mock Implementation)
 * Responsibility: Simulate network latency and HTTP status codes in memory for offline testing and logic verification.
 */
public class MockObsAdapter implements IObsClientAdapter {

    private final long avgLatencyMs;
    private final int errorRateTenThousandths;
    
    private final ThreadLocal<Long> lastRequestBytes = ThreadLocal.withInitial(() -> 0L);

    /**
     * Construct Mock Adapter
     * @param avgLatencyMs Simulated average network latency (ms)
     * @param errorRateTenThousandths Simulated server-side error rate (unit: 1/10000)
     */
    public MockObsAdapter(long avgLatencyMs, int errorRateTenThousandths) {
        this.avgLatencyMs = avgLatencyMs;
        this.errorRateTenThousandths = errorRateTenThousandths;
    }

    @Override
    public int createBucket(String bucketName, String location) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L);
        // Simple mock: 90% success (200), 10% already exists (409)
        int rand = ThreadLocalRandom.current().nextInt(10);
        return rand < 9 ? 200 : 409;
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
        // For Range downloads, success code is usually 206
        return simulateStatusCode(range == null ? 200 : 206);
    }

    @Override
    public int deleteObject(String bucketName, String objectKey) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L);
        return simulateStatusCode(204); // Success for Delete usually returns 204 No Content
    }

    @Override
    public int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize) {
        // Simulate latency for multiple interactions in multipart upload
        for (int i = 0; i < partCount; i++) {
            simulateNetworkLatency();
        }
        lastRequestBytes.set((long) partCount * partSize);
        return simulateStatusCode(200);
    }

    @Override
    public int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint) {
        simulateNetworkLatency();
        lastRequestBytes.set(0L); // Mock implementation doesn't accurately know file size
        return simulateStatusCode(200);
    }

    /**
     * Simulate network latency
     * Architect's note: Use LockSupport.parkNanos instead of Thread.sleep.
     * parkNanos does not throw InterruptedException, has higher precision, and is friendlier to system scheduling.
     */
    private void simulateNetworkLatency() {
        if (avgLatencyMs <= 0) return;

        // Generate random jitter (±20%) to simulate real network fluctuations
        double jitterFactor = 0.8 + (ThreadLocalRandom.current().nextDouble() * 0.4);
        long sleepNanos = (long) (avgLatencyMs * jitterFactor * 1_000_000L);
        
        LockSupport.parkNanos(sleepNanos);
    }

    /**
     * Simulate HTTP Status Code distribution
     * Used to verify if MonitorService correctly tracks 403/404/5xx anomalies
     */
    private int simulateStatusCode(int successCode) {
        if (errorRateTenThousandths <= 0) return successCode;

        int rand = ThreadLocalRandom.current().nextInt(10000);
        if (rand >= errorRateTenThousandths) {
            return successCode;
        }

        // Randomly assign error types
        int errorType = rand % 3;
        return switch (errorType) {
            case 0 -> 403; // AccessDenied
            case 1 -> 404; // NoSuchKey
            default -> 500; // InternalError
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
