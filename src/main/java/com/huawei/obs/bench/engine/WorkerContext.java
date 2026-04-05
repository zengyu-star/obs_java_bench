package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.UserCredential;

import java.nio.ByteBuffer;

/**
 * Worker Thread Execution Context (Thread-Local)
 * Isolates state between threads to avoid lock contention and variable escape.
 */
public class WorkerContext {

    private final int threadId;
    private final BenchConfig config;
    private final UserCredential credential;
    
    // Adapter for Real or Mock OBS traffic
    private final IObsClientAdapter adapter;
    
    // Independently calculated Target Bucket name
    private final String targetBucket;

    // Core Performance: Pre-allocated Zero-Copy Direct Memory
    // ABSOLUTELY NO 'new byte[]' in the benchmark loop!
    private ByteBuffer patternBuffer;
    
    // Read buffer for data validation (e.g., 64KB)
    private ByteBuffer receiveBuffer;

    public WorkerContext(int threadId, BenchConfig config, UserCredential credential, IObsClientAdapter adapter) {
        this.threadId = threadId;
        this.config = config;
        this.credential = credential;
        this.adapter = adapter;
        
        // [Rule]: BucketNameFixed and BucketNamePrefix must not both be empty
        boolean hasFixed = config.bucketNameFixed() != null && !config.bucketNameFixed().isBlank();
        boolean hasPrefix = config.bucketNamePrefix() != null && !config.bucketNamePrefix().isBlank();
        if (!hasFixed && !hasPrefix) {
            throw new IllegalArgumentException("Benchmark Error: BucketNameFixed and BucketNamePrefix (in config.dat) cannot both be empty!");
        }

        // [Rule]: Determine target bucket name
        String rawBucket;
        if (hasFixed) {
            rawBucket = config.bucketNameFixed();
        } else {
            // Requirement: {ak}.{BucketNamePrefix} (Use dot, not hyphen)
            rawBucket = credential.accessKey().toLowerCase() + "." + config.bucketNamePrefix();
        }

        // Final Sanitization: Ensure lowercase and replace illegal underscores with hyphens
        this.targetBucket = rawBucket.toLowerCase().replace('_', '-');
    }

    // ================== Getters & Setters ==================

    public int getThreadId() { return threadId; }
    public BenchConfig getConfig() { return config; }
    public UserCredential getCredential() { return credential; }
    public IObsClientAdapter getAdapter() { return adapter; }
    public String getTargetBucket() { return targetBucket; }
    
    public ByteBuffer getPatternBuffer() { return patternBuffer; }
    public void setPatternBuffer(ByteBuffer patternBuffer) {
        this.patternBuffer = patternBuffer;
    }

    public ByteBuffer getReceiveBuffer() { return receiveBuffer; }
    public void setReceiveBuffer(ByteBuffer receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
    }

    /**
     * High-performance pre-filling of pseudo-random data (Zero-GC LCG)
     * Uses SplitMix64 algorithm with a fixed seed per thread to generate deterministic bitstreams.
     */
    public void fillPatternBuffer() {
        if (patternBuffer == null) {
            return;
        }
        long seed = 0x1234567890ABCDEFL; // Standardized fixed seed for data validation consistency
        patternBuffer.clear();
        while (patternBuffer.remaining() >= 8) {
            seed = com.huawei.obs.bench.utils.HashUtil.splitMix64(seed);
            patternBuffer.putLong(seed);
        }
        // Fill remaining bytes
        while (patternBuffer.hasRemaining()) {
            seed = com.huawei.obs.bench.utils.HashUtil.splitMix64(seed);
            patternBuffer.put((byte) seed);
        }
        // Switch to read mode for upload or verification
        patternBuffer.flip();
    }
}
