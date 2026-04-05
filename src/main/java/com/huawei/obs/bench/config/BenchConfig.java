package com.huawei.obs.bench.config;

/**
 * Global Benchmark Configuration Model (Immutable Record)
 * Maps to all parameters in config.dat.
 * Uses Record to ensure absolute thread-safety under high concurrency.
 */
public record BenchConfig(
    // ==========================================
    // 1. Basic Network & Authentication
    // ==========================================
    String endpoint,
    String protocol,           // http or https
    boolean isTemporaryToken,  // Whether to use STS temporary credentials
    String logLevel,           // Tool log level (DEBUG, INFO, WARN, ERROR)
    String bucketLocation,    // Bucket region ID (e.g., cn-north-4)

    // ==========================================
    // 2. Low-level Java Connection Pool Tuning
    // ==========================================
    int maxConnections,        // Max concurrent connections in the underlying HTTP pool
    int socketTimeoutMs,       // Socket read/write timeout
    int connectionTimeoutMs,   // Connection establishing timeout

    // ==========================================
    // 3. Benchmark Concurrency & Exit Strategy
    // ==========================================
    int usersCount,            // Number of concurrent users
    int threadsPerUser,        // Concurrent threads per user
    long runSeconds,           // Total benchmark duration (seconds), 0 means unlimited
    long requestsPerThread,    // Max requests per thread, 0 means unlimited

    // ==========================================
    // 4. Test Case Routing
    // ==========================================
    int testCaseCode,          // Code: 201, 202, 216, etc.

    // ==========================================
    // 5. Object & Data Attributes
    // ==========================================
    String bucketNameFixed,    // Fixed bucket name (highest priority)
    String bucketNamePrefix,   // Dynamic bucket prefix (ak.prefix)
    String objectNameFixed,    // Fixed object name (highest priority)
    String keyPrefix,          // Object name prefix
    String uploadFilePath,     // Local file path used for Resumable/Multipart upload testing
    long objectSizeMin,        // Minimum object size (Bytes)
    long objectSizeMax,        // Maximum object size (Bytes)
    long partSize,             // Part size (Bytes)

    // ==========================================
    // 6. Advanced Architectural Features
    // ==========================================
    boolean objNamePatternHash,   // Whether to enable consistent hash scattering
    boolean enableDataValidation, // Whether to enable LCG zero-copy validation
    boolean enableDetailLog,      // Whether to enable asynchronous detail logging
    boolean isMockMode,           // Whether to enable offline Mock mode
    boolean enableCheckpoint,     // Whether to enable resumable checkpoints

    // ==========================================
    // 7. Mixed Workload Mode 900
    // ==========================================
    int[] mixOperations,          // List of operation codes for MIX mode
    long mixLoopCount,            // Number of cycles for MIX mode

    // ==========================================
    // 8. Controlled Multipart Actions
    // ==========================================
    int partsForEachUploadID      // [New]: Number of parts per multipart upload
) {
    /**
     * Compact Constructor
     * Includes defensive validations to catch misconfigurations at startup.
     */
    public BenchConfig {
        // 1. Concurrency Parameter Validation
        if (usersCount <= 0) {
            throw new IllegalArgumentException("UsersCount must be > 0");
        }
        if (threadsPerUser <= 0) {
            throw new IllegalArgumentException("ThreadsPerUser must be > 0");
        }

        // 2. Connection Pool Safety Validation: Prevent Worker starvation
        int totalThreads = usersCount * threadsPerUser;
        if (maxConnections < totalThreads) {
            System.err.printf("[WARN] Config Risk: MaxConnections (%d) is less than total concurrent threads (%d). " +
                    "This may cause significant blocking. Please increase MaxConnections!\n", maxConnections, totalThreads);
        }

        // 3. Mixed Mode Validation
        if (testCaseCode == 900 && mixLoopCount <= 0) {
            throw new IllegalArgumentException("MixLoopCount must be a positive integer (> 0) when TestCaseCode is 900");
        }

        // 4. Exit Condition Validation
        if (runSeconds == 0) {
             System.out.println("[INFO] RunSeconds is empty. Benchmark will run until each thread completes " + requestsPerThread + " requests.");
        }

        // 5. Multipart Parameter Validation
        if (testCaseCode == 216 && partsForEachUploadID <= 0) {
             throw new IllegalArgumentException("PartsForEachUploadID must be a positive integer (> 0) when TestCaseCode is 216 (Multipart Upload)");
        }
    }

    /**
     * Get total global concurrent threads
     */
    public int getTotalThreads() {
        return usersCount * threadsPerUser;
    }

    /**
     * Create a new BenchConfig with a modified testCaseCode (CLI override)
     */
    public BenchConfig withTestCaseCode(int code) {
        return new BenchConfig(
            endpoint, protocol, isTemporaryToken, logLevel, bucketLocation,
            maxConnections, socketTimeoutMs, connectionTimeoutMs,
            usersCount, threadsPerUser, runSeconds, requestsPerThread,
            code, // The override
            bucketNameFixed, bucketNamePrefix, objectNameFixed, keyPrefix, uploadFilePath, objectSizeMin, objectSizeMax, partSize,
            objNamePatternHash, enableDataValidation, enableDetailLog, isMockMode, enableCheckpoint,
            mixOperations, mixLoopCount, partsForEachUploadID
        );
    }
}
