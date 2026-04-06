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
    Boolean objNamePatternHash,   // Whether to enable consistent hash scattering
    Boolean enableDataValidation, // Whether to enable LCG zero-copy validation
    boolean enableDetailLog,      // Whether to enable asynchronous detail logging
    boolean isMockMode,           // Whether to enable offline Mock mode
    long mockLatencyMs,           // Mock average network latency (ms)
    int mockErrorRate,            // Mock error rate (per 10,000)
    boolean enableCheckpoint,     // Whether to enable resumable checkpoints

    // ==========================================
    // 7. Mixed Workload Mode 900
    // ==========================================
    int[] mixOperations,          // List of operation codes for MIX mode
    long mixLoopCount,            // Number of cycles for MIX mode

    // ==========================================
    // 8. Controlled Multipart Actions
    // ==========================================
    int partsForEachUploadID,     // [New]: Number of parts per multipart upload
    Integer resumableThreads      // [New]: Internal concurrency for Resumable Upload (TestCase 230)
) {
    private static final java.util.Set<String> ALLOWED_PROTOCOLS = java.util.Set.of("http", "https");
    private static final java.util.Set<String> ALLOWED_LOG_LEVELS = java.util.Set.of("DEBUG", "INFO", "CONFIG", "WARN", "ERROR");

    /**
     * Compact Constructor
     * Strictly enforces all configuration rules at instantiation time.
     * Aligns with Directive 6 of .cursorrules.
     */
    public BenchConfig {
        // Validation moved to ConfigValidator to resolve record constructor mystery.
    }

    private void validateTestCase(int code, java.util.List<String> errors) {
        if (isObjectOperation(code)) {
            if (isEmpty(objectNameFixed) && isEmpty(keyPrefix)) {
                errors.add(String.format("TestCase %d requires either ObjectNameFixed or KeyPrefix.", code));
            }
            if (objNamePatternHash == null) {
                errors.add(String.format("TestCase %d requires 'ObjNamePatternHash' to be explicitly configured.", code));
            }
            if (code != 204 && enableDataValidation == null) {
                errors.add(String.format("TestCase %d requires 'EnableDataValidation' to be explicitly configured.", code));
            }
        }

        switch (code) {
            case 101:
                if (isEmpty(bucketLocation)) errors.add("BucketLocation must be specified for TestCase 101.");
                break;
            case 201:
                if (objectSizeMin <= 0 || objectSizeMax <= 0) errors.add("ObjectSize must be > 0 for PutObject (201).");
                if (objectSizeMin > objectSizeMax) errors.add("ObjectSizeMin cannot be greater than ObjectSizeMax.");
                break;
            case 216:
                checkFile(uploadFilePath, errors, "Multipart (216)");
                if (partSize < 5242880) errors.add("PartSize must be at least 5242880 (5MB) for Multipart (216).");
                if (partsForEachUploadID <= 0) errors.add("PartsForEachUploadID must be > 0 for Multipart (216).");
                break;
            case 230:
                checkFile(uploadFilePath, errors, "Resumable (230)");
                if (partSize < 5242880) errors.add("PartSize must be at least 5242880 (5MB) for Resumable (230).");
                if (resumableThreads != null && resumableThreads <= 0) errors.add("ResumableThreads must be > 0 for Resumable (230).");
                break;
        }
    }

    private boolean isObjectOperation(int code) {
        return code == 201 || code == 202 || code == 204 || code == 216 || code == 230;
    }

    private boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    private void checkFile(String path, java.util.List<String> errors, String context) {
        if (isEmpty(path)) {
            errors.add("UploadFilePath is not specified for " + context + ".");
            return;
        }
        java.io.File f = new java.io.File(path);
        if (!f.exists()) {
            errors.add("UploadFilePath '" + path + "' does not exist for " + context + ".");
        } else if (!f.isFile()) {
            errors.add("UploadFilePath '" + path + "' is not a file.");
        }
    }

    /**
     * Get total global concurrent threads
     */
    public int getTotalThreads() {
        return usersCount * threadsPerUser;
    }

    public BenchConfig withTestCaseCode(int code) {
        return new BenchConfig(
            endpoint, protocol, isTemporaryToken, logLevel, bucketLocation,
            maxConnections, socketTimeoutMs, connectionTimeoutMs,
            usersCount, threadsPerUser, runSeconds, requestsPerThread,
            code, // The override
            bucketNameFixed, bucketNamePrefix, objectNameFixed, keyPrefix, uploadFilePath, objectSizeMin, objectSizeMax, partSize,
            objNamePatternHash, enableDataValidation, enableDetailLog, isMockMode, mockLatencyMs, mockErrorRate, enableCheckpoint,
            mixOperations, mixLoopCount, partsForEachUploadID, resumableThreads
        );
    }

    public BenchConfig withUploadFilePath(String newPath) {
        return new BenchConfig(
            endpoint, protocol, isTemporaryToken, logLevel, bucketLocation,
            maxConnections, socketTimeoutMs, connectionTimeoutMs,
            usersCount, threadsPerUser, runSeconds, requestsPerThread,
            testCaseCode,
            bucketNameFixed, bucketNamePrefix, objectNameFixed, keyPrefix, newPath, // The override
            objectSizeMin, objectSizeMax, partSize,
            objNamePatternHash, enableDataValidation, enableDetailLog, isMockMode, mockLatencyMs, mockErrorRate, enableCheckpoint,
            mixOperations, mixLoopCount, partsForEachUploadID, resumableThreads
        );
    }
}
