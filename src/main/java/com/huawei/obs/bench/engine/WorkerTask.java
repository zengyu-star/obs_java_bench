package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.utils.HashUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Benchmark Worker Thread (The Engine Motor)
 * Core requirement: The execution loop (Phase 2) must achieve extreme Zero-GC performance.
 */
public class WorkerTask implements Runnable {

    private final WorkerContext context;
    private final BenchmarkStats globalStats;
    private final String taskDir;
    private final int workerId;
    
    // Scheduling latches
    private final CountDownLatch readyLatch;
    private final CountDownLatch startGun;
    private final CountDownLatch doneLatch;

    private java.io.PrintWriter detailWriter;

    // [Architect Optimization]: Thread-local builders to reuse internal char[] and avoid allocations
    private final StringBuilder keyBuilder = new StringBuilder(128);
    private final StringBuilder csvBuilder = new StringBuilder(256);

    public WorkerTask(WorkerContext context, BenchmarkStats globalStats, 
                      CountDownLatch readyLatch, CountDownLatch startGun, CountDownLatch doneLatch,
                      String taskDir, int workerId) {
        this.context = context;
        this.globalStats = globalStats;
        this.readyLatch = readyLatch;
        this.startGun = startGun;
        this.doneLatch = doneLatch;
        this.taskDir = taskDir;
        this.workerId = workerId;
    }

    @Override
    public void run() {
        try {
            // =========================================================
            // Phase 1: Preparation & Warm-up
            // =========================================================
            BenchConfig config = context.getConfig();
            
            // [Architect's note]: Ensure pattern buffer is allocated if any involved operation is an upload (PUT/Multipart/Resumable)
            if (config.objectSizeMax() > 0) {
                boolean needsUploadBuffer = false;
                if (config.testCaseCode() == 900 && config.mixOperations() != null) {
                    for (int op : config.mixOperations()) {
                        if (isUploadTask(op)) {
                            needsUploadBuffer = true;
                            break;
                        }
                    }
                } else if (isUploadTask(config.testCaseCode())) {
                    needsUploadBuffer = true;
                }
                
                if (needsUploadBuffer || (config.testCaseCode() == 202 && config.enableDataValidation())) {
                    long requiredSize = config.objectSizeMax();
                    if (config.testCaseCode() == 216) {
                        requiredSize = (long) config.partsForEachUploadID() * config.partSize();
                    }
                    
                    ByteBuffer buffer = ByteBuffer.allocateDirect((int) requiredSize);
                    context.setPatternBuffer(buffer);
                    if (config.enableDataValidation()) {
                        context.fillPatternBuffer();
                    }
                }
            }

            if ((config.testCaseCode() == 202 || config.testCaseCode() == 900) && config.enableDataValidation()) {
                // Download verification buffer (64KB is sufficient)
                context.setReceiveBuffer(ByteBuffer.allocateDirect(64 * 1024));
            }

            // [Architect's note]: Initialize Detail Logger if enabled
            if (config.enableDetailLog()) {
                String logFile = String.format("%s/detail_%d_part0.csv", taskDir, workerId);
                try {
                    this.detailWriter = new java.io.PrintWriter(new java.io.BufferedWriter(new java.io.FileWriter(logFile)));
                    this.detailWriter.println("ThreadID,RequestID,Operation,BucketName,ObjectKey,StartTimeMs,LatencyMs,StatusCode,ObsRequestId");
                } catch (java.io.IOException e) {
                    System.err.println("[WARN] Worker-" + workerId + " failed to init detail log: " + e.getMessage());
                }
            }

            // Report: This thread is armed and ready!
            readyLatch.countDown();
            
            // [Critical Barrier]: Wait for the main thread to pull the trigger for a simultaneous start
            startGun.await();

            // =========================================================
            // Phase 2: Extreme Execution Loop
            // Warning: No 'new' keyword or blocking logs allowed in this block!
            // =========================================================
            long seq = 0;
            long startTimeMs = System.currentTimeMillis();
            long endTimeMs = config.runSeconds() > 0 ? startTimeMs + (config.runSeconds() * 1000L) : Long.MAX_VALUE;
            long maxRequests = config.requestsPerThread() > 0 ? config.requestsPerThread() : Long.MAX_VALUE;
            
            IObsClientAdapter adapter = context.getAdapter();
            String targetBucket = context.getTargetBucket();
            ByteBuffer payloadBuffer = context.getPatternBuffer();
            int baseTestCase = config.testCaseCode();

            // MIX Mode 900 helper variables
            int[] mixOps = config.mixOperations();
            int mixOpsCount = mixOps != null ? mixOps.length : 0;
            long reqsPerBatch = config.requestsPerThread(); // In MIX mode, RequestsPerThread acts as batch size
            
            while (true) {
                // 1. Exit condition check
                if (System.currentTimeMillis() >= endTimeMs) {
                    break;
                }
                
                // If in MIX mode 900, we follow the Loop Count exit strategy if LoopCount > 0
                if (baseTestCase == 900) {
                    if (config.mixLoopCount() > 0 && seq >= (config.mixLoopCount() * mixOpsCount * reqsPerBatch)) {
                        break;
                    }
                } else {
                    if (seq >= maxRequests) break;
                }

                // 2. Operation and Object ID selection
                int currentTestCase = baseTestCase;
                long objectSeqId = seq;

                if (baseTestCase == 900 && mixOpsCount > 0) {
                    long currentBlockIdx = seq / reqsPerBatch;
                    int mixIdx = (int) (currentBlockIdx % mixOpsCount);
                    currentTestCase = mixOps[mixIdx];

                    long currentLoopIteration = seq / (mixOpsCount * reqsPerBatch);
                    long currentReqInBlock = seq % reqsPerBatch;
                    objectSeqId = (currentLoopIteration * reqsPerBatch) + currentReqInBlock;
                }

                // 3. Zero-allocation object key generation
                String objectKey;
                if (config.objectNameFixed() != null && !config.objectNameFixed().isEmpty()) {
                    objectKey = config.objectNameFixed();
                } else {
                    HashUtil.generateObjectKey(keyBuilder, config.keyPrefix(), context.getCredential().username(), context.getThreadId(), objectSeqId, config.objNamePatternHash());
                    objectKey = keyBuilder.toString();
                }

                // 4. Determine Dynamic Object Size & Set Buffer Limit
                long currentSize = config.objectSizeMin();
                if (payloadBuffer != null && isUploadTask(currentTestCase)) {
                    payloadBuffer.rewind();
                    if (currentTestCase == 216) {
                        currentSize = (long) config.partsForEachUploadID() * config.partSize();
                    } else if (config.objectSizeMax() > config.objectSizeMin()) {
                        currentSize = java.util.concurrent.ThreadLocalRandom.current().nextLong(config.objectSizeMin(), config.objectSizeMax() + 1);
                    } else {
                        currentSize = config.objectSizeMax();
                    }
                    payloadBuffer.limit((int) currentSize);
                } else if (payloadBuffer != null) {
                    payloadBuffer.rewind();
                }

                // 5. Execute operation with high-precision timing
                long reqStartNanos = System.nanoTime();
                int statusCode = executeOperation(adapter, config, currentTestCase, targetBucket, objectKey, payloadBuffer, context.getReceiveBuffer());
                long latencyNanos = System.nanoTime() - reqStartNanos;

                // 6. Thread-safe stats update
                updateStats(statusCode, latencyNanos, adapter.getLastRequestBytes());

                // 7. Record detail log if enabled (Zero-GC style)
                if (detailWriter != null) {
                    csvBuilder.setLength(0);
                    csvBuilder.append(workerId).append(',')
                              .append(seq).append(',')
                              .append(currentTestCase).append(',')
                              .append(targetBucket).append(',')
                              .append(objectKey).append(',')
                              .append(System.currentTimeMillis()).append(',');
                    fastAppendDouble(csvBuilder, latencyNanos / 1_000_000.0, 3);
                    csvBuilder.append(',').append(statusCode).append(',')
                              .append(adapter.getLastRequestId());
                    
                    detailWriter.println(csvBuilder.toString());
                }

                seq++;
            }

        } catch (InterruptedException e) {
            // Forcefully interrupted by main thread
            Thread.currentThread().interrupt();
        } finally {
            // =========================================================
            // Phase 3: Teardown
            // =========================================================
            if (detailWriter != null) {
                detailWriter.flush();
                detailWriter.close();
            }
            // Report: Thread task complete
            doneLatch.countDown();
        }
    }

    /**
     * Operation Router: Maps TestCase to the corresponding interface
     */
    private int executeOperation(IObsClientAdapter adapter, BenchConfig config, int testCaseCode, String bucket, String key, ByteBuffer payload, ByteBuffer receiveBuffer) {
        return switch (testCaseCode) {
            case 101 -> adapter.createBucket(bucket, config.bucketLocation());
            case 104 -> adapter.deleteBucket(bucket);
            case 201 -> adapter.putObject(bucket, key, payload);
            case 202 -> adapter.getObject(bucket, key, null, payload, receiveBuffer);
            case 204 -> adapter.deleteObject(bucket, key);
            case 216 -> adapter.multipartUpload(bucket, key, payload, config.partsForEachUploadID(), config.partSize());
            case 230 -> adapter.resumableUpload(bucket, key, config.uploadFilePath(), Runtime.getRuntime().availableProcessors(), config.partSize(), config.enableCheckpoint());
            default -> throw new IllegalArgumentException("Unknown TestCaseCode: " + testCaseCode);
        };
    }

    /**
     * High-performance lock-free statistics update
     */
    private void updateStats(int statusCode, long latencyNanos, long objSize) {
        // 2. Latency histogram record (HdrHistogram)
        if (latencyNanos < 60000000000L) {
            globalStats.latencyHistogram.recordValue(latencyNanos);
        } else {
            globalStats.latencyHistogram.recordValue(60000000000L - 1);
        }

        if (statusCode >= 200 && statusCode < 300) {
            globalStats.successCount.increment();
            globalStats.totalBytesTransferred.add(objSize);
        } else if (statusCode == 403) {
            globalStats.fail403Count.increment();
        } else if (statusCode == 404) {
            globalStats.fail404Count.increment();
        } else if (statusCode >= 500 && statusCode < 600) {
            globalStats.fail5xxCount.increment();
        } else if (statusCode == 600) {
            globalStats.dataValidationFailCount.increment();
        } else {
            // Includes local client exceptions (returned as 0)
            globalStats.clientErrorCount.increment();
        }
    }

    /**
     * Determines if the TestCase requires loading data (Upload types)
     */
    private boolean isUploadTask(int testCaseCode) {
        return testCaseCode == 201 || testCaseCode == 216 || testCaseCode == 230;
    }

    /**
     * [Extreme Performance]: Fast double to string conversion with fixed precision.
     * Avoids Double.toString() which allocates and is slow.
     */
    private void fastAppendDouble(StringBuilder sb, double val, int precision) {
        if (Double.isNaN(val)) { sb.append("NaN"); return; }
        if (Double.isInfinite(val)) { sb.append("Infinity"); return; }
        
        if (val < 0) { sb.append('-'); val = -val; }
        
        long multiplier = 1;
        for (int i = 0; i < precision; i++) multiplier *= 10;
        
        long rounded = Math.round(val * multiplier);
        sb.append(rounded / multiplier);
        sb.append('.');
        
        long fractional = rounded % multiplier;
        // Leading zeros for fractional part
        long divisor = multiplier / 10;
        while (divisor > 0 && fractional < divisor) {
            sb.append('0');
            divisor /= 10;
        }
        if (fractional > 0) {
            sb.append(fractional);
        }
    }
}
