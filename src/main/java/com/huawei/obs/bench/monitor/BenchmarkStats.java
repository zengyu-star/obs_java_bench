package com.huawei.obs.bench.monitor;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.concurrent.atomic.LongAdder;

/**
 * Global Benchmark Statistics Dashboard (Data Plane - Statistics)
 * Core Architectural Requirement: Lock-free accumulation under extreme concurrency; 
 * strictly avoid CAS spin locks from AtomicLong to maintain throughput.
 */
public class BenchmarkStats {

    // =================================================================
    // 1. Base Throughput & Status Code Statistics (Using LongAdder for heat dispersion)
    // =================================================================
    public final LongAdder successCount = new LongAdder();
    
    public final LongAdder fail403Count = new LongAdder();
    public final LongAdder fail404Count = new LongAdder();
    public final LongAdder fail5xxCount = new LongAdder();
    public final LongAdder clientErrorCount = new LongAdder(); // Local network or code exceptions
    public final LongAdder dataValidationFailCount = new LongAdder(); // Count of data integrity check failures
    
    public final LongAdder totalBytesTransferred = new LongAdder();
    
    // Base accumulator for calculating average latency
    public final LongAdder totalLatencyNanos = new LongAdder();

    // =================================================================
    // 2. High-Precision Long-Tail Latency (P50, P90, P99, P99.9)
    // =================================================================
    /**
     * HdrHistogram: A high-performance histogram library designed for real-time statistics.
     * Param 1: highestTrackableValue. Set to 60,000,000,000 ns (60s); values exceeding this are capped.
     * Param 2: numberOfSignificantValueDigits. Set to 3 for <0.1% error margin.
     * Advantage: Fixed memory footprint (hundreds of KB) regardless of recording frequency, preventing OOM.
     */
    public final Histogram latencyHistogram = new ConcurrentHistogram(60000000000L, 3);

    /**
     * Reset all statistics (mainly used for multi-run loops or warm-up phases)
     */
    public void reset() {
        successCount.reset();
        fail403Count.reset();
        fail404Count.reset();
        fail5xxCount.reset();
        clientErrorCount.reset();
        dataValidationFailCount.reset();
        totalBytesTransferred.reset();
        totalLatencyNanos.reset();
        latencyHistogram.reset();
    }
}
