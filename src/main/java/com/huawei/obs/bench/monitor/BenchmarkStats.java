package com.huawei.obs.bench.monitor;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.concurrent.atomic.LongAdder;

/**
 * 全局压测统计看板 (Data Plane - Statistics)
 * 核心架构要求：极高并发下的无锁累加，拒绝 AtomicLong 的 CAS 自旋阻塞。
 */
public class BenchmarkStats {

    // =================================================================
    // 1. 基础吞吐量与状态码统计 (使用 LongAdder 实现高并发下的热点分散计算)
    // =================================================================
    public final LongAdder successCount = new LongAdder();
    
    public final LongAdder fail403Count = new LongAdder();
    public final LongAdder fail404Count = new LongAdder();
    public final LongAdder fail5xxCount = new LongAdder();
    public final LongAdder clientErrorCount = new LongAdder(); // 本地网络或代码异常
    
    public final LongAdder totalBytesTransferred = new LongAdder();
    
    // 用于计算平均时延的基础累加器
    public final LongAdder totalLatencyNanos = new LongAdder();

    // =================================================================
    // 2. 长尾时延高精度统计 (P50, P90, P99, P99.9)
    // =================================================================
    /**
     * HdrHistogram: 专为高性能统计设计的直方图库。
     * 参数1: highestTrackableValue (最高可记录值)。设定为 60,000,000,000 纳秒 (即 60 秒)，超时则不计入直方图。
     * 参数2: numberOfSignificantValueDigits (有效数字位数)。设定为 3，误差范围在 0.1% 以内。
     * 优势：无论记录 1 次还是 1 亿次，占用内存永远固定在几百 KB，绝对不会引发 OOM。
     */
    public final Histogram latencyHistogram = new ConcurrentHistogram(60000000000L, 3);

    // =================================================================
    // 3. 统计看板的重置与清理 (主要用于多次循环压测或预热阶段)
    // =================================================================
    public void reset() {
        successCount.reset();
        fail403Count.reset();
        fail404Count.reset();
        fail5xxCount.reset();
        clientErrorCount.reset();
        totalBytesTransferred.reset();
        totalLatencyNanos.reset();
        latencyHistogram.reset();
    }
}
