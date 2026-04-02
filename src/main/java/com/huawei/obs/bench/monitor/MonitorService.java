package com.huawei.obs.bench.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 旁路监控服务 (Data Plane - Monitor)
 * 负责每秒无锁读取全局统计看板，计算瞬时 TPS 与带宽，并在压测结束时输出 P99 长尾时延。
 */
public class MonitorService {

    private final BenchmarkStats stats;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    // 状态快照：用于计算每一秒的差值 (Delta)
    private long lastSuccessCount = 0;
    private long lastBytesTransferred = 0;
    private long startTimeMs = 0;

    public MonitorService(BenchmarkStats stats) {
        this.stats = stats;
        // 【架构师优化】：监控线程必须是守护线程 (Daemon)，这样当压测 Worker 全部执行完毕时，
        // JVM 可以直接退出，而不会被这个定时任务挂起阻塞。
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Bypass-Monitor-Thread");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动实时监控看板
     */
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            this.startTimeMs = System.currentTimeMillis();
            System.out.println("\n[Monitor] 监控线程已启动，等待发令枪...");
            System.out.println("=========================================================================================");
            System.out.printf("%-10s | %-10s | %-15s | %-12s | %-20s\n", 
                              "Time(s)", "TPS", "Bandwidth(MB/s)", "Total Succ", "Errors(403/404/5xx/Client)");
            System.out.println("=========================================================================================");
            
            // 延迟 1 秒后开始，每隔 1 秒执行一次快照打印
            scheduler.scheduleAtFixedRate(this::printMetricsSnapshot, 1, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * 核心逻辑：计算并打印 1 秒钟内的性能快照
     */
    private void printMetricsSnapshot() {
        // 1. 无锁读取当前的 LongAdder 汇总值
        // sum() 方法在高并发下是弱一致性的，但对于秒级瞬时监控来说，这种极其微小的偏差完全可以接受，换来的是零阻塞。
        long currentSuccess = stats.successCount.sum();
        long currentBytes = stats.totalBytesTransferred.sum();
        
        long err403 = stats.fail403Count.sum();
        long err404 = stats.fail404Count.sum();
        long err5xx = stats.fail5xxCount.sum();
        long errClient = stats.clientErrorCount.sum();

        // 2. 计算与上一秒的差值 (Delta)，这就是瞬时 TPS
        long deltaTps = currentSuccess - lastSuccessCount;
        long deltaBytes = currentBytes - lastBytesTransferred;

        // 3. 计算瞬时带宽 (MB/s)
        double bandwidthMb = (double) deltaBytes / (1024 * 1024);

        // 4. 计算已运行的时间 (秒)
        long elapsedSeconds = (System.currentTimeMillis() - startTimeMs) / 1000;
        elapsedSeconds = elapsedSeconds == 0 ? 1 : elapsedSeconds; // 避免出现 0 秒

        // 5. 格式化打印控制台看板
        String errorMetrics = String.format("%d / %d / %d / %d", err403, err404, err5xx, errClient);
        System.out.printf("%-10d | %-10d | %-15.2f | %-12d | %-20s\n",
                elapsedSeconds, deltaTps, bandwidthMb, currentSuccess, errorMetrics);

        // 6. 更新快照游标
        lastSuccessCount = currentSuccess;
        lastBytesTransferred = currentBytes;
    }

    /**
     * 停止监控并输出最终的 Benchmark 权威报告 (包含长尾时延)
     */
    public void stopAndPrintSummary() {
        if (isRunning.compareAndSet(true, false)) {
            // 优雅关闭定时任务
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // 打印最终压测报告
            long totalTimeSec = (System.currentTimeMillis() - startTimeMs) / 1000;
            totalTimeSec = totalTimeSec == 0 ? 1 : totalTimeSec; 
            
            long totalSuccess = stats.successCount.sum();
            long totalBytes = stats.totalBytesTransferred.sum();
            
            double avgTps = (double) totalSuccess / totalTimeSec;
            double avgBandwidth = (double) totalBytes / (1024 * 1024) / totalTimeSec;

            System.out.println("\n================= 🏆 BENCHMARK FINAL SUMMARY 🏆 =================");
            System.out.printf("Total Time Elapsed: %d seconds\n", totalTimeSec);
            System.out.printf("Total Requests:     %d (Success)\n", totalSuccess);
            System.out.printf("Average TPS:        %.2f req/s\n", avgTps);
            System.out.printf("Average Bandwidth:  %.2f MB/s\n", avgBandwidth);
            System.out.println("-------------------------------------------------------------------");
            
            // 利用 HdrHistogram 秒级输出高精度时延 (从纳秒转换为毫秒)
            System.out.println("[Latency Distribution]");
            System.out.printf("  P50 (Median):  %.2f ms\n", stats.latencyHistogram.getValueAtPercentile(50.0) / 1_000_000.0);
            System.out.printf("  P90 Latency:   %.2f ms\n", stats.latencyHistogram.getValueAtPercentile(90.0) / 1_000_000.0);
            System.out.printf("  P99 Latency:   %.2f ms\n", stats.latencyHistogram.getValueAtPercentile(99.0) / 1_000_000.0);
            System.out.printf("  P99.9 Latency: %.2f ms\n", stats.latencyHistogram.getValueAtPercentile(99.9) / 1_000_000.0);
            System.out.printf("  Max Latency:   %.2f ms\n", stats.latencyHistogram.getMaxValue() / 1_000_000.0);
            System.out.println("===================================================================\n");
        }
    }
}
