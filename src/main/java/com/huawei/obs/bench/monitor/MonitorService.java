package com.huawei.obs.bench.monitor;

import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.utils.LogUtil;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Side-channel Monitoring Service (Data Plane - Monitor)
 * Responsible for lock-free reading of global stats every second, calculating instantaneous TPS and bandwidth,
 * and outputting the final P99 latency report at the end of the benchmark.
 */
public class MonitorService {

    private final BenchmarkStats stats;
    private final BenchConfig config;
    private final String taskDir; // [New]: Output directory for task logs
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private long startTimeMs = 0;
    private long totalPlannedRequests = 0;
    private java.io.PrintWriter realtimeWriter; // [New]: Output stream for realtime.txt

    public MonitorService(BenchmarkStats stats, BenchConfig config, String taskDir) {
        this.stats = stats;
        this.config = config;
        this.taskDir = taskDir;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Bypass-Monitor-Thread");
            t.setDaemon(true);
            return t;
        });

        // Pre-calculate total planned requests for progress display
        if (config.testCaseCode() == 900 && config.mixLoopCount() > 0) {
            this.totalPlannedRequests = config.mixLoopCount() * config.getTotalThreads() * config.mixOperations().length * config.requestsPerThread();
        } else if (config.requestsPerThread() > 0) {
            this.totalPlannedRequests = config.requestsPerThread() * config.getTotalThreads();
        }
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            this.startTimeMs = System.currentTimeMillis();
            try {
                this.realtimeWriter = new java.io.PrintWriter(new java.io.FileWriter(taskDir + "/realtime.txt", true));
            } catch (java.io.IOException e) {
                System.err.println("[WARN] Failed to create realtime.txt: " + e.getMessage());
            }

            System.out.println();
            LogUtil.info("MONITOR", "Monitor thread started, waiting for start gun...");
            
            // Start after 3 seconds delay, execute snapshot printing every 3 seconds
            scheduler.scheduleAtFixedRate(this::printMetricsSnapshot, 3, 3, TimeUnit.SECONDS);
        }
    }

    private void printMetricsSnapshot() {
        long currentSuccess = stats.successCount.sum();
        long currentBytes = stats.totalBytesTransferred.sum();
        long currentFail = stats.fail403Count.sum() + stats.fail404Count.sum() + stats.fail5xxCount.sum() + stats.clientErrorCount.sum();
        long totalReqs = currentSuccess + currentFail;

        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        double elapsedSec = elapsedMs / 1000.0;
        if (elapsedSec < 0.1) elapsedSec = 0.1;

        // Cumulative Metrics
        double cumulTps = currentSuccess / elapsedSec;
        double cumulBw = (double) currentBytes / (1024 * 1024) / elapsedSec;
        double successRate = totalReqs == 0 ? 100.0 : (Double.valueOf(currentSuccess) / totalReqs) * 100.0;

        // Progress calculation
        double progress = 0.0;
        if (config.runSeconds() > 0) {
            progress = (elapsedSec / config.runSeconds()) * 100.0;
        } else if (totalPlannedRequests > 0) {
            progress = (Double.valueOf(totalReqs) / totalPlannedRequests) * 100.0;
        }
        if (progress > 100.0) progress = 100.0;

        String logLine = String.format("[Monitor] RunTime: %10.1fs | Process: %6.2f%% | Cumul TPS: %8.2f | Cumul BW: %8.2f MB/s | Success Rate: %7.3f%% | Total Reqs: %d\n",
                elapsedSec, progress, cumulTps, cumulBw, successRate, totalReqs);
        
        System.out.print(logLine);
        if (realtimeWriter != null) {
            realtimeWriter.print(logLine);
            realtimeWriter.flush();
        }
    }

    public void stopAndPrintSummary() {
        if (isRunning.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            long elapsedMs = System.currentTimeMillis() - startTimeMs;
            double elapsedSec = elapsedMs / 1000.0;
            if (elapsedSec < 0.1) elapsedSec = 0.1;
            
            long totalSuccess = stats.successCount.sum();
            long totalFail403 = stats.fail403Count.sum();
            long totalFail404 = stats.fail404Count.sum();
            long totalFail409 = 0; // Currently not tracked separately but can be
            long totalFail5xx = stats.fail5xxCount.sum();
            long totalFailClient = stats.clientErrorCount.sum();
            long totalFailValid = stats.dataValidationFailCount.sum();
            long totalFailOthers = 0;

            long totalFail = totalFail403 + totalFail404 + totalFail409 + totalFail5xx + totalFailClient + totalFailValid + totalFailOthers;
            long totalRequests = totalSuccess + totalFail;

            double avgTps = (double) totalSuccess / elapsedSec;
            double avgBandwidth = (double) stats.totalBytesTransferred.sum() / (1024 * 1024) / elapsedSec;
 
            java.io.PrintWriter briefWriter = null;
            try {
                briefWriter = new java.io.PrintWriter(new java.io.FileWriter(taskDir + "/brief.txt"));
            } catch (java.io.IOException e) {
                System.err.println("[WARN] Failed to create brief.txt: " + e.getMessage());
            }

            printAndFile(briefWriter, "===========================================");
            printAndFile(briefWriter, "      OBS Java Benchmark Execution Report ");
            printAndFile(briefWriter, "===========================================");
            printAndFile(briefWriter, "Execution Time:      " + LogUtil.getTimestamp());
            printAndFile(briefWriter, "---------------- Configuration ----------------");
            printAndFile(briefWriter, "[Environment]");
            printAndFile(briefWriter, String.format("  Endpoint:          %s", config.endpoint()));
            printAndFile(briefWriter, String.format("  Bucket(Fixed):     %s", config.bucketNameFixed().isEmpty() ? "N/A" : config.bucketNameFixed()));
            printAndFile(briefWriter, String.format("  Bucket(Prefix):    %s", config.bucketNamePrefix().isEmpty() ? "N/A" : config.bucketNamePrefix()));
            printAndFile(briefWriter, String.format("  STS Auth Mode:     %s", config.isTemporaryToken()));
            printAndFile(briefWriter, "[Network]");
            printAndFile(briefWriter, String.format("  Protocol:          %s", config.protocol()));
            printAndFile(briefWriter, String.format("  ConnectTimeout:    %d ms", config.connectionTimeoutMs()));
            printAndFile(briefWriter, String.format("  SocketTimeout:     %d ms", config.socketTimeoutMs()));
            printAndFile(briefWriter, "[TestPlan]");
            printAndFile(briefWriter, String.format("  Total Threads:     %d (%d Users x %d Threads/User)", 
                    config.getTotalThreads(), config.usersCount(), config.threadsPerUser()));
            printAndFile(briefWriter, String.format("  RunSeconds:        %d (%s)", 
                    config.runSeconds(), config.runSeconds() == 0 ? "No Limit" : "Limit Enabled"));
            
            String testMode = (config.testCaseCode() == 900) ? "Batch Mixed Mode (900)" : "Standard TestCase (" + config.testCaseCode() + ")";
            printAndFile(briefWriter, String.format("  TestMode:          %s", testMode));
            printAndFile(briefWriter, String.format("  Reqs/Thread:       %d", config.requestsPerThread()));
            
            printAndFile(briefWriter, "[ObjectSettings]");
            if (config.objectSizeMin() == config.objectSizeMax()) {
                printAndFile(briefWriter, String.format("  ObjectSize:        %d bytes", config.objectSizeMax()));
            } else {
                printAndFile(briefWriter, String.format("  ObjectSize:        %d ~ %d bytes (Dynamic)", config.objectSizeMin(), config.objectSizeMax()));
            }
            printAndFile(briefWriter, String.format("  PartSize:          %d bytes", config.partSize()));
            printAndFile(briefWriter, String.format("  KeyPrefix:         %s", config.keyPrefix()));
            printAndFile(briefWriter, String.format("  KeyHashPrefix:     %s", config.objNamePatternHash()));
            printAndFile(briefWriter, "[Advanced]");
            printAndFile(briefWriter, String.format("  DataValidation:    %s", config.enableDataValidation()));
            printAndFile(briefWriter, String.format("  DetailLog:         %s", config.enableDetailLog()));
            printAndFile(briefWriter, String.format("  MockMode:          %s", config.isMockMode()));

            printAndFile(briefWriter, "---------------- Statistics -------------------");
            printAndFile(briefWriter, String.format("Actual Duration: %.2f s", elapsedSec));
            printAndFile(briefWriter, String.format("Total Requests:  %d", totalRequests));
            printAndFile(briefWriter, String.format("Success:         %d", totalSuccess));
            printAndFile(briefWriter, String.format("Failed:          %d", totalFail));
            printAndFile(briefWriter, String.format("  |- 403 (Forbidden):  %d", totalFail403));
            printAndFile(briefWriter, String.format("  |- 404 (NotFound):   %d", totalFail404));
            printAndFile(briefWriter, String.format("  |- 409 (Conflict):   %d", totalFail409));
            printAndFile(briefWriter, String.format("  |- 4xx (Other):      %d", 0)); 
            printAndFile(briefWriter, String.format("  |- 5xx (Server):     %d", totalFail5xx));
            printAndFile(briefWriter, String.format("  |- Other (Net/SDK):  %d", totalFailClient));
            printAndFile(briefWriter, String.format("  |- Internal Validation Fail: %d", totalFailValid));
            
            printAndFile(briefWriter, "\nPerformance:");
            printAndFile(briefWriter, String.format("  Final TPS:           %.2f", avgTps));
            printAndFile(briefWriter, String.format("  Final Throughput:    %.2f MB/s", avgBandwidth));

            printAndFile(briefWriter, "\n[Latency Distribution]");
            printAndFile(briefWriter, String.format("  P50 (Median):  %.2f ms", stats.latencyHistogram.getValueAtPercentile(50.0) / 1_000_000.0));
            printAndFile(briefWriter, String.format("  P90 Latency:   %.2f ms", stats.latencyHistogram.getValueAtPercentile(90.0) / 1_000_000.0));
            printAndFile(briefWriter, String.format("  P99 Latency:   %.2f ms", stats.latencyHistogram.getValueAtPercentile(99.0) / 1_000_000.0));
            printAndFile(briefWriter, String.format("  P99.9 Latency: %.2f ms", stats.latencyHistogram.getValueAtPercentile(99.9) / 1_000_000.0));
            printAndFile(briefWriter, String.format("  Max Latency:   %.2f ms", stats.latencyHistogram.getMaxValue() / 1_000_000.0));
            printAndFile(briefWriter, "===========================================\n");

            if (briefWriter != null) briefWriter.close();
            if (realtimeWriter != null) realtimeWriter.close();
        }
    }

    private void printAndFile(java.io.PrintWriter writer, String line) {
        System.out.println(line);
        if (writer != null) {
            writer.println(line);
        }
    }
}
