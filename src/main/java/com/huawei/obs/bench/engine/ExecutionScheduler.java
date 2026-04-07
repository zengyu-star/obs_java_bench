package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.adapter.MockObsAdapter;
import com.huawei.obs.bench.adapter.RealObsAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.ObsClientManager;
import com.huawei.obs.bench.config.UserCredential;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.monitor.MonitorService;
import com.huawei.obs.bench.utils.LogUtil;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Core Execution Scheduler (Scheduler Plane)
 * Responsible for assembling Workers, managing the thread pool, and ensuring fair starts via the "Start Gun" mechanism.
 */
public class ExecutionScheduler {

    private final BenchConfig config;
    private final BenchmarkStats globalStats;
    private final ObsClientManager clientManager;
    private final String taskDir;

    public ExecutionScheduler(BenchConfig config, BenchmarkStats globalStats, String taskDir) {
        this.config = config;
        this.globalStats = globalStats;
        this.taskDir = taskDir;
        this.clientManager = ObsClientManager.getInstance();
    }

    /**
     * Start the benchmark process
     * @param users List of parsed multi-tenant credentials
     * @param monitor Side-channel monitoring service
     */
    public void startBenchmark(List<UserCredential> users, MonitorService monitor) throws InterruptedException {
        int totalWorkers = config.getTotalThreads();
        System.out.printf("\n[Scheduler] Starting execution engine, target concurrency: %d ...\n", totalWorkers);

        // ==============================================================
        // 1. Initialize Synchronization Latches (Start Gun Model)
        // ==============================================================
        // Ready Latch: Wait for all threads to allocate DirectBuffers and finish setup
        CountDownLatch readyLatch = new CountDownLatch(totalWorkers);
        // Start Gun: 1 capacity; once main thread counts down, all workers release simultaneously
        CountDownLatch startGun = new CountDownLatch(1);
        // Done Latch: Wait for all workers to finish for final report
        CountDownLatch doneLatch = new CountDownLatch(totalWorkers);

        // ==============================================================
        // 2. Initialize Thread Pool
        // ==============================================================
        // Architect's note: Use a fixed-size thread pool to avoid TPS jitter caused by dynamic scaling during benchmarking
        ExecutorService executorPool = Executors.newFixedThreadPool(totalWorkers, r -> {
            Thread t = new Thread(r);
            t.setName("OBS-Worker-" + t.getId());
            return t;
        });

        // ==============================================================
        // 3. Assemble and Distribute Worker Tasks
        // ==============================================================
        int workerId = 0;
        for (UserCredential user : users) {
            // Build independent Context and Task for each thread of each user
            for (int t = 0; t < config.threadsPerUser(); t++) {
                LogUtil.debug("SCHEDULER", "Initializing Worker Thread for user: " + user.username());
                WorkerContext context = buildWorkerContext(workerId, user);
                WorkerTask worker = new WorkerTask(context, globalStats, readyLatch, startGun, doneLatch, taskDir, workerId);
                
                executorPool.submit(worker);
                workerId++;
            }
        }

        // ==============================================================
        // 4. Start Sequence Control (The Critical Path)
        // ==============================================================
        System.out.println("[Scheduler] Tasks submitted. Waiting for all Workers to allocate memory and signal READY...");
        
        // Block main thread until all Workers complete Phase 1 and signal readiness
        readyLatch.await(); 
        System.out.println("[Scheduler] All Workers are READY.");

        // Start monitor dashboard thread
        monitor.start();

        // PULL THE TRIGGER!
        System.out.println("=========================================================================================");
        System.out.println(" BANG! Benchmark officially started!");
        System.out.println("=========================================================================================");
        startGun.countDown(); 

        // ==============================================================
        // 5. Wait for Completion & Graceful Shutdown
        // ==============================================================
        // Block main thread and wait for all Workers to reach Phase 3
        doneLatch.await();

        // All workers finished, stop dashboard and print final summary report
        monitor.stopAndPrintSummary();

        // Graceful shutdown of thread pool, return OS resources
        executorPool.shutdown();
        if (!executorPool.awaitTermination(10, TimeUnit.SECONDS)) {
            executorPool.shutdownNow();
        }
        System.out.println("[Scheduler] Benchmark execution finished. Main thread exited safely.");
    }

    /**
     * Factory method: Build thread-safe execution context
     */
    private WorkerContext buildWorkerContext(int threadId, UserCredential user) {
        IObsClientAdapter adapter;
        
        if (config.isMockMode()) {
            // Mock Mode: Use configured network latency and error rate
            adapter = new MockObsAdapter(config); 
        } else {
            // Real Mode: Get dedicated ObsClient for this user
            adapter = new RealObsAdapter(clientManager.getClient(user.username()));
        }

        return new WorkerContext(threadId, config, user, adapter);
    }
}
