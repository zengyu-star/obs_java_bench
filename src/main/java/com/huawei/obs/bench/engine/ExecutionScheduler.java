package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.adapter.MockObsAdapter;
import com.huawei.obs.bench.adapter.RealObsAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.ObsClientManager;
import com.huawei.obs.bench.config.UserCredential;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.monitor.MonitorService;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 压测核心调度中心 (Scheduler Plane)
 * 负责组装 Worker、管控并发线程池，并通过“发令枪”机制保证所有线程绝对公平地同时起跑。
 */
public class ExecutionScheduler {

    private final BenchConfig config;
    private final BenchmarkStats globalStats;
    private final ObsClientManager clientManager;

    public ExecutionScheduler(BenchConfig config, BenchmarkStats globalStats) {
        this.config = config;
        this.globalStats = globalStats;
        this.clientManager = ObsClientManager.getInstance();
    }

    /**
     * 启动压测主流程
     * @param users 解析好的多租户凭证列表
     * @param monitor 旁路监控服务
     */
    public void startBenchmark(List<UserCredential> users, MonitorService monitor) throws InterruptedException {
        int totalWorkers = config.getTotalThreads();
        System.out.printf("\n[Scheduler] 开始构建并发执行引擎，目标总并发线程数: %d ...\n", totalWorkers);

        // ==============================================================
        // 1. 初始化三把“锁” (发令枪模型核心)
        // ==============================================================
        // Ready Latch: 等待所有线程完成 DirectBuffer 分配等高耗时准备工作
        CountDownLatch readyLatch = new CountDownLatch(totalWorkers);
        // Start Gun: 只有 1 的容量，主线程 countDown 后，所有 Worker 瞬间释放起跑
        CountDownLatch startGun = new CountDownLatch(1);
        // Done Latch: 等待所有 Worker 执行完毕，用于主线程阻塞收尾
        CountDownLatch doneLatch = new CountDownLatch(totalWorkers);

        // ==============================================================
        // 2. 初始化线程池
        // ==============================================================
        // 架构师建议：采用固定大小的线程池，避免压测中途由于动态扩容造成的 TPS 抖动
        ExecutorService executorPool = Executors.newFixedThreadPool(totalWorkers, r -> {
            Thread t = new Thread(r);
            t.setName("OBS-Worker-" + t.getId());
            return t;
        });

        // ==============================================================
        // 3. 组装并分发 Worker 任务
        // ==============================================================
        int workerId = 0;
        for (UserCredential user : users) {
            // 为每个用户的每个线程构建独立的 Context 和 Task
            for (int t = 0; t < config.threadsPerUser(); t++) {
                WorkerContext context = buildWorkerContext(workerId, user);
                WorkerTask worker = new WorkerTask(context, globalStats, readyLatch, startGun, doneLatch);
                
                executorPool.submit(worker);
                workerId++;
            }
        }

        // ==============================================================
        // 4. 发令起跑流程控制 (The Critical Path)
        // ==============================================================
        System.out.println("[Scheduler] 任务已全部投递，正在等待所有 Worker 申请堆外内存并就绪 (Ready)...");
        
        // 阻塞主线程，直到所有的 Worker 执行完 Phase 1 并汇报就绪
        readyLatch.await(); 
        System.out.println("[Scheduler] 所有 Worker 均已就绪 (READY)。");

        // 启动仪表盘：监控线程先跑起来
        monitor.start();

        // 💥 扣动发令枪！
        System.out.println("=========================================================================================");
        System.out.println(" 🔫 BANG! 发令枪响，压测正式开始！(Benchmark strictly started)");
        System.out.println("=========================================================================================");
        startGun.countDown(); 

        // ==============================================================
        // 5. 阻塞等待结束与优雅收尾
        // ==============================================================
        // 阻塞主线程，静静等待所有的 Worker 执行到 Phase 3
        doneLatch.await();

        // 所有的 Worker 都干完活了，停止仪表盘并打印权威 P99 报告
        monitor.stopAndPrintSummary();

        // 优雅关闭线程池，归还操作系统资源
        executorPool.shutdown();
        if (!executorPool.awaitTermination(10, TimeUnit.SECONDS)) {
            executorPool.shutdownNow();
        }
        System.out.println("[Scheduler] 引擎调度完毕，主线程安全退出。");
    }

    /**
     * 工厂方法：构建线程安全的执行上下文
     */
    private WorkerContext buildWorkerContext(int threadId, UserCredential user) {
        IObsClientAdapter adapter;
        
        if (config.isMockMode()) {
            // 离线自测模式：模拟 20ms 网络延迟和 0.1% 的服务端报错概率
            adapter = new MockObsAdapter(20, 10); 
        } else {
            // 真实发流模式：从管理器中获取该用户专属的 ObsClient，并包裹防腐层 Adapter
            adapter = new RealObsAdapter(clientManager.getClient(user.username()));
        }

        return new WorkerContext(threadId, config, user, adapter);
    }
}
