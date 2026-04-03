package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.utils.HashUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * 压测工作线程 (The Engine Motor)
 * 核心要求：发流主循环 (Phase 2) 中必须做到极限的 Zero-GC。
 */
public class WorkerTask implements Runnable {

    private final WorkerContext context;
    private final BenchmarkStats globalStats;
    
    // 调度发令枪
    private final CountDownLatch readyLatch;
    private final CountDownLatch startGun;
    private final CountDownLatch doneLatch;

    // 【架构师优化1】：线程私有的字符串构造器，复用底层 char[]，避免对象分配
    private final StringBuilder keyBuilder = new StringBuilder(128);

    public WorkerTask(WorkerContext context, BenchmarkStats globalStats, 
                      CountDownLatch readyLatch, CountDownLatch startGun, CountDownLatch doneLatch) {
        this.context = context;
        this.globalStats = globalStats;
        this.readyLatch = readyLatch;
        this.startGun = startGun;
        this.doneLatch = doneLatch;
    }

    @Override
    public void run() {
        try {
            // =========================================================
            // Phase 1: 预备与热身阶段 (Preparation)
            // =========================================================
            BenchConfig config = context.getConfig();
            
            // 【架构师优化2】：提前在系统内核态分配好 DirectBuffer，不计入压测耗时
            if (config.objectSize() > 0) {
                if (isUploadTask(config.testCaseCode()) || (config.testCaseCode() == 202 && config.enableDataValidation())) {
                    ByteBuffer buffer = ByteBuffer.allocateDirect((int) config.objectSize());
                    context.setPatternBuffer(buffer);
                    if (config.enableDataValidation()) {
                        context.fillPatternBuffer();
                    }
                }
            }

            if (config.testCaseCode() == 202 && config.enableDataValidation()) {
                // 下载对拍专用缓冲，64KB 足够流转
                context.setReceiveBuffer(ByteBuffer.allocateDirect(64 * 1024));
            }

            // 汇报：本线程军火已装填完毕！
            readyLatch.countDown();
            
            // 【关键阻塞】：在这里死等主线程扣动发令枪，保证绝对的“同时起跑”
            startGun.await();

            // =========================================================
            // Phase 2: 极限发流阶段 (Execution Loop)
            // 警告：以下代码块内，严禁调用 new 关键字，严禁产生阻塞日志
            // =========================================================
            long seq = 0;
            long startTimeMs = System.currentTimeMillis();
            long endTimeMs = config.runSeconds() > 0 ? startTimeMs + (config.runSeconds() * 1000L) : Long.MAX_VALUE;
            long maxRequests = config.requestsPerThread() > 0 ? config.requestsPerThread() : Long.MAX_VALUE;
            
            IObsClientAdapter adapter = context.getAdapter();
            String targetBucket = context.getTargetBucket();
            ByteBuffer payloadBuffer = context.getPatternBuffer();
            int testCase = config.testCaseCode();

            while (true) {
                // 1. 跳出条件检查 (极速位移比较)
                if (System.currentTimeMillis() >= endTimeMs || seq >= maxRequests) {
                    break;
                }

                // 2. 零分配生成对象名
                HashUtil.generateObjectKey(keyBuilder, config.keyPrefix(), context.getCredential().username(), context.getThreadId(), seq, config.objNamePatternHash());
                String objectKey = keyBuilder.toString(); // 这是整个循环中唯一不可避免的微小对象创建

                // 3. 重置 DirectBuffer 游标 (Zero-Copy 核心)
                if (payloadBuffer != null) {
                    payloadBuffer.rewind(); // 将 position 归 0，再次复用这块内存
                }

                // 4. 发起请求并利用纳秒进行高精度计时
                long reqStartNanos = System.nanoTime();
                int statusCode = executeOperation(adapter, testCase, targetBucket, objectKey, payloadBuffer, context.getReceiveBuffer());
                long latencyNanos = System.nanoTime() - reqStartNanos;

                // 5. 无锁累加统计数据
                updateStats(statusCode, latencyNanos, config.objectSize());

                seq++;
            }

        } catch (InterruptedException e) {
            // 被主线程强制中断
            Thread.currentThread().interrupt();
        } finally {
            // =========================================================
            // Phase 3: 优雅收尾阶段 (Teardown)
            // =========================================================
            // 汇报：本线程任务结束
            doneLatch.countDown();
        }
    }

    /**
     * 业务路由：根据 TestCase 映射到具体接口
     */
    private int executeOperation(IObsClientAdapter adapter, int testCaseCode, String bucket, String key, ByteBuffer payload, ByteBuffer receiveBuffer) {
        return switch (testCaseCode) {
            case 201 -> adapter.putObject(bucket, key, payload);
            case 202 -> adapter.getObject(bucket, key, null, payload, receiveBuffer);
            case 204 -> adapter.deleteObject(bucket, key);
            // 这里未来可以扩充 216(Multipart) 和 230(Resumable)
            default -> throw new IllegalArgumentException("未知的 TestCaseCode: " + testCaseCode);
        };
    }

    /**
     * 高性能无锁统计累加
     */
    private void updateStats(int statusCode, long latencyNanos, long objSize) {
        // 总耗时累加
        globalStats.totalLatencyNanos.add(latencyNanos);
        // 如果接入了 HdrHistogram，可以加一句：globalStats.latencyHistogram.recordValue(latencyNanos);

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
            // 包含了返回 0 的本地客户端异常
            globalStats.clientErrorCount.increment();
        }
    }

    /**
     * 判断是否为需要加载数据的上传类用例
     */
    private boolean isUploadTask(int testCaseCode) {
        return testCaseCode == 201 || testCaseCode == 216 || testCaseCode == 230;
    }
}
