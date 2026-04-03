package com.huawei.obs.bench.engine;

import com.huawei.obs.bench.adapter.IObsClientAdapter;
import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.UserCredential;

import java.nio.ByteBuffer;

/**
 * Worker 线程执行上下文 (Thread-Local)
 * 隔离了线程间的状态，避免高并发下的锁竞争与变量逃逸。
 */
public class WorkerContext {

    private final int threadId;
    private final BenchConfig config;
    private final UserCredential credential;
    
    // 真实发流或 Mock 发流的适配器
    private final IObsClientAdapter adapter;
    
    // 独立计算的 Target Bucket 名称
    private final String targetBucket;

    // 核心大杀器：预分配的零拷贝直接内存 (Direct Memory)
    // 绝对不在压测循环里 new byte[]
    private ByteBuffer patternBuffer;
    
    // 数据校验专用读缓冲，极小的一块直接内存 (例如 64KB)
    private ByteBuffer receiveBuffer;

    public WorkerContext(int threadId, BenchConfig config, UserCredential credential, IObsClientAdapter adapter) {
        this.threadId = threadId;
        this.config = config;
        this.credential = credential;
        this.adapter = adapter;
        
        // 【关键逻辑】：动态确定目标桶名
        if (config.bucketNameFixed() != null && !config.bucketNameFixed().isBlank()) {
            // 模式 A: 使用全局固定桶名
            this.targetBucket = config.bucketNameFixed();
        } else {
            // 模式 B: 使用租户独立的动态桶名 {ak_lowercase}.{prefix}
            this.targetBucket = credential.accessKey().toLowerCase() + "." + config.bucketNamePrefix();
        }
    }

    // ================== Getters & Setters ==================

    public int getThreadId() { return threadId; }
    public BenchConfig getConfig() { return config; }
    public UserCredential getCredential() { return credential; }
    public IObsClientAdapter getAdapter() { return adapter; }
    public String getTargetBucket() { return targetBucket; }
    
    public ByteBuffer getPatternBuffer() { return patternBuffer; }
    public void setPatternBuffer(ByteBuffer patternBuffer) {
        this.patternBuffer = patternBuffer;
    }

    public ByteBuffer getReceiveBuffer() { return receiveBuffer; }
    public void setReceiveBuffer(ByteBuffer receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
    }

    /**
     * 高性能预填充伪随机数据 (Zero-GC LCG)
     * 使用 SplitMix64 算法根据固定种子生成确定性的、无热点的比特流。
     */
    public void fillPatternBuffer() {
        if (patternBuffer == null) {
            return;
        }
        long seed = 0x1234567890ABCDEFL ^ threadId; // 线程私有的固定种子
        patternBuffer.clear();
        while (patternBuffer.remaining() >= 8) {
            seed = com.huawei.obs.bench.utils.HashUtil.splitMix64(seed);
            patternBuffer.putLong(seed);
        }
        // 填剩余零散字节
        while (patternBuffer.hasRemaining()) {
            seed = com.huawei.obs.bench.utils.HashUtil.splitMix64(seed);
            patternBuffer.put((byte) seed);
        }
        // 写入完成，切换为读模式供上传或后续对拍
        patternBuffer.flip();
    }
}
