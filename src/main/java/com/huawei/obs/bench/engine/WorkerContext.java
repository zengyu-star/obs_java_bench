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

    public WorkerContext(int threadId, BenchConfig config, UserCredential credential, IObsClientAdapter adapter) {
        this.threadId = threadId;
        this.config = config;
        this.credential = credential;
        this.adapter = adapter;
        // 使用配置文件中的桶名
        this.targetBucket = config.bucketName(); 
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
}
