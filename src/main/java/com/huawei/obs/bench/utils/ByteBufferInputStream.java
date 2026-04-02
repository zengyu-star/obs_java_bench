package com.huawei.obs.bench.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 零拷贝字节流适配器 (Utility Plane - Zero Copy)
 * 职责：将 NIO 的 ByteBuffer 包装为传统的 InputStream，供 OBS SDK 使用。
 * 架构意义：避免了在高并发上传时将 ByteBuffer 转换为 byte[] 的内存拷贝开销。
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * 读取单个字节
     * 对应 SDK 底层对小数据块或 Header 的解析
     */
    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            return -1; // 流末尾
        }
        // ByteBuffer.get() 会自动移动 position 指针
        return buffer.get() & 0xFF;
    }

    /**
     * 批量读取字节 (性能核心)
     * 对应 SDK 将数据推送到网络 Socket 的过程
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        }

        // 计算本次实际可读取的长度，防止缓冲区溢出
        int bytesToRead = Math.min(len, buffer.remaining());
        
        // 直接从 ByteBuffer 拷贝到目标字节数组
        // 虽然这里有一次到 byte[] 的拷贝（这是 InputStream 接口限制），
        // 但由于 ByteBuffer 是 Direct 模式，它绕过了 JVM 堆内内存的二次转运。
        buffer.get(b, off, bytesToRead);
        
        return bytesToRead;
    }

    /**
     * 获取流剩余的可读字节数
     * SDK 会根据此值设置 Content-Length 头部
     */
    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    /**
     * 重置流位置
     * 用于 SDK 在遇到网络瞬断重试（Retry）时，重新从头读取数据
     */
    @Override
    public synchronized void reset() throws IOException {
        buffer.rewind();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        buffer.mark();
    }
}
