package com.huawei.obs.bench.utils;

/**
 * 压测核心散列算法工具类
 * 采用 SplitMix64 算法确保对象名的强一致性与高离散度，彻底消除云端热点。
 * 核心架构要求：本类的所有方法必须是 Zero-GC（零垃圾回收），严禁内部 new 对象。
 */
public class HashUtil {

    // 预加载 Hex 字符字典，加速位运算映射
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    /**
     * 生成高离散、无热点且确定性的 ObjectKey
     *
     * @param sb            Worker 线程复用的 StringBuilder，避免产生中间 String 对象
     * @param keyPrefix     对象名前缀
     * @param username      执行压测的用户名
     * @param threadId      当前发流的并发编号 (线程 ID)
     * @param seq           当前并发中的操作序号
     * @param hashEnabled   是否开启哈希前缀打散
     */
    public static void generateObjectKey(StringBuilder sb, String keyPrefix, String username, int threadId, long seq, boolean hashEnabled) {
        // 1. 清空复用的缓冲区
        sb.setLength(0);

        // 2. 核心哈希打散逻辑
        if (hashEnabled) {
            // 利用并发编号和序号作为强确定性的 Seed
            long seed = ((long) threadId << 32) ^ seq;

            // 极致性能：连续生成 128 位数据转换为 32 位十六进制前缀
            appendHex32(sb, seed);
            sb.append("-");
        }

        // 3. 按照用户要求拼接：KeyPrefix-用户名-并发编号-序号
        sb.append(keyPrefix).append("-")
          .append(username).append("-")
          .append(threadId).append("-")
          .append(seq);
    }

    /**
     * Java 版的 SplitMix64 散列算法
     * 特点：极简的位运算，极其刚猛的雪崩效应（输入改变 1 bit，输出改变约 50% bit）。
     * 完全无状态、无锁、极高吞吐。
     */
    public static long splitMix64(long seed) {
        long z = (seed + 0x9e3779b97f4a7c15L);
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /**
     * 高性能生成 32 位十六进制前缀 (128 位哈希等效长度)
     */
    private static void appendHex32(StringBuilder sb, long seed) {
        // 第一轮：生成高 64 位 (16 字符)
        long hash1 = splitMix64(seed);
        appendHex16(sb, hash1);
        
        // 第二轮：使用上一次哈希作为种子生成低 64 位 (16 字符)
        long hash2 = splitMix64(hash1);
        appendHex16(sb, hash2);
    }

    /**
     * 极速 16 位十六进制转换器
     */
    private static void appendHex16(StringBuilder sb, long value) {
        for (int i = 60; i >= 0; i -= 4) {
            int digit = (int) ((value >>> i) & 0xF);
            sb.append(HEX_ARRAY[digit]);
        }
    }
}
