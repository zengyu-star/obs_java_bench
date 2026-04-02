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
     * * @param sb            Worker 线程复用的 StringBuilder，避免产生中间 String 对象
     * @param prefix        对象前缀配置
     * @param threadId      当前发流的线程 ID
     * @param seq           当前线程的请求序号
     * @param hashEnabled   是否开启哈希前缀打散
     */
    public static void generateObjectKey(StringBuilder sb, String prefix, int threadId, long seq, boolean hashEnabled) {
        // 1. 清空复用的缓冲区
        sb.setLength(0);
        
        // 2. 拼接前缀
        sb.append(prefix);

        // 3. 核心哈希打散逻辑
        if (hashEnabled) {
            // 利用线程ID和序号作为强确定性的 Seed (高32位放线程ID，低32位与seq异或)
            long seed = ((long) threadId << 32) ^ seq;
            
            // 计算第一段 64 位哈希
            long hash1 = splitMix64(seed);
            // 将第一段哈希按位取反作为 Seed，计算第二段 64 位哈希 (组合成伪 128 位)
            long hash2 = splitMix64(~hash1); 

            // 极速追加 32 字符的十六进制串 (绝对无锁、无 GC)
            appendHexFast(sb, hash1);
            appendHexFast(sb, hash2);
            sb.append("_");
        }
        
        // 4. 追加可读的尾缀，便于日志排查
        sb.append("T").append(threadId).append("_").append(seq);
    }

    /**
     * Java 版的 SplitMix64 散列算法
     * 特点：极简的位运算，极其刚猛的雪崩效应（输入改变 1 bit，输出改变约 50% bit）。
     * 完全无状态、无锁、极高吞吐。
     */
    private static long splitMix64(long seed) {
        long z = (seed + 0x9e3779b97f4a7c15L);
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /**
     * 极速十六进制转换器
     * 架构师防坑：绝对不要用 Long.toHexString(value)，它底层每次都会 new String() 和 char[]，
     * 在几千万次的高并发循环下会瞬间引爆堆内存。
     */
    private static void appendHexFast(StringBuilder sb, long value) {
        // 64 位整数，每次右移 4 位取后 4 位，循环 16 次
        for (int i = 60; i >= 0; i -= 4) {
            int digit = (int) ((value >>> i) & 0xF);
            sb.append(HEX_ARRAY[digit]);
        }
    }
}
