package com.huawei.obs.bench.utils;

/**
 * Core Hashing Utility for Benchmarking
 * Implements the SplitMix64 algorithm to ensure strong consistency and high dispersion of object names,
 * effectively eliminating cloud-side hotspots.
 * Core Architectural Requirement: All methods must be Zero-GC (zero garbage collection); internal 'new' operations are strictly prohibited.
 */
public class HashUtil {

    // Pre-loaded Hex character dictionary to accelerate bitwise mapping
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    /**
     * Generates a high-dispersion, hotspot-free, and deterministic ObjectKey
     *
     * @param sb            StringBuilder reused by the Worker thread to avoid intermediate String objects
     * @param keyPrefix     Prefix for the object name
     * @param username      Username performing the benchmark
     * @param threadId      Concurrency ID (Thread ID) for the current flow
     * @param seq           Sequence number within the current concurrency
     * @param hashEnabled   Whether to enable hash prefix dispersion
     */
    public static void generateObjectKey(StringBuilder sb, String keyPrefix, String username, int threadId, long seq, boolean hashEnabled) {
        // 1. Clear the reused buffer
        sb.setLength(0);

        // 2. Core Hash Dispersion Logic
        if (hashEnabled) {
            // Use concurrency ID and sequence as a strongly deterministic Seed
            long seed = ((long) threadId << 32) ^ seq;

            // Extreme Performance: Generate 128-bit data converted to a 32-character hex prefix
            appendHex32(sb, seed);
            sb.append("-");
        }

        // 3. Concatenate according to requirements: KeyPrefix-Username-ThreadID-Sequence
        sb.append(keyPrefix).append("-")
          .append(username).append("-")
          .append(threadId).append("-")
          .append(seq);
    }

    /**
     * Java Implementation of the SplitMix64 Hashing Algorithm
     * Features: Minimal bitwise operations; extremely strong avalanche effect (1-bit input change leads to approx. 50% bit change in output).
     * Completely stateless, lock-free, and exceptionally high throughput.
     */
    public static long splitMix64(long seed) {
        long z = (seed + 0x9e3779b97f4a7c15L);
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /**
     * High-performance generation of a 32-character hex prefix (equivalent to 128-bit hash length)
     */
    private static void appendHex32(StringBuilder sb, long seed) {
        // Round 1: Generate high 64 bits (16 characters)
        long hash1 = splitMix64(seed);
        appendHex16(sb, hash1);
        
        // Round 2: Generate low 64 bits (16 characters) using the previous hash as seed
        long hash2 = splitMix64(hash1);
        appendHex16(sb, hash2);
    }

    /**
     * High-speed 16-character hex converter
     */
    private static void appendHex16(StringBuilder sb, long value) {
        for (int i = 60; i >= 0; i -= 4) {
            int digit = (int) ((value >>> i) & 0xF);
            sb.append(HEX_ARRAY[digit]);
        }
    }
}
