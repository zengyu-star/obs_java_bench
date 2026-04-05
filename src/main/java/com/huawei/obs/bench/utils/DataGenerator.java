package com.huawei.obs.bench.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Benchmark Data Generator
 * Responsibility: Generate dummy test files for Multipart/Resumable uploads when no local file is provided.
 */
public class DataGenerator {

    private static final int PATTERN_SIZE = 1024 * 1024; // 1MB Pattern
    private static final long FIXED_SEED = 0x1234567890ABCDEFL;

    /**
     * Generates a deterministic test file using SplitMix64 algorithm.
     * This is consistent with the benchmark tool's data validation logic.
     * @param filePath Target path
     * @param sizeMb File size in MB
     * @throws IOException If write fails
     */
    public static void generateTestFile(String filePath, int sizeMb) throws IOException {
        LogUtil.info("GEN", "Generating " + sizeMb + "MB validation-compatible test file at: " + filePath);
        
        byte[] pattern = new byte[PATTERN_SIZE];
        ByteBuffer pb = ByteBuffer.wrap(pattern);
        long state = FIXED_SEED;
        
        // Replicate exactly the logic in WorkerContext.fillPatternBuffer()
        // to ensure bitstream consistency for data validation.
        while (pb.remaining() >= 8) {
            state = HashUtil.splitMix64(state);
            pb.putLong(state);
        }
        while (pb.hasRemaining()) {
            state = HashUtil.splitMix64(state);
            pb.put((byte) state);
        }

        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file))) {
            for (int i = 0; i < sizeMb; i++) {
                bos.write(pattern);
            }
        }
        LogUtil.info("GEN", "Test file generated successfully.");
    }
}
