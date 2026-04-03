package com.huawei.obs.bench.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Standardized Logger Utility to match obs_c_bench style
 * Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [MODULE] message
 */
public class LogUtil {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // [Extreme Performance]: Cache the formatted timestamp to avoid heavy date formatting on every log call
    private static volatile String cachedTimestamp = "";
    private static volatile long lastSecond = 0;

    public static String getTimestamp() {
        long currentMillis = System.currentTimeMillis();
        long currentSecond = currentMillis / 1000;
        
        if (currentSecond != lastSecond) {
            synchronized (LogUtil.class) {
                if (currentSecond != lastSecond) {
                    cachedTimestamp = LocalDateTime.now().format(DATE_FORMATTER);
                    lastSecond = currentSecond;
                }
            }
        }
        return cachedTimestamp;
    }

    public static void info(String module, String message) {
        System.out.println("[" + getTimestamp() + "] [INFO ] [" + module + "] " + message);
    }

    public static void warn(String module, String message) {
        System.out.println("[" + getTimestamp() + "] [WARN ] [" + module + "] " + message);
    }

    public static void error(String module, String message) {
        System.err.println("[" + getTimestamp() + "] [ERROR] [" + module + "] " + message);
    }

    public static void config(String message) {
        System.out.println("[Config] " + message);
    }
}
