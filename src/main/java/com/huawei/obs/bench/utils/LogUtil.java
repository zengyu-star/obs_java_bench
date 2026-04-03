package com.huawei.obs.bench.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Standardized Logger Utility to match obs_c_bench style
 * Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [MODULE] message
 */
public class LogUtil {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public enum Level {
        DEBUG(0), INFO(1), WARN(2), ERROR(3);
        final int value;
        Level(int v) { this.value = v; }
    }

    private static Level currentLevel = Level.INFO;

    public static void setLogLevel(String levelStr) {
        try {
            currentLevel = Level.valueOf(levelStr.toUpperCase());
        } catch (Exception e) {
            currentLevel = Level.INFO;
        }
    }
    
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

    public static void debug(String module, String message) {
        if (currentLevel.value <= Level.DEBUG.value) {
            System.out.println("[" + getTimestamp() + "] [DEBUG] [" + module + "] " + message);
        }
    }

    public static void info(String module, String message) {
        if (currentLevel.value <= Level.INFO.value) {
            System.out.println("[" + getTimestamp() + "] [INFO ] [" + module + "] " + message);
        }
    }

    public static void warn(String module, String message) {
        if (currentLevel.value <= Level.WARN.value) {
            System.out.println("[" + getTimestamp() + "] [WARN ] [" + module + "] " + message);
        }
    }

    public static void error(String module, String message) {
        if (currentLevel.value <= Level.ERROR.value) {
            System.err.println("[" + getTimestamp() + "] [ERROR] [" + module + "] " + message);
        }
    }

    public static void config(String message) {
        System.out.println("[Config] " + message);
    }
}
