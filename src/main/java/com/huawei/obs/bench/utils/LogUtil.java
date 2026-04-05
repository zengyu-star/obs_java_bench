package com.huawei.obs.bench.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Standardized Logger Utility to match obs_c_bench style
 * Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [MODULE] message
 * Re-architected to bridge with Log4j2 for unified file output.
 */
public class LogUtil {
    private static final Logger LOGGER = LogManager.getLogger("com.huawei.obs.bench");

    public static void setLogLevel(String levelStr) {
        try {
            Level level = Level.valueOf(levelStr.toUpperCase());
            // 1. Tool-level logs (bench.log)
            Configurator.setLevel("com.huawei.obs.bench", level);
            // 2. SDK-level and HttpClient logs (sdk.log)
            Configurator.setLevel("com.obs", level);
            Configurator.setLevel("org.apache.http", level);
        } catch (Exception e) {
            Configurator.setLevel("com.huawei.obs.bench", Level.INFO);
            Configurator.setLevel("com.obs", Level.INFO);
            Configurator.setLevel("org.apache.http", Level.INFO);
        }
    }

    public static String getTimestamp() {
        return java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public static void debug(String module, String message) {
        LOGGER.debug("[" + module + "] " + message);
    }

    public static void info(String module, String message) {
        LOGGER.info("[" + module + "] " + message);
    }

    public static void warn(String module, String message) {
        LOGGER.warn("[" + module + "] " + message);
    }

    public static void error(String module, String message) {
        LOGGER.error("[" + module + "] " + message);
    }

    public static void config(String message) {
        // Config messages are always INFO level in our layout
        LOGGER.info("[Config] " + message);
    }
}
