package com.huawei.obs.bench.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Config and Credential Loader
 * Responsible for parsing config.dat (Properties) and users.dat (CSV)
 */
public class ConfigLoader {

    /**
     * Load Global Benchmark Configuration
     * @param configPath Path to config.dat
     * @return Strongly-typed BenchConfig object
     */
    public static BenchConfig loadConfig(String configPath) {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configPath)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("[Fatal] Cannot read configuration file: " + configPath + ". Please check if it exists!", e);
        }

        try {
            // Parse basic fields
            String endpoint = props.getProperty("Endpoint", "obs.cn-north-4.myhuaweicloud.com").trim();
            String protocol = props.getProperty("Protocol", "https").trim();
            boolean isTemporaryToken = Boolean.parseBoolean(props.getProperty("IsTemporaryToken", "false"));

            int maxConnections = Integer.parseInt(props.getProperty("MaxConnections", "2000"));
            int socketTimeoutMs = Integer.parseInt(props.getProperty("SocketTimeoutMs", "60000"));
            int connectionTimeoutMs = Integer.parseInt(props.getProperty("ConnectionTimeoutMs", "30000"));

            int usersCount = Integer.parseInt(props.getProperty("UsersCount", "1"));
            int threadsPerUser = Integer.parseInt(props.getProperty("ThreadsPerUser", "1"));
            long runSeconds = parseRunSeconds(props.getProperty("RunSeconds"));
            long requestsPerThread = parseRequestsPerThread(props.getProperty("RequestsPerThread"));

            int testCaseCode = Integer.parseInt(props.getProperty("TestCaseCode", "201"));

            String bucketNameFixed = props.getProperty("BucketNameFixed", "").trim();
            String bucketNamePrefix = props.getProperty("BucketNamePrefix", "bench-bucket").trim();
            String keyPrefix = props.getProperty("KeyPrefix", "bench_test_").trim();
            long objectSize = Long.parseLong(props.getProperty("ObjectSize", "1048576"));
            long partSize = Long.parseLong(props.getProperty("PartSize", "5242880"));

            boolean objNamePatternHash = Boolean.parseBoolean(props.getProperty("ObjNamePatternHash", "true"));
            boolean enableDataValidation = Boolean.parseBoolean(props.getProperty("EnableDataValidation", "false"));
            boolean enableDetailLog = Boolean.parseBoolean(props.getProperty("EnableDetailLog", "false"));
            boolean isMockMode = Boolean.parseBoolean(props.getProperty("IsMockMode", "false"));

            // Parse Mixed Mode 900 fields
            int[] mixOperations = parseMixOperations(props.getProperty("MixOperation", ""));
            long mixLoopCount = parseLongOrDefault(props.getProperty("MixLoopCount", ""), 0);

            return new BenchConfig(
                endpoint, protocol, isTemporaryToken,
                maxConnections, socketTimeoutMs, connectionTimeoutMs,
                usersCount, threadsPerUser, runSeconds, requestsPerThread,
                testCaseCode,
                bucketNameFixed, bucketNamePrefix, keyPrefix, objectSize, partSize,
                objNamePatternHash, enableDataValidation, enableDetailLog, isMockMode,
                mixOperations, mixLoopCount
            );
        } catch (NumberFormatException e) {
            throw new RuntimeException("[Fatal] Invalid number format in config.dat, please check values!", e);
        }
    }

    private static int[] parseMixOperations(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new int[0];
        }
        String[] parts = raw.split(",");
        int[] ops = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                ops[i] = Integer.parseInt(parts[i].trim());
            } catch (NumberFormatException e) {
                // Ignore invalid codes
            }
        }
        return ops;
    }

    private static long parseLongOrDefault(String raw, long defaultValue) {
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Load Multi-tenant Credentials
     * @param usersPath Path to users.dat
     * @param requiredCount UsersCount declared in config
     * @return List of credentials
     */
    public static List<UserCredential> loadUsers(String usersPath, int requiredCount) {
        List<UserCredential> users = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(usersPath))) {
            String line;
            int lineNumber = 0;
            
            while ((line = reader.readLine()) != null && users.size() < requiredCount) {
                lineNumber++;
                line = line.trim();
                
                // Skip empty lines and comments starting with #
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // Format: username,ak,sk,[sts_token]
                String[] parts = line.split(",");
                if (parts.length < 3) {
                    System.err.println("[WARN] users.dat line " + lineNumber + " format incorrect, skipped: " + line);
                    continue;
                }

                String username = parts[0].trim();
                String ak = parts[1].trim();
                String sk = parts[2].trim();
                // If temporary credentials are enabled, read the 4th column (security_token) if present
                String token = parts.length > 3 ? parts[3].trim() : null;
                
                // originalAk used for potential bucket name construction based on AK
                String originalAk = ak;

                users.add(new UserCredential(username, ak, sk, token, originalAk));
            }
        } catch (IOException e) {
            throw new RuntimeException("[Fatal] Cannot read users file: " + usersPath, e);
        }

        // Validation: Required users count vs actual parsed credentials
        if (users.size() < requiredCount) {
            throw new RuntimeException(String.format(
                "[Fatal] config.dat requires %d users, but only %d valid users found in users.dat!", 
                requiredCount, users.size()
            ));
        }

        return users;
    }

    /**
     * Parses RunSeconds parameter: Must be a positive integer or empty (returns 0)
     */
    private static long parseRunSeconds(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return 0; // Empty means no duration limit
        }
        try {
            long value = Long.parseLong(rawValue.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("[Fatal] RunSeconds must be empty or a positive integer, 0 is not allowed (Current input: \"" + rawValue + "\")");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[Fatal] Invalid RunSeconds format, must be empty or a positive integer (Current input: \"" + rawValue + "\")");
        }
    }

    /**
     * Parses RequestsPerThread parameter: Must be a non-negative integer
     */
    private static long parseRequestsPerThread(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return 0; // Default to 0 (unlimited)
        }
        try {
            long value = Long.parseLong(rawValue.trim());
            if (value < 0) {
                throw new IllegalArgumentException("[Fatal] RequestsPerThread must be a non-negative integer (Current value: " + value + ")");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[Fatal] Invalid RequestsPerThread format, must be a non-negative integer (Current input: \"" + rawValue + "\")");
        }
    }
}
