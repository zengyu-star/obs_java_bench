package com.huawei.obs.bench.config;

import com.huawei.obs.bench.utils.LogUtil;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Configuration Validator (Fool-proofing Logic)
 * Ensures all parameters are valid before the benchmark starts.
 */
public class ConfigValidator {

    private static final Set<String> ALLOWED_PROTOCOLS = new HashSet<>(List.of("http", "https"));
    private static final Set<String> ALLOWED_LOG_LEVELS = new HashSet<>(List.of("DEBUG", "INFO", "CONFIG", "WARN", "ERROR"));

    public static void validate(BenchConfig config, String usersPath) {
        List<String> errors = new ArrayList<>();

        // 1. Global Requirements
        validateGlobal(config, usersPath, errors);

        // 2. TestCase-Specific Requirements
        validateTestCase(config.testCaseCode(), config, errors);

        // 3. Recursive Validation for MixMode (900)
        if (config.testCaseCode() == 900) {
            if (config.mixOperations() == null || config.mixOperations().length == 0) {
                errors.add("MixMode (900) requires 'MixOperation' to be configured.");
            } else {
                for (int op : config.mixOperations()) {
                    validateTestCase(op, config, errors);
                }
                if (config.mixLoopCount() <= 0) {
                    errors.add("MixLoopCount must be > 0 for MixMode (900).");
                }
            }
        }

        if (!errors.isEmpty()) {
            reportErrorsAndExit(errors);
        }
    }

    private static void validateGlobal(BenchConfig config, String usersPath, List<String> errors) {
        if (config.endpoint() == null || config.endpoint().isBlank()) {
            errors.add("Endpoint cannot be empty.");
        }
        
        // UsersCount > 1
        if (config.usersCount() <= 1) {
            errors.add("UsersCount must be an integer greater than 1.");
        }
        
        // UsersCount <= lines in users.dat
        int actualUserCount = countValidUsers(usersPath);
        if (config.usersCount() > actualUserCount) {
            errors.add(String.format("UsersCount (%d) exceeds the number of valid credentials in %s (%d).", 
                       config.usersCount(), usersPath, actualUserCount));
        }

        if (config.threadsPerUser() <= 0) {
            errors.add("ThreadsPerUser must be > 0.");
        }
        if (config.maxConnections() <= 0) {
            errors.add("MaxConnections must be > 0.");
        }
        if (config.socketTimeoutMs() <= 0) {
            errors.add("SocketTimeoutMs must be > 0.");
        }
        if (config.connectionTimeoutMs() <= 0) {
            errors.add("ConnectionTimeoutMs must be > 0.");
        }
        
        // Mock Parameters Check
        if (config.isMockMode()) {
            if (config.mockLatencyMs() < 0) {
                errors.add("MockLatencyMs must be >= 0.");
            }
            if (config.mockErrorRate() < 0 || config.mockErrorRate() > 10000) {
                errors.add("MockErrorRate must be between 0 and 10000.");
            }
        }

        // Protocol Check
        if (!ALLOWED_PROTOCOLS.contains(config.protocol().toLowerCase())) {
            errors.add("Protocol must be 'http' or 'https' (Current: " + config.protocol() + ").");
        }

        // LogLevel Check
        if (!ALLOWED_LOG_LEVELS.contains(config.logLevel().toUpperCase())) {
            errors.add("LogLevel must be one of DEBUG, INFO, CONFIG, WARN, ERROR (Current: " + config.logLevel() + ").");
        }

        // Bucket Check
        if (isEmpty(config.bucketNameFixed()) && isEmpty(config.bucketNamePrefix())) {
            errors.add("Either BucketNameFixed or BucketNamePrefix must be specified.");
        }
    }

    private static void validateTestCase(int code, BenchConfig config, List<String> errors) {
        // Name Constraints for 2xx
        if (isObjectOperation(code)) {
            if (isEmpty(config.objectNameFixed()) && isEmpty(config.keyPrefix())) {
                errors.add(String.format("TestCase %d requires either ObjectNameFixed or KeyPrefix.", code));
            }
            
            // Explicit flag checks
            if (config.objNamePatternHash() == null) {
                errors.add(String.format("TestCase %d requires 'ObjNamePatternHash' to be explicitly configured (true/false).", code));
            }
            
            // EnableDataValidation check (Excluded for 204)
            if (code != 204 && config.enableDataValidation() == null) {
                errors.add(String.format("TestCase %d requires 'EnableDataValidation' to be explicitly configured (true/false).", code));
            }
        }

        switch (code) {
            case 101:
                if (isEmpty(config.bucketLocation())) {
                    errors.add("BucketLocation must be specified for TestCase 101.");
                }
                break;
            case 201:
                if (config.objectSizeMin() <= 0 || config.objectSizeMax() <= 0) {
                    errors.add("ObjectSize must be > 0 for PutObject (201).");
                }
                if (config.objectSizeMin() > config.objectSizeMax()) {
                    errors.add("ObjectSizeMin cannot be greater than ObjectSizeMax.");
                }
                break;
            case 216:
                checkFile(config.uploadFilePath(), errors, "Multipart (216)");
                if (config.partSize() < 5242880) {
                    errors.add("PartSize must be at least 5242880 (5MB) for Multipart (216).");
                }
                if (config.partsForEachUploadID() <= 0) {
                    errors.add("PartsForEachUploadID must be > 0 for Multipart (216).");
                }
                break;
            case 230:
                checkFile(config.uploadFilePath(), errors, "Resumable (230)");
                if (config.partSize() < 5242880) {
                    errors.add("PartSize must be at least 5242880 (5MB) for Resumable (230).");
                }
                if (config.resumableThreads() != null && config.resumableThreads() <= 0) {
                    errors.add("ResumableThreads must be > 0 for Resumable (230).");
                }
                break;
        }
    }

    private static boolean isObjectOperation(int code) {
        return code == 201 || code == 202 || code == 204 || code == 216 || code == 230;
    }

    private static boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static void checkFile(String path, List<String> errors, String context) {
        if (isEmpty(path)) {
            errors.add("UploadFilePath is not specified for " + context + ".");
            return;
        }
        File f = new File(path);
        if (!f.exists()) {
            errors.add("UploadFilePath '" + path + "' does not exist for " + context + ".");
        } else if (!f.isFile()) {
            errors.add("UploadFilePath '" + path + "' is not a file.");
        }
    }

    private static int countValidUsers(String usersPath) {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(usersPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                if (line.split(",").length >= 3) count++;
            }
        } catch (IOException e) {
            // Error will be caught during actual loading; validator just reports missing file
            return 0;
        }
        return count;
    }

    private static void reportErrorsAndExit(List<String> errors) {
        System.err.println("\n[Configuration Error] The following parameters are invalid:");
        for (int i = 0; i < errors.size(); i++) {
            System.err.println((i + 1) + ". " + errors.get(i));
        }
        System.err.println("\nPlease fix config.dat and try again.\n");
        System.exit(1);
    }
}
