package com.huawei.obs.bench;

import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.ConfigLoader;
import com.huawei.obs.bench.config.ConfigValidator;
import com.huawei.obs.bench.config.ObsClientManager;
import com.huawei.obs.bench.config.UserCredential;
import com.huawei.obs.bench.engine.ExecutionScheduler;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.monitor.MonitorService;

import com.huawei.obs.bench.utils.DataGenerator;
import com.huawei.obs.bench.utils.LogUtil;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.logging.log4j.core.LoggerContext;

/**
 * obs_java_bench Core Entrypoint
 * Responsibility: Assemble planes, initialize dependencies, and trigger the benchmark.
 */
public class Bootstrap {

    public static void main(String[] args) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String taskDir = "logs/task_" + timestamp;
        new File(taskDir).mkdirs();

        // [Architect's note]: Bridge the dynamic task directory to Log4j2 SdkFile Appender
        System.setProperty("logDir", taskDir);
        LoggerContext.getContext(false).reconfigure();

        printHeader(taskDir);
        LogUtil.debug("MAIN", "Initializing benchmark system with taskDir: " + taskDir);

        // 1. Smart CLI Argument Parsing
        String configPath = "config.dat";
        String usersPath = "users.dat";
        Integer testCaseCodeOverride = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("gen") || arg.equalsIgnoreCase("generate")) {
                int sizeMb = 100;
                if (i + 1 < args.length && args[i + 1].matches("\\d+(MB|mb)?")) {
                    String sizeStr = args[i + 1].replaceAll("(?i)mb", "");
                    sizeMb = Integer.parseInt(sizeStr);
                }
                try {
                    DataGenerator.generateTestFile("test_data.bin", sizeMb);
                    System.out.printf("[SUCCESS] Validation-compatible test_data.bin (%dMB) generated.\n", sizeMb);
                    System.exit(0);
                } catch (IOException e) {
                    System.err.println("[ERROR] Failed to generate test file: " + e.getMessage());
                    System.exit(1);
                }
            }
            if (arg.matches("\\d{3}")) {
                testCaseCodeOverride = Integer.parseInt(arg);
            } else if (arg.endsWith(".dat") || !arg.contains(".")) {
                if (configPath.equals("config.dat") && new File(arg).exists() && arg.contains("config")) {
                     configPath = arg;
                } else if (usersPath.equals("users.dat") && new File(arg).exists() && arg.contains("users")) {
                     usersPath = arg;
                } else {
                    if (configPath.equals("config.dat")) configPath = arg;
                    else usersPath = arg;
                }
            }
        }

        if (!new File(configPath).exists() || !new File(usersPath).exists()) {
            LogUtil.error("MAIN", "Configuration files not found!");
            System.err.printf("Please ensure %s and %s exist.\n", configPath, usersPath);
            System.exit(1);
        }

        try {
            LogUtil.info("MAIN", "Loading configuration...");
            BenchConfig config = ConfigLoader.loadConfig(configPath);
            LogUtil.setLogLevel(config.logLevel());
            if (testCaseCodeOverride != null) {
                LogUtil.info("MAIN", "CLI Override detected! Using TestCaseCode: " + testCaseCodeOverride);
                config = config.withTestCaseCode(testCaseCodeOverride);
            }

            // [Fix]: Validate AFTER all overrides are applied
            ConfigValidator.validate(config, usersPath);

            // [Logging]: ResumableThreads Summary
            int cpuCores = Runtime.getRuntime().availableProcessors();
            if (config.resumableThreads() != null) {
                if (config.resumableThreads() > cpuCores * 2) {
                    LogUtil.warn("MAIN", String.format(
                        "ResumableThreads (%d) is quite high (CPU cores: %d). Ensure your network and memory can handle it.",
                        config.resumableThreads(), cpuCores));
                }
                LogUtil.config("Resumable Global Shared Threads: " + config.resumableThreads());
            } else {
                LogUtil.config("Resumable Global Shared Threads: " + cpuCores + " (Auto-detected)");
            }

            // C-style Config Summary
            LogUtil.config(String.format("Multi-User Mode: %d Users Loaded. %d Threads/User. Total Threads: %d", 
                    config.usersCount(), config.threadsPerUser(), config.getTotalThreads()));
            LogUtil.config("Data Validation: " + (config.enableDataValidation() ? "ENABLED" : "DISABLED"));
            LogUtil.config("Detail Request Log: " + (config.enableDetailLog() ? "ENABLED" : "DISABLED"));
            if (config.objectSizeMin() == config.objectSizeMax()) {
                LogUtil.config("Object Size: " + config.objectSizeMax() + " Bytes");
            } else {
                LogUtil.config("Object Size: " + config.objectSizeMin() + " ~ " + config.objectSizeMax() + " Bytes (Dynamic)");
            }

            List<UserCredential> users;
            if (config.isTemporaryToken()) {
                File tempTokenFile = new File(usersPath);
                
                if (tempTokenFile.exists()) {
                    LogUtil.info("MAIN", "IsTemporaryToken enabled. Using credentials from: " + usersPath);
                    users = ConfigLoader.loadUsers(tempTokenFile.getAbsolutePath(), config.usersCount());
                } else {
                    LogUtil.info("MAIN", "IsTemporaryToken enabled. " + usersPath + " not found. Fetching STS tokens for " + config.usersCount() + " users...");

                    ProcessBuilder pb = new ProcessBuilder("python3", "generate_temp_ak_sk.py", String.valueOf(config.usersCount()));
                    pb.inheritIO();
                    Process p = pb.start();
                    int exitCode = p.waitFor();

                    if (exitCode != 0) {
                        throw new RuntimeException("FATAL: Failed to generate temporary credentials. Python script exited with code " + exitCode);
                    }

                    if (!tempTokenFile.exists()) {
                        // If the script generated temptoken.dat instead of the specified usersPath, try to find it
                        File defaultTempFile = new File("temptoken.dat");
                        if (defaultTempFile.exists()) {
                            tempTokenFile = defaultTempFile;
                        } else {
                            throw new RuntimeException("FATAL: No temporary credentials generated.");
                        }
                    }
                    users = ConfigLoader.loadUsers(tempTokenFile.getAbsolutePath(), config.usersCount());
                }
            } else {
                users = ConfigLoader.loadUsers(usersPath, config.usersCount());
            }

            LogUtil.info("MAIN", "Initializing connection pool...");
            ObsClientManager clientManager = ObsClientManager.getInstance();
            clientManager.initClients(config, users);

            // [TestCase 230 Enhancement]: Automatic Test Data Generation & Checkpoint Setup
            if (config.testCaseCode() == 230 || config.testCaseCode() == 216) {
                File cpDir = new File("upload_checkpoint");
                if (!cpDir.exists()) cpDir.mkdirs();
                
                if (config.testCaseCode() == 230) {
                    String uploadPath = config.uploadFilePath();
                    if (uploadPath == null || uploadPath.isEmpty()) {
                        uploadPath = "test_data.bin";
                        // Re-fetch config with default path if empty
                        config = ConfigLoader.loadConfig(configPath).withUploadFilePath(uploadPath);
                    }
                    
                    File uploadFile = new File(uploadPath);
                    if (!uploadFile.exists()) {
                        LogUtil.warn("MAIN", "UploadFilePath not found. Triggering auto-generation...");
                        DataGenerator.generateTestFile(uploadPath, 100); // Default 100MB
                    }
                }
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LogUtil.warn("MAIN", "Exit signal received. Releasing resources...");
                clientManager.shutdownAll();
                LogUtil.info("MAIN", "Resources fully released.");
            }, "Obs-Shutdown-Hook"));

            BenchmarkStats globalStats = new BenchmarkStats();
            MonitorService monitorService = new MonitorService(globalStats, config, taskDir);

            ExecutionScheduler scheduler = new ExecutionScheduler(config, globalStats, taskDir);
            scheduler.startBenchmark(users, monitorService);

            LogUtil.info("MAIN", "Execution report saved to: " + taskDir + "/brief.txt");

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("\n[Startup Failed] Benchmark was interrupted!");
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("\n[System Error] " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printHeader(String taskDir) {
        System.out.println("---------------------------------------------------------------");
        LogUtil.info("MAIN", "--- OBS Java Benchmark Tool ---");
        LogUtil.info("MAIN", "Task Output Dir: " + taskDir);
        System.out.println("---------------------------------------------------------------");
    }
}
