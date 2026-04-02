package com.huawei.obs.bench;

import com.huawei.obs.bench.config.BenchConfig;
import com.huawei.obs.bench.config.ConfigLoader;
import com.huawei.obs.bench.config.ObsClientManager;
import com.huawei.obs.bench.config.UserCredential;
import com.huawei.obs.bench.engine.ExecutionScheduler;
import com.huawei.obs.bench.monitor.BenchmarkStats;
import com.huawei.obs.bench.monitor.MonitorService;

import java.io.File;
import java.util.List;

/**
 * obs_java_bench 核心启动类 (The Entrypoint)
 * 职责：组装四大平面，初始化依赖项，扣动压测总开关。
 */
public class Bootstrap {

    public static void main(String[] args) {
        printBanner();

        // 1. 确定配置文件路径 (默认读取 conf 目录，支持通过参数覆盖)
        String configPath = args.length > 0 ? args[0] : "conf/config.dat";
        String usersPath = args.length > 1 ? args[1] : "conf/users.dat";

        if (!new File(configPath).exists() || !new File(usersPath).exists()) {
            System.err.println("[致命错误] 找不到配置文件！");
            System.err.printf("请确保 %s 和 %s 文件存在。\n", configPath, usersPath);
            System.exit(1);
        }

        try {
            // ==============================================================
            // Phase 1: 控制面 (Control Plane) 初始化
            // ==============================================================
            System.out.println("[Bootstrap] 正在加载配置文件...");
            BenchConfig config = ConfigLoader.loadConfig(configPath);
            List<UserCredential> users = ConfigLoader.loadUsers(usersPath, config.usersCount());

            System.out.println("[Bootstrap] 正在初始化底层连接池...");
            // 单例管理器：集中初始化所有的 ObsClient 并接管其生命周期
            ObsClientManager clientManager = ObsClientManager.getInstance();
            clientManager.initClients(config, users);

            // 注册 JVM 钩子：当接收到 Ctrl+C 或 kill 信号时，强制释放底层的网络连接资源
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[ShutdownHook] 收到退出信号，正在优雅释放资源...");
                clientManager.shutdownAll();
            }, "Obs-Shutdown-Hook"));

            // ==============================================================
            // Phase 2: 数据面 (Data Plane) 初始化
            // ==============================================================
            BenchmarkStats globalStats = new BenchmarkStats();
            MonitorService monitorService = new MonitorService(globalStats);

            // ==============================================================
            // Phase 3: 调度面 (Scheduler Plane) 组装并点火
            // ==============================================================
            ExecutionScheduler scheduler = new ExecutionScheduler(config, globalStats);
            
            // 将控制权交给调度器，主线程将在此阻塞直到压测结束
            scheduler.startBenchmark(users, monitorService);

        } catch (IllegalArgumentException | IllegalStateException e) {
            // 捕获配置错误，拒绝打印长篇大论的堆栈，直接给测试人员最清晰的错误提示
            System.err.println("\n[启动失败 - 配置或状态异常] " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("\n[启动失败] 压测主流程被强行中断！");
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("\n[系统异常] 发生未捕获的严重错误：");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 打印极客风的启动 Banner
     */
    private static void printBanner() {
        System.out.println("===============================================================");
        System.out.println("   ____  ____  _____     __                  ____                  __  ");
        System.out.println("  / __ \\/ __ )/ ___/    / /___ __   ______ _/ __ )___  ____  _____/ /_ ");
        System.out.println(" / / / / __  /\\__ \\_   / / __ `/ | / / __ `/ __  / _ \\/ __ \\/ ___/ __ \\");
        System.out.println("/ /_/ / /_/ /___/ / | / / /_/ /| |/ / /_/ / /_/ /  __/ / / / /__/ / / /");
        System.out.println("\\____/_____//____/| |/ /\\__,_/ |___/\\__,_/_____/\\___/_/ /_/\\___/_/ /_/ ");
        System.out.println("                  |___/                                                ");
        System.out.println("       HUAWEI Cloud OBS High-Performance Benchmark Tool (Java Edition)");
        System.out.println("===============================================================");
    }
}
