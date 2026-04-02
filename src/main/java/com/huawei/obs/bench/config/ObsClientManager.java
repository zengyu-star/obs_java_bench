package com.huawei.obs.bench.config;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OBS 客户端管理器 (Control Plane - Singleton)
 * 职责：管理多租户 ObsClient 实例及其生命周期，并针对压测场景优化底层 HTTP 连接池。
 */
public class ObsClientManager {

    private static final ObsClientManager INSTANCE = new ObsClientManager();

    // 客户端池：Key 为 username，支持多租户并发压测
    private final Map<String, ObsClient> clientPool = new ConcurrentHashMap<>();

    private ObsClientManager() {
    }

    public static ObsClientManager getInstance() {
        return INSTANCE;
    }

    /**
     * 根据配置批量初始化所有租户的 ObsClient
     * * @param config 全局压测配置
     * @param users  用户凭证列表
     */
    public void initClients(BenchConfig config, List<UserCredential> users) {
        // 1. 构建高并发压测专用配置
        ObsConfiguration obsConfig = new ObsConfiguration();
        obsConfig.setEndPoint(config.endpoint());
        obsConfig.setHttpsOnly(config.isSecure());

        // 【架构师调优】：设置底层 HTTP 连接池最大并发数。
        // 必须确保 MaxConnections >= 总线程数，否则 Worker 线程会因拿不到连接而阻塞。
        obsConfig.setMaxConnections(config.maxConnections());
        
        // 设置超时时间
        obsConfig.setSocketTimeout(config.socketTimeoutMs());
        obsConfig.setConnectionTimeout(config.connectionTimeoutMs());

        // 【架构师调优】：压测场景通常不建议开启过多的自动重试，以免掩盖真实的服务端压力波动
        obsConfig.setMaxErrorRetry(1);

        System.out.printf("[ObsClientManager] 正在为 %d 个用户初始化连接池 (MaxConnections: %d)...\n", 
                          users.size(), config.maxConnections());

        // 2. 为每个用户凭证创建独立的 ObsClient 实例
        for (UserCredential user : users) {
            ObsClient client;
            try {
                if (config.isTemporaryToken() && user.isStsToken()) {
                    // 使用临时 AK/SK/Token 初始化
                    client = new ObsClient(user.accessKey(), user.secretKey(), user.securityToken(), obsConfig);
                } else {
                    // 使用永久 AK/SK 初始化
                    client = new ObsClient(user.accessKey(), user.secretKey(), obsConfig);
                }
                clientPool.put(user.username(), client);
            } catch (Exception e) {
                System.err.printf("[致命错误] 无法为用户 %s 初始化 ObsClient: %s\n", user.username(), e.getMessage());
                throw new RuntimeException("ObsClient 初始化失败", e);
            }
        }
        System.out.println("[ObsClientManager] 客户端池初始化完成。");
    }

    /**
     * 获取指定用户的 ObsClient 实例
     */
    public ObsClient getClient(String username) {
        ObsClient client = clientPool.get(username);
        if (client == null) {
            throw new IllegalStateException("未找到用户 [" + username + "] 对应的 ObsClient 实例，请检查初始化逻辑。");
        }
        return client;
    }

    /**
     * 优雅停机：关闭所有客户端并释放底层连接池资源
     */
    public void shutdownAll() {
        System.out.println("[ObsClientManager] 正在关闭所有 ObsClient 并释放连接资源...");
        clientPool.forEach((username, client) -> {
            try {
                // 必须显示调用 close()，否则底层的 OkHttp 线程池可能不会立即退出
                client.close();
            } catch (IOException e) {
                // 停机时的异常仅记录，不影响主流程退出
                System.err.println("关闭客户端失败: " + username);
            }
        });
        clientPool.clear();
        System.out.println("[ObsClientManager] 资源已完全释放。");
    }
}
