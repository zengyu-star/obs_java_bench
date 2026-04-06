package com.huawei.obs.bench.config;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * OBS Client Manager (Control Plane - Singleton)
 * Responsibility: Manage Multi-tenant ObsClient instances and optimize HTTP connection pool.
 */
public class ObsClientManager {

    private static final ObsClientManager INSTANCE = new ObsClientManager();

    // Client pool: Key is username, supporting multi-tenant concurrent benchmarking
    private final Map<String, ObsClient> clientPool = new ConcurrentHashMap<>();

    // [New]: Global shared thread pool for resumable upload tasks (Case 230)
    private ExecutorService resumableExecutor;

    private ObsClientManager() {
    }

    public static ObsClientManager getInstance() {
        return INSTANCE;
    }

    /**
     * Initialize ObsClient pool for multiple tenants
     * @param config Global benchmark configuration
     * @param users  List of user credentials
     */
    public void initClients(BenchConfig config, List<UserCredential> users) {
        // 1. Build configuration optimized for high-concurrency benchmarking
        ObsConfiguration obsConfig = new ObsConfiguration();
        obsConfig.setEndPoint(config.endpoint());
        obsConfig.setHttpsOnly("https".equalsIgnoreCase(config.protocol()));

        // [Architect Tuning]: Set max concurrent connections for the underlying HTTP pool.
        // Must ensure MaxConnections >= total threads, otherwise Worker threads will block.
        obsConfig.setMaxConnections(config.maxConnections());
        
        // Set timeout values
        obsConfig.setSocketTimeout(config.socketTimeoutMs());
        obsConfig.setConnectionTimeout(config.connectionTimeoutMs());

        // [Architect Tuning]: Excessive auto-retries are not recommended in benchmarking 
        // to avoid masking real server-side pressure fluctuations.
        obsConfig.setMaxErrorRetry(1);

        System.out.printf("[ObsClientManager] Initializing connection pool for %d users (MaxConnections: %d)...\n", 
                          users.size(), config.maxConnections());

        // 2. Create independent ObsClient instances for each user credential
        for (UserCredential user : users) {
            ObsClient client;
            try {
                if (config.isTemporaryToken() && user.isStsToken()) {
                    // Initialize using temporary AK/SK/Token
                    client = new ObsClient(user.accessKey(), user.secretKey(), user.securityToken(), obsConfig);
                } else {
                    // Initialize using permanent AK/SK
                    client = new ObsClient(user.accessKey(), user.secretKey(), obsConfig);
                }
                clientPool.put(user.username(), client);
            } catch (Exception e) {
                System.err.printf("[Fatal Error] Failed to initialize ObsClient for user %s: %s\n", user.username(), e.getMessage());
                throw new RuntimeException("ObsClient initialization failed", e);
            }
        }

        // 3. Initialize Shared Resumable Thread Pool (Global architecture optimization)
        int poolSize = config.resumableThreads() != null ? config.resumableThreads() : Runtime.getRuntime().availableProcessors();
        this.resumableExecutor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r);
            t.setName("OBS-Resumable-Global-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        System.out.printf("[ObsClientManager] Initialization complete. (Client Pool: %d, Resumable Global Pool: %d)\n", 
                          users.size(), poolSize);
    }

    /**
     * Get the shared global executor for resumable uploads
     */
    public ExecutorService getResumableExecutor() {
        return resumableExecutor;
    }

    /**
     * Get the ObsClient instance for a specific user
     */
    public ObsClient getClient(String username) {
        ObsClient client = clientPool.get(username);
        if (client == null) {
            throw new IllegalStateException("ObsClient instance not found for user [" + username + "], please check initialization logic.");
        }
        return client;
    }

    /**
     * Graceful shutdown: Close all clients and release underlying connection pool resources
     */
    public void shutdownAll() {
        System.out.println("[ObsClientManager] Closing all ObsClients and releasing connection resources...");
        clientPool.forEach((username, client) -> {
            try {
                // Must explicitly call close(), otherwise the underlying OkHttp thread pool may not exit immediately
                client.close();
            } catch (IOException e) {
                // Log exceptions during shutdown, do not block the main exit flow
                System.err.println("Failed to close client: " + username);
            }
        });
        clientPool.clear();
        
        if (resumableExecutor != null) {
            System.out.println("[ObsClientManager] Shutting down Resumable Global Thread Pool...");
            resumableExecutor.shutdown();
            try {
                if (!resumableExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    resumableExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                resumableExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.println("[ObsClientManager] Resources fully released.");
    }
}
