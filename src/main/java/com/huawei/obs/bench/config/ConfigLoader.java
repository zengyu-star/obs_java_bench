package com.huawei.obs.bench.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 压测配置与凭证加载器
 * 负责解析 config.dat (Properties格式) 和 users.dat (CSV格式)
 */
public class ConfigLoader {

    /**
     * 加载全局压测配置
     * @param configPath config.dat 文件路径
     * @return 强类型的 BenchConfig 对象
     */
    public static BenchConfig loadConfig(String configPath) {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configPath)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("[致命错误] 无法读取配置文件: " + configPath + "。请检查文件是否存在！", e);
        }

        // 采用容错机制解析，并提供合理的默认值
        try {
            return new BenchConfig(
                props.getProperty("Endpoint", "obs.cn-north-4.myhuaweicloud.com").trim(),
                props.getProperty("Protocol", "https").trim(),
                Boolean.parseBoolean(props.getProperty("IsTemporaryToken", "false")),

                Integer.parseInt(props.getProperty("MaxConnections", "2000")),
                Integer.parseInt(props.getProperty("SocketTimeoutMs", "60000")),
                Integer.parseInt(props.getProperty("ConnectionTimeoutMs", "30000")),

                Integer.parseInt(props.getProperty("UsersCount", "1")),
                Integer.parseInt(props.getProperty("ThreadsPerUser", "1")),
                parseRunSeconds(props.getProperty("RunSeconds")),
                parseRequestsPerThread(props.getProperty("RequestsPerThread")),

                Integer.parseInt(props.getProperty("TestCaseCode", "201")),

                props.getProperty("BucketNameFixed", "").trim(),
                props.getProperty("BucketNamePrefix", "bench-bucket").trim(),
                props.getProperty("KeyPrefix", "bench_test_").trim(),
                Long.parseLong(props.getProperty("ObjectSize", "1048576")),
                Long.parseLong(props.getProperty("PartSize", "5242880")),

                Boolean.parseBoolean(props.getProperty("ObjNamePatternHash", "true")),
                Boolean.parseBoolean(props.getProperty("EnableDataValidation", "false")),
                Boolean.parseBoolean(props.getProperty("EnableDetailLog", "false")),
                Boolean.parseBoolean(props.getProperty("IsMockMode", "false"))
            );
        } catch (NumberFormatException e) {
            throw new RuntimeException("[致命错误] config.dat 中存在非法的数字格式，请检查！", e);
        }
    }

    /**
     * 加载多租户用户凭证
     * @param usersPath users.dat 文件路径
     * @param requiredCount config 中声明的 UsersCount
     * @return 用户凭证列表
     */
    public static List<UserCredential> loadUsers(String usersPath, int requiredCount) {
        List<UserCredential> users = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(usersPath))) {
            String line;
            int lineNumber = 0;
            
            while ((line = reader.readLine()) != null && users.size() < requiredCount) {
                lineNumber++;
                line = line.trim();
                
                // 跳过空行和以 # 开头的注释行
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // 假设 users.dat 的格式为: username,ak,sk,[sts_token]
                String[] parts = line.split(",");
                if (parts.length < 3) {
                    System.err.println("[WARN] users.dat 第 " + lineNumber + " 行格式不正确，已跳过: " + line);
                    continue;
                }

                String username = parts[0].trim();
                String ak = parts[1].trim();
                String sk = parts[2].trim();
                // 如果启用了临时凭证，则读取第四列，否则设为 null
                String token = parts.length > 3 ? parts[3].trim() : null;
                
                // originalAk 用于后续如果需要基于 AK 拼接独立的 Bucket 名称
                String originalAk = ak;

                users.add(new UserCredential(username, ak, sk, token, originalAk));
            }
        } catch (IOException e) {
            throw new RuntimeException("[致命错误] 无法读取用户凭证文件: " + usersPath, e);
        }

        // 防呆校验：配置的并发用户数大于实际提供的凭证数
        if (users.size() < requiredCount) {
            throw new RuntimeException(String.format(
                "[致命错误] config.dat 声明需要 %d 个用户，但 users.dat 中只解析到了 %d 个有效用户！", 
                requiredCount, users.size()
            ));
        }

        return users;
    }

    /**
     * 解析 RunSeconds 参数：大于0的整数或为空(返回0)
     */
    private static long parseRunSeconds(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return 0; // 空表示不限时长
        }
        try {
            long value = Long.parseLong(rawValue.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("[致命错误] RunSeconds 必须为空，或者大于 0 的整数值，不允许配置为 0（当前输入: \"" + rawValue + "\"）");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[致命错误] RunSeconds 格式非法，必须为空，或者大于 0 的整数值（当前输入: \"" + rawValue + "\"）");
        }
    }

    /**
     * 解析 RequestsPerThread 参数：必须是大于0的整数
     */
    private static long parseRequestsPerThread(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            throw new IllegalArgumentException("[致命错误] RequestsPerThread 不能为空，必须配置为大于 0 的整数");
        }
        try {
            long value = Long.parseLong(rawValue.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("[致命错误] RequestsPerThread 必须是大于 0 的整数（当前值: " + value + "）");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[致命错误] RequestsPerThread 格式非法，必须是大于 0 的整数（当前输入: \"" + rawValue + "\"）");
        }
    }
}
