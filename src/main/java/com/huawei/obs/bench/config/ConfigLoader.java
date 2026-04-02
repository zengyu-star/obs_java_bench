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
                Boolean.parseBoolean(props.getProperty("IsSecure", "false")),
                Boolean.parseBoolean(props.getProperty("IsTemporaryToken", "false")),

                Integer.parseInt(props.getProperty("MaxConnections", "2000")),
                Integer.parseInt(props.getProperty("SocketTimeoutMs", "60000")),
                Integer.parseInt(props.getProperty("ConnectionTimeoutMs", "30000")),

                Integer.parseInt(props.getProperty("UsersCount", "1")),
                Integer.parseInt(props.getProperty("ThreadsPerUser", "1")),
                Long.parseLong(props.getProperty("RunSeconds", "0")),
                Long.parseLong(props.getProperty("RequestsPerThread", "0")),

                Integer.parseInt(props.getProperty("TestCaseCode", "201")),

                props.getProperty("BucketName", "bench-bucket").trim(),
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
}
