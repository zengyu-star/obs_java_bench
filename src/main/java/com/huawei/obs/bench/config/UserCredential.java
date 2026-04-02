package com.huawei.obs.bench.config;

/**
 * 用户凭证模型 (Immutable Record)
 * 映射 users.dat 中的每一行身份信息。
 */
public record UserCredential(
    String username,      // 租户标识名
    String accessKey,     // AK
    String secretKey,     // SK
    String securityToken, // STS Token (仅在临时授权时有效)
    String originalAk     // 原始 AK (用于区分不同租户的 Bucket 命名)
) {
    /**
     * 判断当前凭证是否包含 STS 临时 Token
     */
    public boolean isStsToken() {
        return securityToken != null && !securityToken.isBlank();
    }

    /**
     * 重写 toString，防止在日志中意外泄露敏感的 SecretKey
     */
    @Override
    public String toString() {
        return "UserCredential{" +
                "username='" + username + '\'' +
                ", accessKey='" + (accessKey != null ? accessKey.substring(0, 4) + "****" : "null") + '\'' +
                ", hasToken=" + isStsToken() +
                '}';
    }
}
