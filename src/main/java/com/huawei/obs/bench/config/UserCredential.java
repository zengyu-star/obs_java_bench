package com.huawei.obs.bench.config;

/**
 * User Credential Model (Immutable Record)
 * Maps to a single row of identity information in users.dat.
 */
public record UserCredential(
    String username,      // Tenant/User identification
    String accessKey,     // AK
    String secretKey,     // SK
    String securityToken, // STS Token (Active only for temporary authorization)
    String originalAk     // Original AK (Used for tenant-specific bucket naming)
) {
    /**
     * Checks if the current credential contains an STS temporary token
     */
    public boolean isStsToken() {
        return securityToken != null && !securityToken.isBlank();
    }

    /**
     * Overrides toString to prevent accidental leakage of SecretKey in logs
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
