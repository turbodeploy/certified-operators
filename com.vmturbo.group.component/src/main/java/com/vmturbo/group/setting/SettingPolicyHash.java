package com.vmturbo.group.setting;

import javax.annotation.Nonnull;

import org.apache.commons.codec.digest.DigestUtils;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * Utility class to calculate hash for setting policies.
 */
public class SettingPolicyHash {
    private SettingPolicyHash() {}

    /**
     * Calculates hash for a setting policy suitable to identify changes in the policy.
     * SettingPolicy contains in addition OID and settingType(DISCOVERED). These values are not
     * used in identifying changes in setting policy
     *
     * @param settingPolicyInfo policy info to hash
     * @return hash value
     */
    @Nonnull
    public static byte[] hash(@Nonnull SettingPolicyInfo settingPolicyInfo) {
        return DigestUtils.sha256(settingPolicyInfo.toByteArray());
    }
}
