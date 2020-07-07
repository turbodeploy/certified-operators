package com.vmturbo.group.policy;

import javax.annotation.Nonnull;

import org.apache.commons.codec.digest.DigestUtils;

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;

/**
 * Utility class to hash placement policies.
 */
public class PlacementPolicyHash {
    private PlacementPolicyHash() {}

    /**
     * Creates a hash that is used to determine whether there are any changes in the placement
     * policy. We hash the whole policy because
     * {@link com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo} does not contain target id.
     *
     * @param policy placement policy to hash
     * @return hash value
     */
    @Nonnull
    public static byte[] hash(@Nonnull Policy policy) {
        return DigestUtils.sha256(policy.toByteArray());
    }
}
