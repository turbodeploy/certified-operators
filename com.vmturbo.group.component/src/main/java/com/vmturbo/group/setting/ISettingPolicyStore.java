package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Setting policy store view for executing within a transaction.
 */
public interface ISettingPolicyStore {
    /**
     * Returns a collection of discovered policies that already exist in the database.
     *
     * @return map of policy id by policy name, groupped by target id
     */
    @Nonnull
    Map<Long, Map<String, DiscoveredObjectVersionIdentity>> getDiscoveredPolicies();

    /**
     * Deletes the setting policies from the store identified by OIDs. {@code allowedType}
     * is an additional restriction to specify which types of policies are expected to be deleted.
     * It will cause a {@link StoreOperationException} if one of the requested OIDs refers to
     * another policy type.
     *
     * @param oids OIDs of policies to delete
     * @param allowedType type of the policies. Used as an additional check to ensure, that
     *         client is expecting removal of a specific type of policies only
     * @throws StoreOperationException if some operation failed with this store.
     */
    void deletePolicies(@Nonnull Collection<Long> oids, @Nonnull Type allowedType)
            throws StoreOperationException;

    /**
     * Creates policies as defined in the parameter. Store does not perform any cleanup or merging.
     * It is up to a client to remove the policy before calling this method it it is expected to
     * substitute some other policies.
     *
     * @param settingPolicies policies to create
     * @throws StoreOperationException if some storage exception occurred.
     */
    void createSettingPolicies(@Nonnull Collection<SettingPolicy> settingPolicies)
            throws StoreOperationException;

    /**
     * Returns a policy by OID, if it exists.
     *
     * @param id policy OID to return.
     * @return setting policy, if any, otherwise - an empty {@link Optional}
     * @throws StoreOperationException if store operation failed
     */
    @Nonnull
    Optional<SettingPolicy> getPolicy(long id) throws StoreOperationException;

    /**
     * Search for policies using the specified filter.
     *
     * @param filter filter to apply on the policies
     * @return all the policies matching the filter.
     * @throws StoreOperationException if store operation failed
     */
    @Nonnull
    Collection<SettingPolicy> getPolicies(@Nonnull SettingPolicyFilter filter)
            throws StoreOperationException;

    /**
     * Update setting policy.
     *
     * @param id the policy id
     * @param newPolicyInfo new policy info overwriting the previous one
     * @return pair of updated setting policy and flag describing should we remove accepted and
     * rejected actions associated with policy or not
     * @throws StoreOperationException if store operation failed
     */
    @Nonnull
    Pair<SettingPolicy, Boolean> updateSettingPolicy(long id,
            @Nonnull SettingPolicyInfo newPolicyInfo) throws StoreOperationException;
}
