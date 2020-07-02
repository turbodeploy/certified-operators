package com.vmturbo.group.policy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.DiscoveredPolicyUpdater;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.setting.CollectionsUpdate;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Updater for discovered placement policies. It's main purpose is to process incoming discovered
 * data to update representation in a DAO.
 *
 * <p>There is nothing referencing placement policies, so it's safe to just remove all the setting
 * policies for every discovered target and recreate it back (preserving OIDs for existing
 * policies).
 */
public class DiscoveredPlacementPolicyUpdater extends DiscoveredPolicyUpdater<Policy, PolicyInfo, DiscoveredPolicyInfo> {
    private static final String POLICY_TYPE_LOGGING = "Placement";
    private static final DataMetricSummary DURATION_TIMER = DataMetricSummary
            .builder()
            .withName("group_discovered_placement_policy_update_duration")
            .withHelp("Duration of a discovered placement policy update procedure.")
            .build()
            .register();

    /**
     * Constructs discovered placement policies updater.
     *
     * @param identityProvider identity provider used to assign OIDs to new policies.
     */
    public DiscoveredPlacementPolicyUpdater(@Nonnull IdentityProvider identityProvider) {
        super(identityProvider);
    }

    /**
     * Update the set of policies discovered by a particular target.
     * The new set of policies will completely replace the old, even if the new set is empty.
     *
     * @param store placement policy store to use.
     * @param discoveredPolicies discovered policies grouped by targets
     * @param groupOids A mapping from group display names to group OIDs. We need this
     *         mapping because discovered policies reference groups by display name.
     * @param undiscoveredTargets the set of oids for targets that have been removed.
     * @throws StoreOperationException If there is an error interacting with the database.
     */
    public void updateDiscoveredPolicies(@Nonnull IPlacementPolicyStore store,
            @Nonnull Map<Long, Collection<DiscoveredPolicyInfo>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupOids, Set<Long> undiscoveredTargets) throws StoreOperationException {
        final DataMetricTimer timer = DURATION_TIMER.startTimer();
        getLogger().info("Updating discovered placement policies for {} targets: {}",
                discoveredPolicies.size(), discoveredPolicies.keySet());
        final Map<Long, Map<String, DiscoveredObjectVersionIdentity>> allExistingPolicies =
                store.getDiscoveredPolicies();
        final CollectionsUpdate<Policy> update = analyzeAllPolicies(allExistingPolicies,
                discoveredPolicies, groupOids, undiscoveredTargets);


        getLogger().debug("Deleting {}...", update.getObjectsToDelete()::size);
        store.deletePolicies(update.getObjectsToDelete());
        getLogger().debug("Inserting {} policies will be added: {}",
                update.getObjectsToAdd()::size,
                () -> update.getObjectsToAdd()
                        .stream()
                        .map(policy -> policy.getPolicyInfo().getName() + "(" + policy.getId() + ")")
                        .collect(Collectors.joining(",")));
        store.createPolicies(update.getObjectsToAdd());
        getLogger().info(
                "Successfully processed {} discovered placement policies. from {} targets. Took {}s",
                discoveredPolicies.values()
                        .stream()
                        .map(Collection::size)
                        .reduce(Integer::sum)
                        .orElse(0),
                discoveredPolicies.size(),
                timer.observe());
    }

    @Nonnull
    @Override
    protected String getPolicyType() {
        return POLICY_TYPE_LOGGING;
    }

    @Nonnull
    @Override
    protected Function<DiscoveredPolicyInfo, Optional<PolicyInfo>> createPolicyMapper(
            @Nonnull Map<String, Long> groupIds, long targetId) {
        final DiscoveredPoliciesMapper mapper = new DiscoveredPoliciesMapper(groupIds);
        return mapper::inputPolicy;
    }

    @Override
    @Nonnull
    protected Optional<PolicyDecision<Policy>> analyzePolicy(@Nonnull DiscoveredPolicyInfo policyInfo,
            @Nonnull Function<DiscoveredPolicyInfo, Optional<PolicyInfo>> policiesMapper,
            @Nonnull Map<String, DiscoveredObjectVersionIdentity> existingPolicies, long targetId) {
        final Optional<PolicyInfo> policyInfoOptional = policiesMapper.apply(policyInfo);
        if (!policyInfoOptional.isPresent()) {
            return Optional.empty();
        }
        final long effectiveOid;
        final boolean delete;
        final DiscoveredObjectVersionIdentity existingIdentity = existingPolicies.get(
                policyInfo.getPolicyName());
        if (existingIdentity != null) {
            effectiveOid = existingIdentity.getOid();
            delete = true;
        } else {
            effectiveOid = getIdentityProvider().next();
            delete = false;
        }
        final Policy policy = policyInfoOptional
                .map(info -> Policy
                        .newBuilder()
                        .setId(effectiveOid)
                        .setTargetId(targetId)
                        .setPolicyInfo(info)
                        .build())
                .orElse(null);
        final byte[] hash = PlacementPolicyHash.hash(policy);
        if (existingIdentity != null && Arrays.equals(hash, existingIdentity.getHash())) {
            return Optional.of(new PolicyDecision<>(effectiveOid, false, null));
        } else {
            return Optional.of(new PolicyDecision<>(effectiveOid, delete, policy));
        }
    }

}
