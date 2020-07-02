package com.vmturbo.group.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.group.DiscoveredPolicyUpdater;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Updater for discovered placement policies. It's main purpose is to process incoming discovered
 * data to update representation in a DAO.
 *
 * <p>There is nothing referencing placement policies, so it's safe to just remove all the setting
 * policies for every discovered target and recreate it back (preserving OIDs for existing
 * policies).
 */
public class DiscoveredPlacementPolicyUpdater extends DiscoveredPolicyUpdater {
    private static final String POLICY_TYPE_LOGGING = "Placement";

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
            @Nonnull Map<Long, ? extends Collection<DiscoveredPolicyInfo>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupOids, Set<Long> undiscoveredTargets) throws StoreOperationException {
        final long startTime = System.currentTimeMillis();
        getLogger().info("Updating discovered placement policies for {} targets: {}",
                discoveredPolicies.size(), discoveredPolicies.keySet());
        final Map<Long, Map<String, Long>> allExistingPolicies = store.getDiscoveredPolicies();
        final Collection<Policy> policiesToAdd = new ArrayList<>();
        final Collection<Long> policiesToRemove = new ArrayList<>();

        // remove the policies associated to targets that are no longer around
        policiesToRemove.addAll(findPoliciesAssociatedTargetsRemoved(allExistingPolicies,
            discoveredPolicies.keySet(), undiscoveredTargets, POLICY_TYPE_LOGGING));

        for (Entry<Long, ? extends Collection<DiscoveredPolicyInfo>> policyEntry : discoveredPolicies
                .entrySet()) {
            final long targetId = policyEntry.getKey();
            final Collection<DiscoveredPolicyInfo> policyInfos = policyEntry.getValue();
            final Map<String, Long> existingPolicies = allExistingPolicies.getOrDefault(targetId,
                    Collections.emptyMap());
            final DiscoveredPoliciesMapper policiesMapper = new DiscoveredPoliciesMapper(
                    groupOids.row(targetId));
            policiesToRemove.addAll(existingPolicies.values());
            int addedPolicies = 0;
            int skippedPolicies = 0;
            for (DiscoveredPolicyInfo policyInfo : policyInfos) {
                final Long existingOid = existingPolicies.get(policyInfo.getPolicyName());
                final long effectiveOid;
                if (existingOid == null) {
                    effectiveOid = getIdentityProvider().next();
                } else {
                    effectiveOid = existingOid;
                }
                final Optional<PolicyInfo> policyInfoOptional = policiesMapper.inputPolicy(
                        policyInfo);
                if (policyInfoOptional.isPresent()) {
                    addedPolicies++;
                    policiesToAdd.add(Policy.newBuilder()
                            .setId(effectiveOid)
                            .setTargetId(targetId)
                            .setPolicyInfo(policyInfoOptional.get())
                            .build());
                } else {
                    skippedPolicies++;
                }
            }
            getLogger().info(
                    "For target {} {} placement policies will overwrite existing {} policies. {} skipped because of errors",
                    targetId, addedPolicies, existingPolicies.size(), skippedPolicies);
        }
        getLogger().info("Removing {} discovered placement policies for {} targets",
                policiesToRemove.size(), discoveredPolicies.size());
        store.deletePolicies(policiesToRemove);
        getLogger().info("Adding (restoring) {} discovered placement policies for {} targets",
                policiesToAdd.size(), discoveredPolicies.size());
        store.createPolicies(policiesToAdd);
        getLogger().info("Updating discovered placement policies finished. Took {} ms",
                System.currentTimeMillis() - startTime);
    }
}
