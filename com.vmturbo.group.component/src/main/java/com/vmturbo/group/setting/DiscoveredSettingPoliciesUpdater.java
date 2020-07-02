package com.vmturbo.group.setting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.DiscoveredPolicyUpdater;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Updater for discovered setting policies is used to executed appropriate operations on the
 * {@link ISettingPolicyStore} to store all the setting policies discovered by targets.
 */
public class DiscoveredSettingPoliciesUpdater extends DiscoveredPolicyUpdater {
    private static final String POLICY_TYPE_LOGGING = "Setting";


    /**
     * Constructs setting policies updater.
     *
     * @param identityProvider identity provider used to assign new OIDs for new setting
     *         policies.
     */
    public DiscoveredSettingPoliciesUpdater(@Nonnull IdentityProvider identityProvider) {
        super(identityProvider);
    }

    /**
     * Performs an default settings policies update.
     *
     * @param policyStore setting policy store to use to retrieve and store setting policies
     * @param discoveredPolicies policies discovered by targets
     * @param groupNamesByTarget group OIDs by group discovered srouce id, groupped by
     *         targets
     * @param undiscoveredTargets the set of oids for targets that have been removed.
     * @throws StoreOperationException if update failed
     */
    public void updateSettingPolicies(@Nonnull ISettingPolicyStore policyStore,
            @Nonnull Map<Long, List<DiscoveredSettingPolicyInfo>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupNamesByTarget, Set<Long> undiscoveredTargets) throws StoreOperationException {
        getLogger().info("Updating discovered setting policies for targets: {}",
                discoveredPolicies::keySet);
        if (getLogger().isTraceEnabled()) {
            for (Entry<Long, List<DiscoveredSettingPolicyInfo>> entry : discoveredPolicies.entrySet()) {
                getLogger().trace("Target {} reported {} policies: {}", entry.getKey(),
                        entry.getValue().size(), entry.getValue()
                                .stream()
                                .map(DiscoveredSettingPolicyInfo::getName)
                                .sorted()
                                .collect(Collectors.toList()));
            }
            for (Entry<Long, Map<String, Long>> entry : groupNamesByTarget.rowMap().entrySet()) {
                getLogger().trace("Target {} has {} groups: {}", entry.getKey(), entry.getValue().size(),
                        entry.getValue().keySet().stream().sorted().collect(Collectors.toList()));
            }
        }
        final CollectionsUpdate<SettingPolicy> collectionsUpdate =
                analyzeSettings(policyStore.getDiscoveredPolicies(), discoveredPolicies,
                        groupNamesByTarget, undiscoveredTargets);
        getLogger().debug("The following policies will be removed {}: {}",
                () -> collectionsUpdate.getObjectsToDelete().size(),
                collectionsUpdate::getObjectsToDelete);
        policyStore.deletePolicies(collectionsUpdate.getObjectsToDelete(), Type.DISCOVERED);
        getLogger().debug("The following policies will be added {}: {}",
                () -> collectionsUpdate.getObjectsToAdd().size(),
                () -> collectionsUpdate.getObjectsToAdd()
                        .stream()
                        .map(policy -> policy.getInfo().getName() + "(" + policy.getId() + ")")
                        .collect(Collectors.joining(",")));
        policyStore.createSettingPolicies(collectionsUpdate.getObjectsToAdd());
        getLogger().info("Successfully processed {} discovered setting policies. {} of them saved",
                discoveredPolicies.values().stream().mapToLong(Collection::size).sum(),
                collectionsUpdate.getObjectsToAdd().size());
    }

    @Nonnull
    private CollectionsUpdate<SettingPolicy> analyzeSettings(
            @Nonnull Map<Long, Map<String, Long>> existingPolicies,
            @Nonnull Map<Long, List<DiscoveredSettingPolicyInfo>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupNamesByTarget, Set<Long> undiscoveredTargets) {
        final Collection<SettingPolicy> objectsToAdd = new ArrayList<>();
        final Collection<Long> objectsToDelete = new ArrayList<>();

        // remove the policies associated to targets that are no longer around
        objectsToDelete.addAll(findPoliciesAssociatedTargetsRemoved(existingPolicies,
            discoveredPolicies.keySet(), undiscoveredTargets, POLICY_TYPE_LOGGING));

        for (Entry<Long, List<DiscoveredSettingPolicyInfo>> entry : discoveredPolicies.entrySet()) {
            final long targetId = entry.getKey();
            final DiscoveredSettingPoliciesMapper policyMapper =
                    new DiscoveredSettingPoliciesMapper(targetId, groupNamesByTarget.row(targetId));
            final Map<String, Long> targetExistingPolicies =
                    existingPolicies.getOrDefault(targetId, Collections.emptyMap());
            getLogger().trace("Existing policies for target {}: {}", targetId, targetExistingPolicies);
            objectsToDelete.addAll(targetExistingPolicies.values());
            for (DiscoveredSettingPolicyInfo discoveredPolicy : entry.getValue()) {
                final Optional<SettingPolicyInfo> policyInfo =
                        policyMapper.mapToSettingPolicyInfo(discoveredPolicy);
                if (policyInfo.isPresent()) {
                    final Long existingOid = targetExistingPolicies.get(discoveredPolicy.getName());
                    final long effectiveOid;
                    if (existingOid == null) {
                        effectiveOid = getIdentityProvider().next();
                    } else {
                        effectiveOid = existingOid;
                    }
                    final SettingPolicy policy = SettingPolicy.newBuilder()
                            .setId(effectiveOid)
                            .setInfo(policyInfo.get())
                            .setSettingPolicyType(Type.DISCOVERED)
                            .build();
                    objectsToAdd.add(policy);
                }
            }
        }
        return new CollectionsUpdate<>(objectsToAdd, objectsToDelete);
    }
}
