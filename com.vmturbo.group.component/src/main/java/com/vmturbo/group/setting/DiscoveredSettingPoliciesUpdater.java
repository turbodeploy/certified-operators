package com.vmturbo.group.setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.DiscoveredPolicyUpdater;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Updater for discovered setting policies is used to executed appropriate operations on the
 * {@link ISettingPolicyStore} to store all the setting policies discovered by targets.
 */
public class DiscoveredSettingPoliciesUpdater extends
        DiscoveredPolicyUpdater<SettingPolicy, SettingPolicyInfo, DiscoveredSettingPolicyInfo> {
    private static final String POLICY_TYPE_LOGGING = "Setting";
    private static final DataMetricSummary DURATION_TIMER = DataMetricSummary
            .builder()
            .withName("group_discovered_setting_policy_update_duration")
            .withHelp("Duration of a discovered setting policy update procedure.")
            .build()
            .register();

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
            @Nonnull Map<Long, Collection<DiscoveredSettingPolicyInfo>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupNamesByTarget, Set<Long> undiscoveredTargets) throws StoreOperationException {
        final DataMetricTimer timer = DURATION_TIMER.startTimer();
        getLogger().info("Updating discovered setting policies for targets: {}",
                discoveredPolicies::keySet);
        if (getLogger().isTraceEnabled()) {
            for (Entry<Long, Collection<DiscoveredSettingPolicyInfo>> entry : discoveredPolicies.entrySet()) {
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
        final Map<Long, Map<String, DiscoveredObjectVersionIdentity>> existingPolicies =
                policyStore.getDiscoveredPolicies();
        final CollectionsUpdate<SettingPolicy> update = analyzeAllPolicies(existingPolicies,
                discoveredPolicies, groupNamesByTarget, undiscoveredTargets);
        getLogger().debug("Deleting {}...", update.getObjectsToDelete()::size);
        policyStore.deletePolicies(update.getObjectsToDelete(), Type.DISCOVERED);
        getLogger().debug("Inserting {} policies will be added: {}",
                update.getObjectsToAdd()::size,
                () -> update.getObjectsToAdd()
                        .stream()
                        .map(policy -> policy.getInfo().getName() + "(" + policy.getId() + ")")
                        .collect(Collectors.joining(",")));
        policyStore.createSettingPolicies(update.getObjectsToAdd());
        getLogger().info("Successfully processed {} discovered setting policies. from {} targets, took {}s",
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
    protected Function<DiscoveredSettingPolicyInfo, Optional<SettingPolicyInfo>> createPolicyMapper(
            @Nonnull Map<String, Long> groupIds, long targetId) {
        final DiscoveredSettingPoliciesMapper policyMapper = new DiscoveredSettingPoliciesMapper(
                targetId, groupIds);
        return policyMapper::mapToSettingPolicyInfo;
    }

    @Override
    @Nonnull
    protected Optional<PolicyDecision<SettingPolicy>> analyzePolicy(
            @Nonnull DiscoveredSettingPolicyInfo discoveredPolicy,
            @Nonnull Function<DiscoveredSettingPolicyInfo, Optional<SettingPolicyInfo>> policyMapper,
            @Nonnull Map<String, DiscoveredObjectVersionIdentity> existingPolicies,
            long targetId) {
        final Optional<SettingPolicyInfo> policyInfo = policyMapper.apply(discoveredPolicy);
        if (policyInfo.isPresent()) {
            final DiscoveredObjectVersionIdentity existingIdentity = existingPolicies.get(
                    discoveredPolicy.getName());
            final long effectiveOid;
            final boolean delete;
            if (existingIdentity == null) {
                effectiveOid = getIdentityProvider().next();
                delete = false;
            } else {
                final byte[] hash = SettingPolicyHash.hash(policyInfo.get());
                if (Arrays.equals(hash, existingIdentity.getHash())) {
                    return Optional.of(
                            new PolicyDecision<>(existingIdentity.getOid(), false, null));
                }
                effectiveOid = existingIdentity.getOid();
                delete = true;
            }
            final SettingPolicy policy = SettingPolicy.newBuilder()
                    .setId(effectiveOid)
                    .setInfo(policyInfo.get())
                    .setSettingPolicyType(Type.DISCOVERED)
                    .build();
            return Optional.of(new PolicyDecision<>(effectiveOid, delete, policy));
        } else {
            return Optional.empty();
        }
    }
}
