package com.vmturbo.group.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.setting.ISettingPolicyStore;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingPolicyHash;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Mock memory-based store for setting policies.
 */
public class MockSettingPolicyStore implements ISettingPolicyStore {

    private final Map<Long, SettingPolicy> settingPolicies = new HashMap<>();

    @Nonnull
    @Override
    public Map<Long, Map<String, DiscoveredObjectVersionIdentity>> getDiscoveredPolicies() {
        final Map<Long, Map<String, DiscoveredObjectVersionIdentity>> result = new HashMap<>();
        for (SettingPolicy policy : settingPolicies.values()) {
            if (policy.getSettingPolicyType() == Type.DISCOVERED) {
                final long target = policy.getInfo().getTargetId();
                final String name = policy.getInfo().getName();
                result.computeIfAbsent(target, key -> new HashMap<>()).put(name,
                        new DiscoveredObjectVersionIdentity(policy.getId(),
                                SettingPolicyHash.hash(policy.getInfo())));
            }
        }
        return result;
    }

    @Override
    public void deletePolicies(@Nonnull Collection<Long> oids, @Nonnull Type allowedType)
            throws StoreOperationException {
        for (long oid : oids) {
            final SettingPolicy policy = settingPolicies.get(oid);
            if (policy == null) {
                throw new StoreOperationException(Status.NOT_FOUND,
                        "Policy not found with Id " + oid);
            }
            settingPolicies.remove(oid);
        }
    }

    @Override
    public void createSettingPolicies(@Nonnull Collection<SettingPolicy> settingPolicies)
            throws StoreOperationException {
        for (SettingPolicy policy : settingPolicies) {
            this.settingPolicies.put(policy.getId(), policy);
        }
    }

    /**
     * Adds a mew settomg stpre without interactions with method
     * {@link ISettingPolicyStore#createSettingPolicies(Collection)} to perform correct method calls
     * verifications.
     *
     * @param settingPolicies setting policies to add.
     */
    public void addSettingPolicies(@Nonnull SettingPolicy... settingPolicies) {
        for (SettingPolicy policy : settingPolicies) {
            this.settingPolicies.put(policy.getId(), policy);
        }
    }

    @Nonnull
    @Override
    public Optional<SettingPolicy> getPolicy(long id) {
        return Optional.ofNullable(settingPolicies.get(id));
    }

    @Nonnull
    private static Predicate<SettingPolicy> getFilter(@Nullable SettingPolicyFilter filter) {
        // filter can be null when executing Mockito.when()
        if (filter == null) {
            return x -> true;
        }
        Predicate<SettingPolicy> resultFilter = x -> true;
        if (!filter.getDesiredEntityTypes().isEmpty()) {
            resultFilter = resultFilter.and(policy -> filter.getDesiredEntityTypes()
                    .contains(policy.getInfo().getEntityType()));
        }
        if (!filter.getDesiredIds().isEmpty()) {
            resultFilter =
                    resultFilter.and(policy -> filter.getDesiredIds().contains(policy.getId()));
        }
        if (!filter.getDesiredTargetIds().isEmpty()) {
            resultFilter = resultFilter.and(policy -> filter.getDesiredTargetIds()
                    .contains(policy.getInfo().getTargetId()));
        }
        if (!filter.getDesiredNames().isEmpty()) {
            resultFilter = resultFilter.and(
                    policy -> filter.getDesiredNames().contains(policy.getInfo().getName()));
        }
        if (!filter.getDesiredTypes().isEmpty()) {
            resultFilter = resultFilter.and(
                    policy -> filter.getDesiredTypes().contains(policy.getSettingPolicyType()));
        }
        if (!filter.getActivationSchedules().isEmpty()) {
            resultFilter = resultFilter.and(
                    policy -> filter.getActivationSchedules().contains(policy.getInfo().getScheduleId()));
        }
        return resultFilter;
    }

    @Nonnull
    @Override
    public Collection<SettingPolicy> getPolicies(@Nonnull SettingPolicyFilter filter) {
        final Predicate<SettingPolicy> policyPredicate = getFilter(filter);
        return settingPolicies.values()
                .stream()
                .filter(policyPredicate)
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Pair<SettingPolicy, Boolean> updateSettingPolicy(long id,
            @Nonnull SettingPolicyInfo newPolicyInfo) throws StoreOperationException {
        final SettingPolicy settingPolicy = settingPolicies.get(id);
        if (settingPolicy == null) {
            throw new StoreOperationException(Status.NOT_FOUND, "Policy not found with Id " + id);
        }
        return Pair.create(settingPolicy.toBuilder().setInfo(newPolicyInfo).build(), false);
    }
}
