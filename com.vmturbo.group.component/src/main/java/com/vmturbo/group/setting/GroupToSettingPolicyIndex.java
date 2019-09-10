package com.vmturbo.group.setting;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;

/**
 * Class which maintains an index from GroupId to SettingPolicyId.
 * NOTE: If a group is removed, its index entry is not deleted. See: OM-44888
 */
public class GroupToSettingPolicyIndex {

    /**
     * Mapping from GroupId -> Set of SettingPolicy Ids.
     */
    final private Map<Long, Set<Long>> groupToSettingPolicyIndex =
            new HashMap<>();

    public GroupToSettingPolicyIndex(Stream<SettingPolicy> settingPolicyStream) {
        settingPolicyStream.forEach(settingPolicy -> add(settingPolicy));
    }

    /**
     * Return the setting policy ids associated with the group.
     * @param groupId ID of the group.
     * @return Set of setting policy ids associated with the group.
     */
    public synchronized Set<Long> getSettingPolicyIdsForGroup(long groupId) {
        return getSettingPolicyIdsForGroup(Collections.singleton(groupId)).get(groupId);
    }

    /**
     * Return the setting policy ids associated with the given group ids.
     *
     * @param groupIds Set of group IDs.
     * @return Mapping of GroupId to Set of SettingPolicy Ids.
     */
    public synchronized Map<Long, Set<Long>> getSettingPolicyIdsForGroup(Set<Long> groupIds) {

        return groupIds.stream().collect(Collectors.toMap(
                Function.identity(),
                groupId -> groupToSettingPolicyIndex.getOrDefault(groupId, new HashSet<>())));

    }

    /**
     * Add the index entries associated with the given setting policy.
     * @param settingPolicy Setting Policy.
     */
    public synchronized void add(SettingPolicy settingPolicy) {
        add(Collections.singletonList(settingPolicy));
    }

    /**
     * Add the index entries associated with the given setting policy.
     * @param settingPolicies List of setting policies.
     */
    public synchronized void add(List<SettingPolicy> settingPolicies) {
            settingPolicies.stream().forEach( settingPolicy ->
                getGroupIdsFromPolicy(settingPolicy).stream()
                        .forEach(groupId ->
                                groupToSettingPolicyIndex.computeIfAbsent(groupId,
                                        k -> new HashSet<>())
                                        .add(settingPolicy.getId())));
    }

    /**
     * Update the index entries associated with the given setting policy.
     * @param settingPolicy Setting Policy.
     */
    public synchronized void update(SettingPolicy settingPolicy) {
        update(Collections.singletonList(settingPolicy));
    }

    /**
     * Update the index entries associated with the given setting policy.
     * @param settingPolicies List of Setting Policies.
     */
    public synchronized void update(List<SettingPolicy> settingPolicies) {
        settingPolicies.forEach(policy ->
            LogManager.getLogger().info("Updating setting policy {}", policy.getInfo().getDisplayName()));

        remove(settingPolicies);
        add(settingPolicies);
    }

    /**
     * Remove the index entries associated with the given setting policy.
     * @param settingPolicy Setting Policy.
     */
    public synchronized void remove(SettingPolicy settingPolicy) {
        remove(Collections.singletonList(settingPolicy));
    }

    /**
     * Remove the index entries associated with the given setting policy.
     * @param settingPolicies List of Setting Policies.
     */
    public synchronized void remove(List<SettingPolicy> settingPolicies) {
        Set<Long> settingPolicyIdsToDelete = settingPolicies.stream()
                .map(SettingPolicy::getId)
                .collect(Collectors.toSet());

        groupToSettingPolicyIndex.entrySet()
                .forEach(entry -> entry.getValue().removeAll(settingPolicyIdsToDelete));

        groupToSettingPolicyIndex.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    /**
     * Clear the index.
     */
    public synchronized void clear() {
        this.groupToSettingPolicyIndex.clear();
    }

    private Set<Long> getGroupIdsFromPolicy(SettingPolicy policy) {
        if (policy.hasInfo() && policy.getInfo().hasScope()) {
            return new HashSet<>(policy.getInfo().getScope().getGroupsList());
        }

        return Collections.emptySet();
    }
}
