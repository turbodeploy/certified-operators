package com.vmturbo.group.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroupId;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The {@link GroupStitchingManager} coordinates group stitching operations.
 *
 * <p>Stitching is the process of taking the groups discovered from different targets and applying
 * certain operations to produce a unified group topology.
 */
public class GroupStitchingManager {

    private static final Logger logger = LogManager.getLogger();

    /**
     * This is a map of priorities of the probes. Default priority is 0. The probe with the less
     * priority value wins. If the probe wins, then it's group properties (like display name)
     * will be present in the resulting stitched group. If probe loses, it only put static members
     * and discovered targets to the resulting stitched group.
     */
    private static final Map<String, Integer> PROBE_PRIORITIES =
            ImmutableMap.of(SDKProbeType.AZURE_STORAGE_BROWSE.toString(), 1,
                    SDKProbeType.APPINSIGHTS.toString(), 1);

    /**
     * A metric that tracks duration of execution for group stitching.
     */
    private static final DataMetricSummary GROUP_STITCHING_EXECUTION_DURATION_SUMMARY =
            DataMetricSummary.builder()
                    .withName("group_stitching_execution_duration_seconds")
                    .withHelp("Duration of execution of all group stitching operations.")
                    .build()
                    .register();

    private final IdentityProvider identityProvider;

    /**
     * Constructs group stitching manager.
     *
     * @param identityProvider identity provider used to assign new OIDs to new groups
     */
    public GroupStitchingManager(@Nonnull IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Stitch the groups discovered by different targets together to produce a unified group.
     *
     * @param groupStore group store to retrieve existing groups information from
     * @param stitchingContext The input context containing the groups to be stitched.
     * @return A {@link GroupStitchingContext} that contains the results of applying the stitching
     *         operations to the groups.
     */
    public StitchingResult stitch(@Nonnull IGroupStore groupStore,
            @Nonnull final GroupStitchingContext stitchingContext) {
        final DataMetricTimer executionTimer =
                GROUP_STITCHING_EXECUTION_DURATION_SUMMARY.startTimer();
        final Collection<DiscoveredGroupId> existingDiscoveredGroups = groupStore.getDiscoveredGroupsIds();
        final GroupIdProvider idProvider = new CombinedGroupProvider(existingDiscoveredGroups);
        final Map<String, StitchingGroup> stitchingGroups = new HashMap<>();
        final Map<Long, Collection<UploadedGroup>> targetsToGroups =
                stitchingContext.getUploadedGroupsMap();
        for (Long targetId : getTargetsSorted(stitchingContext.getTargetIdToProbeType())) {
            for (UploadedGroup group : targetsToGroups.get(targetId)) {
                final String stitchKey = idProvider.getStitchingKey(targetId, group);
                final StitchingGroup foundGroup = stitchingGroups.get(stitchKey);
                if (foundGroup != null) {
                    foundGroup.mergedGroup(group.getDefinition(), targetId);
                } else {
                    final Optional<Long> existingOid = idProvider.getId(targetId, group);
                    final long oid = existingOid.orElseGet(identityProvider::next);
                    final StitchingGroup stitchingGroup =
                            new StitchingGroup(oid, group.getDefinition(),
                                    group.getSourceIdentifier(), targetId, !existingOid.isPresent());
                    stitchingGroups.put(stitchKey, stitchingGroup);
                }
            }
        }
        executionTimer.observe();

        final Set<Long> groupsToDelete = new HashSet<>(existingDiscoveredGroups.stream()
                .map(DiscoveredGroupId::getOid)
                .collect(Collectors.toSet()));
        groupsToDelete.removeAll(stitchingGroups.values()
                .stream()
                .map(StitchingGroup::getOid)
                .collect(Collectors.toList()));
        // These are groups that we know about, but they are related to targets, that are not
        // discovered yet. We simply do not touch them.
        if (!stitchingContext.getUndiscoveredTargets().isEmpty()) {
            groupsToDelete.removeAll(
                    groupStore.getGroupsByTargets(stitchingContext.getUndiscoveredTargets()));
        }
        return new StitchingResult(stitchingGroups.values(), groupsToDelete);
    }

    /**
     * Returns the sorted list of targets. Targets with the highest priorities will be the first
     * in the list.
     *
     * @param targetToProbeMap map of discovered targets to probes
     * @return priority list of targets
     */
    @Nonnull
    private List<Long> getTargetsSorted(@Nonnull Map<Long, String> targetToProbeMap) {
        return targetToProbeMap.entrySet()
                .stream()
                .sorted(Comparator.comparingInt(
                        entry -> PROBE_PRIORITIES.getOrDefault(entry.getValue(), 0)))
                .map(Entry::getKey)
                .collect(Collectors.toList());
    }

    private static Pair<Long, String> createGroupLocalKey(DiscoveredGroupId group) {
        return Pair.create(group.getTarget(), group.getSourceId());
    }

    /**
     * Interface providing identity information about the group.
     */
    private interface GroupIdProvider {
        /**
         * Retrieves an existing OID for the specified group reported for the specified target.
         *
         * @param target target group reported by
         * @param group group to get OID for
         * @return existing OID or {@link Optional#empty()}
         */
        @Nonnull
        Optional<Long> getId(long target, @Nonnull UploadedGroup group);

        /**
         * Returns a stitching key - an internally used string identifier, which is used while the
         * stitching operation is performed. Every provider implements it in its own way. No
         * stitching keys from different providers may be identical.
         * @param target target group reported by
         * @param group group to get stitching key for
         * @return stitching key
         */
        @Nonnull
        String getStitchingKey(long target, @Nonnull UploadedGroup group);
    }

    /**
     * Combined group provider is a convenience class to wrap all the other identity providers.
     */
    private static class CombinedGroupProvider implements GroupIdProvider {

        private final GroupIdProvider rgProvider;
        private final GroupIdProvider regularProvider;

        CombinedGroupProvider(@Nonnull Collection<DiscoveredGroupId> discoveredGroups) {
            this.rgProvider = new CrossTargetStitcher(discoveredGroups);
            this.regularProvider = new TargetLocalStitcher(discoveredGroups);
        }

        @Nonnull
        private GroupIdProvider getProvider(@Nonnull UploadedGroup group) {
            if (group.getDefinition().hasType() && group.getDefinition().getType() == GroupType.RESOURCE) {
                return rgProvider;
            } else {
                return regularProvider;
            }
        }

        @Nonnull
        @Override
        public Optional<Long> getId(long target, @Nonnull UploadedGroup group) {
            return getProvider(group).getId(target, group);
        }

        @Nonnull
        @Override
        public String getStitchingKey(long target, @Nonnull UploadedGroup group) {
            return getProvider(group).getStitchingKey(target, group);
        }
    }

    /**
     * Identity provider for cross-target stitchable groups.
     * Groups are identified here by group type + discovered source id. If multiple values
     * are present, we ignore them, as they are obviously related to target-local groups
     */
    private static class CrossTargetStitcher implements GroupIdProvider {
        private final Map<String, Long> groupsToIds;

        CrossTargetStitcher(@Nonnull Collection<DiscoveredGroupId> discoveredGroups) {
            this.groupsToIds = Collections.unmodifiableMap(discoveredGroups.stream()
                    .collect(Collectors.groupingBy(
                            group -> Pair.create(group.getGroupType(), group.getSourceId())))
                    .values()
                    .stream()
                    .filter(set -> set.size() == 1)
                    .map(set -> set.iterator().next())
                    .collect(Collectors.toMap(DiscoveredGroupId::getSourceId,
                            DiscoveredGroupId::getOid, (val1, val2) -> val1)));
        }

        @Nonnull
        @Override
        public Optional<Long> getId(long target, @Nonnull UploadedGroup group) {
            return Optional.ofNullable(groupsToIds.get(group.getSourceIdentifier()));
        }

        @Nonnull
        @Override
        public String getStitchingKey(long target, @Nonnull UploadedGroup group) {
            return "resource-" + GroupProtoUtil.createIdentifyingKey(group);
        }
    }

    /**
     * Identity provider for groups, that should not be stitched between targets.
     */
    private static class TargetLocalStitcher implements GroupIdProvider {
        private final Map<Pair<Long, String>, Long> groupsToIds;

        TargetLocalStitcher(@Nonnull Collection<DiscoveredGroupId> discoveredGroups) {
            groupsToIds = Collections.unmodifiableMap(discoveredGroups.stream()
                    .filter(group -> group.getTarget() != null)
                    .collect(Collectors.toMap(GroupStitchingManager::createGroupLocalKey,
                            DiscoveredGroupId::getOid)));
        }

        @Nonnull
        @Override
        public Optional<Long> getId(long target, @Nonnull UploadedGroup group) {
            return Optional.ofNullable(
                    groupsToIds.get(Pair.create(target, group.getSourceIdentifier())));
        }

        @Nonnull
        @Override
        public String getStitchingKey(long target, @Nonnull UploadedGroup group) {
            return "regular-" + target + "-" + GroupProtoUtil.createIdentifyingKey(group);
        }
    }
}
