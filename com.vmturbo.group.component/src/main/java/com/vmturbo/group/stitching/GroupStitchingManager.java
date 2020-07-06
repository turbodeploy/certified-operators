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
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
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
                    SDKProbeType.APPINSIGHTS.toString(), 1,
                    SDKProbeType.AWS_BILLING.toString(), 1);

    /**
     * A metric that tracks duration of execution for group stitching.
     */
    private static final DataMetricSummary GROUP_STITCHING_EXECUTION_DURATION_SUMMARY =
            DataMetricSummary.builder()
                    .withName("group_stitching_execution_duration_seconds")
                    .withHelp("Duration of execution of all group stitching operations.")
                    .build()
                    .register();

    private static final String CROSS_TARGET_STITCHING_KEY = "crs-tgt-%s";
    private static final String TARGET_LOCAL_STITCHING_KEY = "tgt-lcl-%d-%s";

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
        final GroupIdProvider crossTargetIdProvider = new CrossTargetStitcher(existingDiscoveredGroups);
        final GroupIdProvider targetLocalIdProvider = new TargetLocalStitcher(existingDiscoveredGroups);
        final Map<String, StitchingGroup> stitchingGroups = new HashMap<>();
        final Map<Long, Collection<UploadedGroup>> targetsToGroups =
                stitchingContext.getUploadedGroupsMap();
        for (Long targetId : getTargetsSorted(stitchingContext.getTargetIdToProbeType())) {
            for (UploadedGroup group : targetsToGroups.get(targetId)) {
                final GroupIdProvider idProvider =
                        (group.getDefinition().getType() == GroupType.RESOURCE
                                || group.getDefinition().getType() == GroupType.BILLING_FAMILY)
                                ? crossTargetIdProvider : targetLocalIdProvider;
                final String stitchKey = idProvider.getStitchingKey(targetId, group);
                final StitchingGroup foundGroup = stitchingGroups.get(stitchKey);
                if (foundGroup != null) {
                    logger.trace("Found 2 groups for stitching: {} and {}", () -> foundGroup,
                            group::getDefinition);
                    foundGroup.mergedGroup(group.getDefinition(), targetId);
                } else {
                    final Optional<DiscoveredObjectVersionIdentity> existingOid = idProvider.getId(
                            stitchKey);
                    final long oid;
                    if (existingOid.isPresent()) {
                        oid = existingOid.get().getOid();
                        logger.trace("Found existing OID {} for group {}", existingOid::get,
                                group::getDefinition);
                    } else {
                        oid = identityProvider.next();
                        logger.trace("Assigning new OID {} to group {}", () -> oid,
                                group::getDefinition);
                    }
                    final StitchingGroup stitchingGroup = new StitchingGroup(oid,
                            group.getDefinition(), group.getSourceIdentifier(), targetId,
                            !existingOid.isPresent(),
                            existingOid.map(DiscoveredObjectVersionIdentity::getHash).orElse(null));
                    stitchingGroups.put(stitchKey, stitchingGroup);
                }
            }
        }
        executionTimer.observe();

        final Set<Long> groupsToDelete = new HashSet<>(existingDiscoveredGroups.stream()
                .map(DiscoveredGroupId::getIdentity)
                .map(DiscoveredObjectVersionIdentity::getOid)
                .collect(Collectors.toSet()));
        groupsToDelete.removeAll(stitchingGroups.values()
                .stream()
                .map(StitchingGroup::getOid)
                .collect(Collectors.toList()));
        // These are groups that we know about, but they are related to targets, that are not
        // discovered yet. We simply do not touch them and remove them from groupToDelete set when
        // it is not empty.
        if (!stitchingContext.getUndiscoveredTargets().isEmpty() && !groupsToDelete.isEmpty()) {
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

    /**
     * Interface providing identity information about the group.
     */
    private interface GroupIdProvider {
        /**
         * Retrieves an existing OID for the specified group reported for the specified target.
         *
         * @param stitchingKey stitching key used to uniquely identify the group.
         * @return existing OID or {@link Optional#empty()}
         */
        @Nonnull
        Optional<DiscoveredObjectVersionIdentity> getId(@Nonnull String stitchingKey);

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
     * Identity provider for cross-target stitchable groups.
     * Groups are identified here by group type + discovered source id. If multiple values
     * are present, we ignore them, as they are obviously related to target-local groups
     */
    private static class CrossTargetStitcher implements GroupIdProvider {
        private final Map<String, DiscoveredObjectVersionIdentity> groupsToIds;

        CrossTargetStitcher(@Nonnull Collection<DiscoveredGroupId> discoveredGroups) {
            this.groupsToIds = Collections.unmodifiableMap(discoveredGroups.stream()
                    .collect(Collectors.groupingBy(
                            group -> Pair.create(group.getGroupType(), group.getSourceId())))
                    .values()
                    .stream()
                    .filter(set -> set.size() == 1)
                    .map(set -> set.iterator().next())
                    .collect(Collectors.toMap(key -> String.format(CROSS_TARGET_STITCHING_KEY,
                            GroupProtoUtil.createIdentifyingKey(key.getGroupType(),
                                    key.getSourceId())),
                            DiscoveredGroupId::getIdentity,
                            (val1, val2) -> val1)));
        }

        @Nonnull
        @Override
        public Optional<DiscoveredObjectVersionIdentity> getId(@Nonnull String stitchingKey) {
            return Optional.ofNullable(groupsToIds.get(stitchingKey));
        }

        @Nonnull
        @Override
        public String getStitchingKey(long target, @Nonnull UploadedGroup group) {
            return String.format(CROSS_TARGET_STITCHING_KEY,
                    GroupProtoUtil.createIdentifyingKey(group));
        }

        public String toString() {
            return groupsToIds.toString();
        }
    }

    /**
     * Identity provider for groups, that should not be stitched between targets.
     */
    private static class TargetLocalStitcher implements GroupIdProvider {
        private final Map<String, DiscoveredObjectVersionIdentity> groupsToIds;

        TargetLocalStitcher(@Nonnull Collection<DiscoveredGroupId> discoveredGroups) {
            groupsToIds = Collections.unmodifiableMap(discoveredGroups.stream()
                    .filter(group -> group.getTarget() != null)
                    .collect(Collectors.toMap(
                            key -> String.format(TARGET_LOCAL_STITCHING_KEY, key.getTarget(),
                                    GroupProtoUtil.createIdentifyingKey(key.getGroupType(),
                                            key.getSourceId())),
                            DiscoveredGroupId::getIdentity)));
        }

        @Nonnull
        @Override
        public Optional<DiscoveredObjectVersionIdentity> getId(@Nonnull String stitchingKey) {
            return Optional.ofNullable(groupsToIds.get(stitchingKey));
        }

        @Nonnull
        @Override
        public String getStitchingKey(long target, @Nonnull UploadedGroup group) {
            return String.format(TARGET_LOCAL_STITCHING_KEY, target,
                    GroupProtoUtil.createIdentifyingKey(group));
        }

        public String toString() {
            return groupsToIds.toString();
        }
    }
}
