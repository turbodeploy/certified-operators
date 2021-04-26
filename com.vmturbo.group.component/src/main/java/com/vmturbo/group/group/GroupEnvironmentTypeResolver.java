package com.vmturbo.group.group;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.CloudTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroupId;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Utility class for resolving group environment (& cloud) type.
 */
public class GroupEnvironmentTypeResolver {

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * A cache for targets, used when calculating the cloud type for groups with environment type
     * CLOUD.
     */
    private final ThinTargetCache thinTargetCache;

    /**
     * Group store to use when querying for group ids.
     */
    private final IGroupStore groupStore;

    /**
     * Constructor.
     *
     * @param thinTargetCache Cache that stores target information.
     * @param groupStore groupStore used in queries.
     */
    public GroupEnvironmentTypeResolver(@Nonnull ThinTargetCache thinTargetCache,
            @Nonnull final IGroupStore groupStore) {
        this.thinTargetCache = thinTargetCache;
        this.groupStore = groupStore;
    }

    /**
     * Returns the environment type and the cloud type for a group, given the list of its entities.
     *
     * @param groupId the group's id.
     * @param entities the list of group's entities (with all the necessary information for
     *                 environment type calculation).
     * @param discoveredGroups a map from group id to {@link DiscoveredGroupId} containing all the
     *                         discovered groups. For empty discovered groups, we try to deduce the
     *                         environment & cloud type based on the probe that discovered them.
     *                         When querying for environment type of user groups (e.g. during
     *                         creation/update), an empty optional can be passed as it is ignored.
     * @return the groups' environment and cloud type, wrapped inside a {@link GroupEnvironment}
     *         object.
     */
    public GroupEnvironment getEnvironmentAndCloudTypeForGroup(
            final long groupId,
            @Nonnull final Set<EntityWithOnlyEnvironmentTypeAndTargets> entities,
            @Nonnull final Multimap<Long, Long> discoveredGroups) {
        GroupEnvironment groupEnvironment;
        if (entities.size() > 0) {
            groupEnvironment = getEnvironmentAndCloudTypeForNonEmptyGroup(groupId, entities);
        } else {
            groupEnvironment = getEnvironmentAndCloudTypeForEmptyGroup(groupId, discoveredGroups);
        }

        if (groupEnvironment.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.HYBRID
                && groupEnvironment.getCloudType() == CloudTypeEnum.CloudType.UNKNOWN_CLOUD
                && !hasAppOrContainerEnvironmentTarget()) {
            // Fix the case where a group of non-cloud entities would report HYBRID env type.
            groupEnvironment = new GroupEnvironment(EnvironmentTypeEnum.EnvironmentType.ON_PREM,
                    CloudTypeEnum.CloudType.UNKNOWN_CLOUD);
        }
        return groupEnvironment;
    }

    /**
     * Calculates the environment & cloud type for the given group, based on the environment & cloud
     * type of its entities.
     *
     * @param groupId id of the group
     * @param entities group's entities
     * @return the environment & cloud type of the group, bundled in a {@link GroupEnvironment}.
     */
    private GroupEnvironment getEnvironmentAndCloudTypeForNonEmptyGroup(final long groupId,
            @Nonnull final Set<EntityWithOnlyEnvironmentTypeAndTargets> entities) {
        GroupEnvironment groupEnvironment = new GroupEnvironment();
        // Iterate through all entities in the group, and add their environment/cloud type to the
        // group's environment/cloud types.
        for (EntityWithOnlyEnvironmentTypeAndTargets entity : entities) {
            EnvironmentTypeEnum.EnvironmentType entityEnvironmentType =
                    entity.getEnvironmentType();
            if (entityEnvironmentType == null) {
                logger.warn("Environment type for entity with uuid {} missing; Entity was "
                                + "skipped during calculation of environment type for group "
                                + "with uuid {}",
                        entity.getOid(), groupId);
                continue;
            }
            groupEnvironment.addEnvironmentType(entityEnvironmentType);
            // calculate cloud type
            if (entity.getDiscoveringTargetIdsCount() > 0
                    && !groupEnvironment.getEnvironmentType().equals(
                    EnvironmentTypeEnum.EnvironmentType.ON_PREM)) {
                // If the entity is discovered by several targets, iterate over them to find one
                // with cloud type
                for (Long targetId : entity.getDiscoveringTargetIdsList()) {
                    Optional<CloudType> cloudTypeFromTarget = getCloudTypeFromTarget(targetId);
                    if (cloudTypeFromTarget.isPresent()) {
                        CloudType.toProtoCloudType(cloudTypeFromTarget.get())
                                .ifPresent(groupEnvironment::addCloudType);
                        break;
                    }
                }
                // If both environmentType and cloudType are HYBRID, then we can ignore the rest
                // of the entities since the result won't change.
                if (groupEnvironment.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.HYBRID
                        && groupEnvironment.getCloudType() == CloudTypeEnum.CloudType.HYBRID_CLOUD) {
                    break;
                }
            }
        }
        return groupEnvironment;
    }

    /**
     * Tries to resolve an empty group's environment & cloud type, by using heuristics or (in the
     * case of discovered groups) information from the target that discovered it.
     *
     * @param groupId id of the group
     * @param discoveredGroups A map containing information about discovered groups.
     * @return a {@link GroupEnvironment} bundling the environment & cloud type of the group (if
     *         they could be resolved, otherwise unknown for both).
     */
    private GroupEnvironment getEnvironmentAndCloudTypeForEmptyGroup(
            final long groupId,
            @Nonnull final Multimap<Long, Long>  discoveredGroups) {
        GroupEnvironment groupEnvironment = new GroupEnvironment();
        final GroupType groupType = groupStore.getGroupType(groupId);
        final Set<MemberType> expectedTypes =
                groupStore.getExpectedMemberTypesForGroup(groupId).columnKeySet();
        // If the group is discovered, try to derive env/cloud types based on the group's origin.
        if (discoveredGroups.containsKey(groupId)) {
            for (Long targetId : discoveredGroups.get(groupId)) {
                if (targetId != null) {
                    Optional<CloudType> cloudTypeFromTarget = getCloudTypeFromTarget(targetId);
                    if (cloudTypeFromTarget.isPresent()) {
                        CloudType.toProtoCloudType(cloudTypeFromTarget.get())
                                .ifPresent(groupEnvironment::addCloudType);
                        groupEnvironment.addEnvironmentType(
                                EnvironmentTypeEnum.EnvironmentType.CLOUD);
                    } else {
                        groupEnvironment.addEnvironmentType(
                                EnvironmentTypeEnum.EnvironmentType.ON_PREM);
                    }
                }
            }
        // case for empty cloud groups or regular groups with cloud groups
        } else if (groupType == GroupType.RESOURCE || groupType == GroupType.BILLING_FAMILY
                || expectedTypes.contains(
                MemberType.newBuilder().setGroup(GroupType.RESOURCE).build())
                || expectedTypes.contains(
                MemberType.newBuilder().setGroup(GroupType.BILLING_FAMILY).build())) {
            groupEnvironment.addEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD);
        }
        return groupEnvironment;
    }

    /**
     * Check whether the environment contains "App" or "Container" target.
     *
     * @return return true if the environment contains "App" or "Container" target,
     *         otherwise false.
     */
    private boolean hasAppOrContainerEnvironmentTarget() {
        return thinTargetCache.getAllTargets()
                .stream()
                .map(ThinTargetInfo::probeInfo)
                .collect(Collectors.toSet())
                .stream().anyMatch(probeInfo -> ProbeCategory.isAppOrContainerCategory(
                        ProbeCategory.create(probeInfo.category())));
    }

    /**
     * Fetches the cloud type that corresponds to a target.
     *
     * @param targetId the target's id
     * @return an optional containing the corresponding cloud type, or empty if no cloud type can be
     *         associated with that target.
     */
    private Optional<CloudType> getCloudTypeFromTarget(final long targetId) {
        Optional<ThinTargetInfo> thinInfo = thinTargetCache.getTargetInfo(targetId);
        if (thinInfo.isPresent() && (!thinInfo.get().isHidden())) {
            ThinTargetCache.ThinTargetInfo getProbeInfo = thinInfo.get();
            return CloudType.fromProbeType(getProbeInfo.probeInfo().type());
        }
        return Optional.empty();
    }
}
