package com.vmturbo.topology.processor.consistentscaling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingAndPolicyIdRecord;

/**
 * This manages the grouping of entities into consistent scaling groups.
 */
public class ConsistentScalingManager {

    private static final Logger logger = LogManager.getLogger();
    private final ConsistentScalingConfig config_;

    // Maps Entity OIDs to their internal scaling group
    private Map<Long, ScalingGroup> entityToScalingGroup = new HashMap<>();
    // Maps Group ID + CSP type to internal scaling group
    private Map<String, ScalingGroup> groups = new HashMap<>();

    // List of discovered groups that have consistent scaling enabled via mediation (e.g., AWS
    // ASGs, Azure availability sets and scale sets).
    private Set<String> discoveredGroups = new HashSet<>();
    // List of entities that are consistent scaling due to policy
    private List<ScalingGroupMember> enabledEntities = new ArrayList<>();
    // List of entities that are not consistent scaling due to policy
    private Set<Long> disabledEntities = new HashSet<>();

    /**
     * Constructor.
     *
     * @param config CSM configuration
     */
    public ConsistentScalingManager(ConsistentScalingConfig config) {
        this.config_ = config;
    }

    /**
     * Return stream consisting of merged set of setting policies applied to all members of this
     * scaling group.  After scaling groups are built, this set of setting policies will be applied
     * to all members of the group.
     *
     * @return stream of setting policies
     */
    public Stream<Pair<Set<Long>, List<SettingPolicy>>> getPoliciesStream() {
        return this.groups.values().stream()
            .filter(sg -> !sg.getSettingPolicies().isEmpty())
            .distinct()
            .map(group -> new Pair<>(group.getMemberList(),
                new ArrayList<>(group.getSettingPolicies())));
    }

    /**
     * Return the scaling group ID of the indicated entity OID.
     * @param entityOid the OID of the entity to look up
     * @return Optional scaling group ID if the entity is in a scaling group.  If the OID is invalid or
     * the entity is not in a scaling group, return Optional.empty().
     */
    public Optional<String> getScalingGroupId(Long entityOid) {
        return entityOid == null
            ? Optional.empty()
            : Optional.ofNullable(entityToScalingGroup.get(entityOid))
                    .map(ScalingGroup::getScalingGroupId);
    }

    /**
     * Add a scaling group membership setting to each member of a scaling group.  These settings are
     * uploaded to the group component so that other components (especially market) can query
     * scaling group definitions.
     *
     * @param userSettingsByEntityAndName existing map of Topology entity to setting and policy
     *                                    records.  This map will be updated with
     *                                    scalingGroupMembership settings for each entity.
     */
    public void addScalingGroupSettings(final Map<Long, Map<String, SettingAndPolicyIdRecord>>
                                            userSettingsByEntityAndName) {
        groups.values().forEach(group -> {
            String groupName = group.getScalingGroupId();
            // Add a scaling group membership setting to each member
            group.getMemberList().forEach(oid -> {
                Map<String, SettingAndPolicyIdRecord> policies = userSettingsByEntityAndName.get(oid);
                if (policies == null) {
                    policies = new HashMap<>();
                    userSettingsByEntityAndName.put(oid, policies);
                }
                Setting setting = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.ScalingGroupMembership
                        .getSettingName())
                    .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(groupName)).build();
                SettingAndPolicyIdRecord settingAndPolicyIdRecord =
                    new SettingAndPolicyIdRecord(setting, 0L, Type.USER, false);
                policies.put(EntitySettingSpecs.ScalingGroupMembership.getSettingName(),
                    settingAndPolicyIdRecord);
            });
        });
    }

    /**
     * Internal scaling group definition.
     */
    class ScalingGroupMember {
        public Grouping grouping;
        public TopologyEntity entity;
        public List<SettingPolicy> policies;

        ScalingGroupMember(final Grouping grouping, final TopologyEntity entity,
                           final List<SettingPolicy> policies) {
            this.grouping = grouping;
            this.entity = entity;
            this.policies = policies;
        }
    }

    private void addPolicyOverride(final boolean enabled, final Grouping grouping,
                                   final List<TopologyEntity> allEntitiesInGroup,
                                   final List<SettingPolicy> policies) {
        if (enabled) {
            allEntitiesInGroup.forEach(e -> enabledEntities
                .add(new ScalingGroupMember(grouping, e, policies)));
        } else {
            allEntitiesInGroup.forEach(e -> disabledEntities.add(e.getOid()));
        }
    }

    /**
     * Locate all entities that are members of a consistent scaling group and build internal scaling
     * groups. This also tracks policy settings for entities.
     *
     * @param grouping           Associated user Group.
     * @param allEntitiesInGroupStream Stream of TopologyEntity instances to add to CSM
     * @param settingPolicies    policies that apply to group
     */
    public void addEntities(final Grouping grouping,
                            final Stream<TopologyEntity> allEntitiesInGroupStream,
                            final List<SettingPolicy> settingPolicies) {
        if (!config_.isEnabled()) {
            return;
        }
        // Check for a consistent scaling setting
        List<TopologyEntity> allEntitiesInGroup =
                allEntitiesInGroupStream.collect(Collectors.toList());
        for (SettingPolicy sp : settingPolicies) {
            for (SettingProto.Setting setting : sp.getInfo().getSettingsList()) {
                if (setting.hasSettingSpecName()) {
                    final String specName = setting.getSettingSpecName();
                    if (specName
                        .equals(EntitySettingSpecs.EnableConsistentResizing.getSettingName())) {
                        if (setting.hasBooleanSettingValue()) {
                            addPolicyOverride(setting.getBooleanSettingValue().getValue(),
                                grouping, allEntitiesInGroup, settingPolicies);
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Clear CSM state that doesn't need to persist.
     */
    public void clear() {
        groups.clear();
        enabledEntities.clear();
        disabledEntities.clear();
        entityToScalingGroup.clear();
    }

    /**
     * Clear discovered groups.
     */
    public void clearDiscoveredGroups() {
        discoveredGroups.clear();
    }

    /**
     * Generate a group key based on the group's UUID and the CSP of the ServiceEntity.
     *
     * @param grouping the group for which to generate a key
     * @return a string representing a group key qualified by CSP type
     */
    private String makeGroupKey(Grouping grouping) {
        return Long.toString(grouping.getId());
    }

    /**
     * Add a service entity to a group.  If this service entity is found to be a member of multiple
     * scaling groups, this logic will merge those groups into a single scaling group.
     *
     * @param member ScalingGroupMember information to add
     */
    @VisibleForTesting
    private void addEntity(ScalingGroupMember member) {
        Grouping grouping = member.grouping;
        TopologyEntity te = member.entity;
        // Do not add non-controllable entities to scaling groups
        if (!member.entity.getTopologyEntityDtoBuilder().getAnalysisSettings().getControllable()) {
            return;
        }
        Long entityId = te.getOid();
        // If the SE is already in a group, and this is a new group, reuse the group
        ScalingGroup memberOf = entityToScalingGroup.get(entityId);
        String groupKey = makeGroupKey(grouping);
        ScalingGroup scalingGroup = groups.get(groupKey);

        // If the SE is a member of an existing group, we might need to merge groups
        if (memberOf != null && scalingGroup != null) {
            if (memberOf != scalingGroup) {
                // Merge the groups. memberOf is the master because it already contains this SE,
                // and scalingGroup will disappear.
                if (logger.isDebugEnabled()) {
                    logger.debug("Merging scaling group {} into {}",
                        scalingGroup, memberOf);
                }
                memberOf.getMemberList().addAll(scalingGroup.getMemberList());
                scalingGroup.getMemberList().forEach(id -> entityToScalingGroup.put(id, memberOf));
                // Merge template exclusions
                memberOf.getSettingPolicies().addAll(scalingGroup.getSettingPolicies());
                memberOf.getContributingGroups().addAll(scalingGroup.getContributingGroups());
                groups.put(groupKey, memberOf); // both keys now point to same scaling group
            }
            return;
        }

        // If this is a new scaling group, either reuse the group that the SE is already a member
        // of, or create a new one.
        if (scalingGroup == null) {
            if (memberOf != null) {
                scalingGroup = memberOf;
            } else {
                // Need to create a new group
                scalingGroup = new ScalingGroup(groupKey, te.getEnvironmentType());
                if (logger.isDebugEnabled()) {
                    logger.debug("Created scaling group: {}", scalingGroup);
                }
            }
            groups.put(groupKey, scalingGroup);
        }

        if (memberOf == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Adding {} to scaling group: {}", te.getDisplayName(), scalingGroup);
            }
            entityToScalingGroup.put(entityId, scalingGroup);
            scalingGroup.addEntity(entityId, grouping);
        }
        if (member.policies != null) {
            scalingGroup.getSettingPolicies().addAll(member.policies);
        }
    }

    /**
     * Build internal scaling groups.  This combines the list of discovered groups with consistent
     * scaling with entities that are configured as consistent scaling via policy and builds scaling
     * groups, and merges groups as user groups with common entities are encountered.  Due to this
     * merging, an entity can only be a member of a single scaling group.
     *
     * @param graph        the topology
     * @param groupingList List of known groups
     */
    public void buildScalingGroups(final TopologyGraph<TopologyEntity> graph,
                                   final @Nonnull Iterator<Grouping> groupingList) {
        if (!config_.isEnabled()) {
            return;
        }
        // Iterate over all groups and identify those that have consistent scaling enabled in
        // the probe.  Add the members of those groups to the enabled list.
        groupingList.forEachRemaining(grouping -> {
            // If this group is a consistent scaling group, add its members to the enabled list.
            if (discoveredGroups.contains(grouping.getDefinition().getDisplayName())) {
                grouping
                    .getDefinition()
                    .getStaticGroupMembers()
                    .getMembersByTypeList().forEach(g -> {
                        g.getMembersList().forEach(oid -> {
                            enabledEntities.add(new ScalingGroupMember(grouping,
                                graph.getEntity(oid).get(), null));
                        });
                    });
            }
        });

        // Remove all entities that have consistent scaling explicitly disabled via policy and
        // add the rest to scaling groups.
        enabledEntities.stream()
            .filter(member -> !disabledEntities.contains(member.entity.getOid()))
            .forEach(member -> addEntity(member));
    }

    /**
     * Query the list of groups and building internal scaling groups.  See above for details.
     *
     * @param graph         topology
     * @param groupResolver group resolver
     */
    public void buildScalingGroups(final TopologyGraph<TopologyEntity> graph,
                                   final @Nonnull GroupResolver groupResolver) {
        ListFilter.Builder listFilter = ListFilter.newBuilder();
        GroupFilter groupFilter = GroupFilter.newBuilder()
            // Need to exclude clusters, resource groups, etc. to make the query more efficient.
            .setGroupType(GroupType.REGULAR)
            .addDirectMemberTypes(MemberType.newBuilder()
                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
            .addDirectMemberTypes(MemberType.newBuilder()
                .setEntity(EntityType.CONTAINER_VALUE))
            .build();
        buildScalingGroups(graph, groupResolver.getGroupServiceClient()
            .getGroups(GetGroupsRequest.newBuilder().setGroupFilter(groupFilter).build()));
    }

    /**
     * Add a discovered scaling group.  This notes the name of groups that have been discovered that
     * have the consistent scaling setting in the SDK DTO set to true.  This is currently set on AWS
     * ASGs, Azure Availability and Scale Sets, and Kubernetes replica sets/deployments.  Since this
     * setting is lost in the group uploader stage, we note the group names for later use in the
     * entity settings resolution stage to determine which entities are in a scaling group.
     *
     * @param group discovered group.
     */
    public void addDiscoveredGroup(InterpretedGroup group) {
        if (!config_.isEnabled()) {
            return;
        }
        logger.debug("CSM adding an InterpretedGroup: {}", group);
        CommonDTO.GroupDTO dto = group.getOriginalSdkGroup();
        if (dto.getIsConsistentResizing()) {
            discoveredGroups.add(dto.getDisplayName());
        }
    }


    /**
     * Contains state for internal scaling groups.
     */
    class ScalingGroup {
        private boolean isCloud;
        private final String key;
        private Set<Long> memberList;
        private Set<String> contributingGroups;
        private Set<SettingPolicy> policies;

        @Override
        public String toString() {
            return String.format("CSG-%s[%s]", isCloud ? "Cloud" : "OnPrem",
                getContributingGroupsString());
        }

        ScalingGroup(String key, EnvironmentType environmentType) {
            this.key = key;
            this.isCloud = environmentType == EnvironmentType.CLOUD;
            this.memberList = new HashSet<>();
            this.contributingGroups = new HashSet<>();
            this.policies = new HashSet<>();
        }

        Set<Long> getMemberList() {
            return this.memberList;
        }

        public void addEntity(final Long entityId, final Grouping grouping) {
            memberList.add(entityId);
            // Display names are more user-friendly, so use it if it's present
            contributingGroups.add(grouping.getDefinition().getDisplayName());
        }

        public String getContributingGroupsString() {
            return this.contributingGroups.stream().collect(Collectors.joining(", "));
        }

        Set<String> getContributingGroups() {
            return this.contributingGroups;
        }

        public Set<SettingPolicy> getSettingPolicies() {
            return this.policies;
        }

        /**
         * Return the key that will be used to identify this scaling group in the group component.
         * This value will be used as the scaling group ID as well as the visible group identifier
         * in resize/configure actions.
         *
         * @return scaling group ID
         */
        public String getScalingGroupId() {
            return getContributingGroupsString();
        }
    }
}
