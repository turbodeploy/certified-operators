package com.vmturbo.topology.processor.consistentscaling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingAndPolicyIdRecord;
import com.vmturbo.topology.processor.group.settings.TopologyProcessorSettingsConverter;

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
     * uploaded to the group component so that other components (e.g., market, SMA) can query
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
            // Add a scaling group membership setting to each group.
            group.getMemberList().stream()
                // Since the settings are shared amongst all members of the scaling group, the
                // setting will apply to all of them.
                .findAny()
                .ifPresent(oid -> {
                    // policies cannot be null, because all scaling group members have an entry
                    // pre-populated in userSettingsByEntityAndName.
                    Map<String, SettingAndPolicyIdRecord> policies =
                        userSettingsByEntityAndName.get(oid);
                    Setting setting = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.ScalingGroupMembership
                            .getSettingName())
                        .setStringSettingValue(StringSettingValue.newBuilder()
                            .setValue(groupName)).build();
                    SettingAndPolicyIdRecord settingAndPolicyIdRecord =
                        new SettingAndPolicyIdRecord(TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                Collections.singletonList(setting)), 0L,
                                Type.USER, false);
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

        ScalingGroupMember(final Grouping grouping, final TopologyEntity entity) {
            this.grouping = grouping;
            this.entity = entity;
        }
    }

    private void addPolicyOverride(final boolean enabled, final Grouping grouping,
                                   final List<TopologyEntity> allEntitiesInGroup) {
        if (enabled) {
            allEntitiesInGroup.forEach(e -> enabledEntities
                .add(new ScalingGroupMember(grouping, e)));
        } else {
            allEntitiesInGroup.forEach(e -> disabledEntities.add(e.getOid()));
        }
    }

    /**
     * Locate all entities that are members of a consistent scaling group and build internal scaling
     * groups. This also tracks policy settings for entities.
     *
     * @param resolvedGroups Resolved groups by ID.
     * @param topologyGraph Stream of TopologyEntity instances to add to CSM
     * @param settingPolicies    policies that apply to group
     */
    public void addEntities(final Map<Long, ResolvedGroup> resolvedGroups,
                            final TopologyGraph<TopologyEntity> topologyGraph,
                            final List<SettingPolicy> settingPolicies) {
        if (!config_.isEnabled()) {
            return;
        }
        // Check for a consistent scaling setting
        for (SettingPolicy sp : settingPolicies) {
            for (SettingProto.Setting setting : sp.getInfo().getSettingsList()) {
                if (setting.hasSettingSpecName()) {
                    final String specName = setting.getSettingSpecName();
                    if (specName.equals(EntitySettingSpecs.EnableConsistentResizing.getSettingName())) {
                        if (setting.hasBooleanSettingValue()) {
                            // We only care about the entities in the group that the policy
                            // actually applies to.
                            sp.getInfo().getScope().getGroupsList().forEach(groupId -> {
                                final ResolvedGroup resolvedGroup = resolvedGroups.get(groupId);
                                if (resolvedGroup != null) {
                                    final List<TopologyEntity> relevantEntitiesInGroup =
                                        resolvedGroup.getEntitiesOfType(ApiEntityType.fromType(sp.getInfo().getEntityType())).stream()
                                            .map(topologyGraph::getEntity)
                                            .filter(Optional::isPresent)
                                            .map(Optional::get)
                                            .collect(Collectors.toList());
                                    addPolicyOverride(setting.getBooleanSettingValue().getValue(),
                                        resolvedGroup.getGroup(), relevantEntitiesInGroup);
                                }
                            });
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
    }

    /**
     * Build internal scaling groups.  This combines the entities configured as consistent scaling
     * via policy and builds scaling groups, and merges groups as user groups with common entities
     * are encountered.  Due to this merging, an entity can only be a member of a single scaling
     * group.
     *
     * @param settings entity OID to settings map that is to be populated with shared settings
     */
    public void buildScalingGroups(final @Nonnull  Map<Long, Map<String, SettingAndPolicyIdRecord>> settings) {
        if (!config_.isEnabled()) {
            return;
        }

        // Remove all entities that have consistent scaling explicitly disabled via policy and
        // add the rest to scaling groups.
        enabledEntities.stream()
            .filter(member -> !disabledEntities.contains(member.entity.getOid()))
            .forEach(member -> addEntity(member));

        /*
         * Now pre-populate the settings map. The EntitySettingsResolver populates a single
         * SettingAndPolicyIdRecord map for each scaling group.  This function populates the
         * map from OID to settings map with empty settings such that all members in each scaling
         * group point to the same settings map. This way, when the EntitySettingsResolver
         * resolves a setting is for any entity in a scaling group, that setting is automatically
         * applied to all members of that group.
         */
        for (ScalingGroup group : groups.values()) {
            Map<String, SettingAndPolicyIdRecord> records = new HashMap<>();
            for (Long oid : group.getMemberList()) {
                settings.put(oid, records);
            }
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
