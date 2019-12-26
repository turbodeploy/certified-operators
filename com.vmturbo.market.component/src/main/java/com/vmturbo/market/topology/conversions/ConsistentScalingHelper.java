package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Class to provide consistent scaling functions that reside in the market component.
 */
public class ConsistentScalingHelper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * This is the list of commodities that are taken into consideration when leveling usage for
     * cloud scaling groups.
     */
    private static final ImmutableSet<Integer> consistentScalingCommodities = ImmutableSet.of(
        CommodityDTO.CommodityType.CPU_VALUE, CommodityType.CPU_PROVISIONED_VALUE,
        CommodityDTO.CommodityType.MEM_VALUE, CommodityType.MEM_PROVISIONED_VALUE
    );

    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final HashMap<Long, ScalingGroup> oidToGroup;
    private Map<String, ScalingGroup> groups;

    /**
     * Create a {@link ConsistentScalingHelper} instance.
     * @param settingPolicyService {@link SettingPolicyServiceBlockingStub} to use for querying
     *                             scaling group membership.
     */
    public ConsistentScalingHelper(SettingPolicyServiceBlockingStub settingPolicyService) {
        this.settingPolicyService = settingPolicyService;
        this.groups = new HashMap<>();      // Scaling group ID to scaling group
        this.oidToGroup = new HashMap<>();  // Trader oid to scaling group
    }

    /**
     * Query the setting policy service for consistent scaling membership entries.
     * @param settingPolicyService handle to setting policy service
     * @return a stream of EntitySettingGroup objects that contain scaling group membership groups
     * and entities.
     */
    private static Stream<EntitySettingGroup>
    fetchConsistentScalingSettings(final SettingPolicyServiceBlockingStub settingPolicyService) {
        EntitySettingFilter.Builder entitySettingFilter = EntitySettingFilter.newBuilder()
            .addSettingName(EntitySettingSpecs.ScalingGroupMembership.getSettingName());
        GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
            .setSettingFilter(entitySettingFilter)
            .setIncludeSettingPolicies(false).build();
        return SettingDTOUtil.flattenEntitySettings(settingPolicyService.getEntitySettings(request));
    }

    /**
     * Initializes the consistent resizer
     * - Queries the group component for scaling group membership
     * - Initializes top usage tracker. As each topology entity is processed, the top usage for each
     *   group will be updated.
     * @param topology map of OID to topology entity
     */
    void initialize(final Map<Long, TopologyEntityDTO> topology) {
        logger.info("Initializing ConsistentScalingHelper");

        // Get scaling group membership information
        List<EntitySettingGroup> entitySettingGroups =
            fetchConsistentScalingSettings(settingPolicyService)
                // Consistent scaling is meaningless on groups with less than two members, so don't
                // create them.
                .filter(esg -> esg.getEntityOidsList().size() > 1)
                .collect(Collectors.toList());

        for (EntitySettingGroup esg : entitySettingGroups) {
            // EntitySettingGroup contains the setting name, which represents the scaling group
            // name.  This name represents the concatenation of the user groups contributed to
            // the group.  This name will serve as the scaling group ID that is in the Trader.
            final String groupName = esg.getSetting().getStringSettingValue().getValue();
            ScalingGroup group = new ScalingGroup(groupName);
            if (logger.isDebugEnabled()) {
                logger.debug("Created scaling group {}", groupName);
            }
            int groupFactor = 0;
            for (Long oid : esg.getEntityOidsList()) {
                TopologyEntityDTO entity = topology.get(oid);
                if (entity == null) {
                    // Invalid OID, so skip
                    continue;
                }
                group.addMember(oid);
                if (canBeGroupLeader(entity)) {
                    groupFactor++;
                    if (groupFactor == 1) {
                        // The first leader candidate is the group leader
                        group.setGroupLeader(entity);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Scaling group leader for {} is {}", groupName,
                                entity.getDisplayName());
                        }
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding {} to scaling group {}", entity.getDisplayName(),
                        groupName);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Group factor for {} is {}", groupName, groupFactor);
            }
            group.setGroupFactor(groupFactor);
            // If there is no group leader, then the scaling group is not valid at this time,
            // so discard it.
            if (group.getGroupLeader() != null) {
                for (Long oid : esg.getEntityOidsList()) {
                    oidToGroup.put(oid, group);
                }
                groups.put(groupName, group);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Group {} has no group leader - discarding", groupName);
                }
            }
        }
        logger.info("ConsistentScalingHelper initialization complete");
    }

    /**
     * Return whether the entity can be a group leader.  A member can be a group leader if it
     * can be placed in the market.
     * @param entity entity to check
     * @return true if the entity can be a scaling group leader
     */
    private boolean canBeGroupLeader(final TopologyEntityDTO entity) {
        return entity.getEntityState() == EntityState.POWERED_ON;
    }

    /**
     * Return the scaling group ID for the provided OID, if available.
     * @param oid OID to check
     * @return Optional containing the scaling group ID, if available.
     */
    public Optional<String> getScalingGroupId(final Long oid) {
        return Optional.ofNullable(oidToGroup.get(oid)).map(ScalingGroup::getName);
    }

    /**
     * Return list of members OIDs in a scaling group.
     * @param scalingGroupId group ID
     * @return List of OIDs in scaling group.  Returns empty list if the scaling group does not
     * exist or is otherwise invalid.
     */
    Set<Long> getGroupMembers(final String scalingGroupId) {
        if (scalingGroupId == null) {
            return new HashSet<>();
        }
        ScalingGroup group = groups.get(scalingGroupId);
        if (group == null) {
            return new HashSet<>();
        }
        return group.getMembers();
    }

    /**
     * Get this entity's scaling group peers.
     * @param oid OID of entity to check
     * @param includeSelf true if the oid being looked up should be included in the peers list
     * @return set of peer members.  Return empty set if the oid does not belong to a scaling group
     * or if it is the only member of a group and includeSelf is false.
     */
    Set<Long> getPeers(Long oid, boolean includeSelf) {
        Optional<String> id = getScalingGroupId(oid);
        if (id.isPresent()) {
            Set<Long> peers = getGroupMembers(id.get());
            if (!includeSelf) {
                peers.remove(oid);
            }
            return peers;
        } else {
            return new HashSet<>();
        }
    }

    /**
     * Get the scaling group associated with a topology entity.
     * @param entity to check
     * @param cloudOnly true if matching only cloud entities
     * @return ScalingGroup, if present.  If the entity is not a cloud entity and a cloud match
     * was requested, also return Optional.empty().
     */
    Optional<ScalingGroup> getScalingGroup(final TopologyEntityDTO entity,
                                           final boolean cloudOnly) {
        if (entity == null || (cloudOnly && entity.getEnvironmentType() != EnvironmentType.CLOUD)) {
            return Optional.empty();
        }
        return Optional.ofNullable(oidToGroup.get(entity.getOid()));
    }

    /**
     * Get the group factor for the specified entity.  The group factor is used for cloud VM
     * pricing, so Container and on-prem VMs return a group factor of 1.
     * @param entityDTO entity to look up
     * @return group factor for the entity
     */
    public int getGroupFactor(TopologyEntityDTO entityDTO) {
        Optional<ScalingGroup> scalingGroup = getScalingGroup(entityDTO, true);
        return scalingGroup.isPresent() ? scalingGroup.get().getGroupFactor(entityDTO.getOid()) : 1;
    }

    public Collection<ScalingGroup> getGroups() {
        return this.groups.values();
    }

    /**
     * Return whether we support consistently scaling the given commodity type.
     * @param commodityType the commodity type to check
     * @return true if we consistently scale this commodity type.
     */
    public boolean isCommodityConsistentlyScalable(final TopologyDTO.CommodityType commodityType) {
        return commodityType != null &&
            consistentScalingCommodities.contains(commodityType.getType());
    }

    /**
     * Return the scaling group top usage and provider Id of the given OID.
     * @param oid OID for which to obtain scaling group usage.
     * @return Optional scaling group usage if available, or empty.
     */
    public Optional<ScalingGroupUsage> getScalingGroupUsage(final Long oid) {
        ScalingGroup sg = oidToGroup.get(oid);
        return sg != null ? Optional.of(sg.getScalingGroupUsage()) : Optional.empty();
    }

    /**
     * Factory for instances of {@link ConsistentScalingHelper}.
     */
    public static class ConsistentScalingHelperFactory {
        private SettingPolicyServiceBlockingStub settingPolicyService;

        /**
         * Constructor.
         * @param settingPolicyService {@link SettingPolicyServiceBlockingStub} to use for querying
         *                             scaling group membership.
         */
        public ConsistentScalingHelperFactory(SettingPolicyServiceBlockingStub settingPolicyService) {
            this.settingPolicyService = settingPolicyService;
        }

        /**
         * Create a new {@link ConsistentScalingHelper} instance.
         * @param topologyInfo information about the topology
         * @param shoppingListOidToInfos the map of shopping list oids to infos
         * @return new instance of{@link TierExcluder}
         */
        @Nonnull
        public ConsistentScalingHelper newConsistentScalingHelper(TopologyInfo topologyInfo,
                                           Map<Long, ShoppingListInfo> shoppingListOidToInfos) {
            return new ConsistentScalingHelper(settingPolicyService);
        }
    }

    /**
     * Contains consistent scaling state that is required by the market component.
     */
    static class ScalingGroup {
        private String name;
        private Set<Long> members;
        private ScalingGroupUsage usage;

        // Cloud group leader builder.  This will always be Optional.empty() for Container and
        // on-prem VM groups.
        private TopologyEntityDTO groupLeader;
        private int groupFactor;

        ScalingGroup(String name) {
            this.name = name;
            this.groupLeader = null;
            this.groupFactor = 0;
            this.usage = new ScalingGroupUsage(name);
            this.members = new HashSet<>();
        }

        void addMembers(final List<Long> entityOidsList) {
            members.addAll(entityOidsList);
        }

        void addMember(final Long entityOid) {
            members.add(entityOid);
        }

        public String getName() {
            return this.name;
        }

        Set<Long> getMembers() {
            return this.members;
        }

        ScalingGroupUsage getScalingGroupUsage() {
            return this.usage;
        }

        /**
         * Get the OID of the group leader.
         * @return OID of the group leader.  This will fail if the group is empty.  This should
         * never happen because ScalingGroup creation is triggered by the presence of the first
         * member in the group, and there is no way to remove a member.
         */
        Long getGroupLeader() {
            if (this.groupLeader == null) {
                return null;
            }
            return this.groupLeader.getOid();
        }

        boolean isGroupLeader(final Long oid) {
            return getGroupLeader().equals(oid);
        }

        int getGroupFactor(Long oid) {
            // The first eligible member in the member list is the group leader, so return the group
            // factor.  All other members of the scaling group have a group factor of 0.
            return isGroupLeader(oid) ? this.groupFactor : 0;
        }

        public boolean isCloudGroup() {
            return this.groupLeader.getEnvironmentType() == EnvironmentType.CLOUD;
        }

        public void setGroupLeader(final TopologyEntityDTO entity) {
            this.groupLeader = entity;
            if (logger.isDebugEnabled()) {
                logger.debug("Setting {} as leader of scaling group {}",
                    entity.getDisplayName(), this.getName());
            }
        }

        public void setGroupFactor(final int groupFactor) {
            this.groupFactor = groupFactor;
        }
    }
}
