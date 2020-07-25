package com.vmturbo.common.protobuf.topology;

import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION;
import static com.vmturbo.platform.common.builders.SDKConstants.FREE_STORAGE_CLUSTER;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionPhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionStorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionVirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfoOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utilities for dealing with protobuf messages in topology/TopologyDTO.proto.
 */
public final class TopologyDTOUtil {

    private static DateTimeFormatter hmsFormat = DateTimeFormatter
        .ofPattern("HH:mm:ss")
        .withZone(ZoneOffset.UTC);

    private static final String STORAGE_CLUSTER_WITH_GROUP = "group";

    private static final String STORAGE_CLUSTER_ISO = "iso-";

    /**
     * Name of alleviate pressure plan type.
     */
    private static final String ALLEVIATE_PRESSURE_PLAN_TYPE = "ALLEVIATE_PRESSURE";

    /**
     * Name of optimize cloud plan type.
     */
    private static final String OPTIMIZE_CLOUD_PLAN = "OPTIMIZE_CLOUD";

    /**
     * Types of entities that are considered as workloads.
     */
    public static final Set<EntityType> WORKLOAD_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE,
            EntityType.DATABASE,
            EntityType.DATABASE_SERVER);

    /**
     * The primary tiers entity types. Cloud consumers like VMs and DBs can only consume from one
     * primary tier like compute / database tier. But they can consume from multiple
     * secondary tiers like storage tiers.
     */
    public static final Set<Integer> PRIMARY_TIER_VALUES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE, EntityType.DATABASE_TIER_VALUE);

    /**
     * Max allowable length of commodity key String.
     */
    public static final int MAX_KEY_LENGTH = 80;

    private static final Map<Integer, Integer> PRIMARY_TIER_FOR_CONSUMER_TYPE = ImmutableMap.of(
        EntityType.VIRTUAL_VOLUME_VALUE, EntityType.STORAGE_TIER_VALUE
    );

    public static final Set<Integer> TIER_VALUES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE, EntityType.STORAGE_TIER_VALUE);

    /**
     * Storage types.
     */
    public static final Set<EntityType> STORAGE_TYPES =
            ImmutableSet.of(EntityType.STORAGE, EntityType.STORAGE_TIER);

    /**
     * VC Probe collects the sum of CPU ready (wait time) over 20 second intervals and this sum is
     * only meaningful if you know what interval this value is collected over. Ideally this
     * interval should come from the probe. See https://kb.vmware.com/s/article/2002181 for more details.
     */
    public static final double QX_VCPU_BASE_COEFFICIENT = 20000.0;

    private TopologyDTOUtil() {
    }

    public static long getOid(@Nonnull final PartialEntity partialEntity) {
        switch (partialEntity.getTypeCase()) {
            case FULL_ENTITY:
                return partialEntity.getFullEntity().getOid();
            case MINIMAL:
                return partialEntity.getMinimal().getOid();
            case ACTION:
                return partialEntity.getAction().getOid();
            case API:
                return partialEntity.getApi().getOid();
            default:
                throw new IllegalArgumentException("Invalid type: " + partialEntity.getTypeCase());
        }
    }

    /**
     * Determine whether or not an entity is placed in whatever topology it belongs to.
     *
     * @param entity The {@link TopologyDTO.TopologyEntityDTO} to evaluate.
     * @return Whether or not the entity is placed (in whatever topology it belongs to).
     */
    public static boolean isPlaced(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
                // Only non-negative numbers are valid IDs, so we only consider an entity
                // to be placed if all commodities are bought from valid provider IDs.
                .allMatch(commBought -> commBought.hasProviderId() && commBought.getProviderId() >= 0);
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for a plan.
     *
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return Whether or not the described topology is generated for a plan.
     */
    public static boolean isPlan(@Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return topologyInfo.hasPlanInfo();
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for an optimize cloud plan.
     *
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return Whether or not the described topology is generated for a optimize cloud plan.
     */
    public static boolean isOptimizeCloudPlan(@Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return isPlan(topologyInfo) && topologyInfo.getPlanInfo().hasPlanType() &&
                OPTIMIZE_CLOUD_PLAN.equals(topologyInfo.getPlanInfo().getPlanType());
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for a plan of the given type.
     *
     * @param type A type of plan project.
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return Whether or not the described topology is generated for a plan of the given type.
     */
    public static boolean isPlanType(@Nonnull final PlanProjectType type,
                                     @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return isPlan(topologyInfo) && topologyInfo.getPlanInfo().getPlanProjectType() == type;
    }

    /**
     * Determine whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for alleviate pressure plan.
     * @param topologyInfo A type of plan project.
     * @return true if plan is of type alleviate pressure.
     */
    public static boolean isAlleviatePressurePlan(@Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
       return isPlan(topologyInfo) && topologyInfo.getPlanInfo().getPlanType().equals(ALLEVIATE_PRESSURE_PLAN_TYPE);
    }

    /**
     * Determines whether or not the topology described by a {@link TopologyDTO.TopologyInfo}
     * is generated for a Migrate to Public Cloud plan.
     *
     * @param topologyInfo The {@link TopologyDTO.TopologyInfo} describing a topology.
     * @return true if the plan is MPC
     */
    public static boolean isCloudMigrationPlan(@Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        return isPlan(topologyInfo) && PlanProjectType.CLOUD_MIGRATION.name().equals(
                topologyInfo.getPlanInfo().getPlanType());
    }

    /**
     * Checks if MCP plan allows for resize - only for consumption (Optimized) plan type.
     *
     * @param topologyInfo TopologyInfo to check.
     * @return Whether plan allows for resize.
     */
    public static boolean isResizableCloudMigrationPlan(@Nonnull final TopologyDTO.TopologyInfo
                                                                topologyInfo) {
        if (!isCloudMigrationPlan(topologyInfo)) {
            return false;
        }
        return CLOUD_MIGRATION_PLAN__CONSUMPTION.equals(topologyInfo.getPlanInfo().getPlanSubType());
    }

    /**
     * Gets the TopologyEntityDTOs of type connectedEntityType which are connected to entity.
     *
     * @param entity entity for which connected entities are retrieved
     * @param connectedEntityType the type of connectedEntity which should be retrieved
     * @return List of connected TopologyEntityDTOs
     */
    @Nonnull
    public static List<TopologyEntityDTO> getConnectedEntitiesOfType(
            @Nonnull final TopologyEntityDTO entity, final int connectedEntityType,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        return entity.getConnectedEntityListList().stream()
                .filter(e -> e.getConnectedEntityType() == connectedEntityType)
                .map(e -> topology.get(e.getConnectedEntityId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get the {@link TopologyEntityDTO}s of type connectedEntityType which are connected to an entity.
     *
     * @param topologyEntity entity for which connected entities are retrieved
     * @param connectedEntityType the type of connectedEntity which should be retrieved
     * @return List of connected TopologyEntityDTOs
     */
    @Nonnull
    public static List<TopologyEntityDTO> getConnectedEntitiesOfType(
            @Nonnull final TopologyEntityDTO topologyEntity, final Set<Integer> connectedEntityType,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        return topologyEntity.getConnectedEntityListList().stream()
            .filter(entity -> connectedEntityType.contains(entity.getConnectedEntityType()))
            .map(entity -> topology.get(entity.getConnectedEntityId()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Return a list containing the oids of the connected entities of the given type.
     *
     * @param entity to start from
     * @param connectedEntityType type of entity to search for
     * @return list of oids of connected entities
     */
    public static Stream<Long> getOidsOfConnectedEntityOfType(
        @Nonnull final TopologyEntityDTO entity, final int connectedEntityType) {
        return entity.getConnectedEntityListList().stream()
            .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                connectedEntityType)
            .map(ConnectedEntity::getConnectedEntityId);
    }

    /**
     * Is the entity type a primary tier entity type?
     * A primary tier is a tier like compute tier. Cloud consumers like VMs and DBs DBSs can only
     * consume from one primary tier like compute / database / database server tier. But they can
     * consume from multiple secondary tiers like storage tiers.
     *
     * @param entityType the entity type to be checked
     * @return true if the the entity type is a primary tier entity type. false otherwise.
     */
    public static boolean isPrimaryTierEntityType(int entityType) {
        return PRIMARY_TIER_VALUES.contains(entityType);
    }

    /**
     * Checks if the change provider refers to a move action within the same region. In that case,
     * the market generated move action is translated to a scale action.
     * If the move involves different regions (as in the case of cloud-to-cloud migration), then
     * we show move action as-is, and don't translate it to a scale.
     *
     * @param action Move action whose change providers to check.
     * @return True if change providers refers to a move action within same region.
     */
    public static boolean isMoveWithinSameRegion(@Nonnull final ActionDTO.Action action) {
        if (action.getInfo().getActionTypeCase() != ActionTypeCase.MOVE) {
            // Not a move action
            return false;
        }
        // First check the primary (tier) change provider, verify same entity (e.g compute tier) type.
        final ChangeProvider primaryChangeProvider = ActionDTOUtil.getPrimaryChangeProvider(action);
        if (!primaryChangeProvider.hasSource() || !primaryChangeProvider.hasDestination()) {
            // Not a move action
            return false;
        }
        boolean bothPrimaryTier = isPrimaryTierEntityType(primaryChangeProvider.getSource().getType())
                && isPrimaryTierEntityType(primaryChangeProvider.getDestination().getType());
        if (!bothPrimaryTier) {
            // Don't translate moves that are not b/w primary tiers.
            return false;
        }

        // Primary compute tiers source and destination are same. Check if we are doing a
        // region change, if so, we need to show a MOVE instead of SCALE action.
        // Find the change provider for region move
        final Optional<ChangeProvider> optRegion = action.getInfo().getMove()
                .getChangesList()
                .stream()
                .filter(cp -> cp.hasSource()
                        && cp.hasDestination()
                        && cp.getSource().getType() == EntityType.REGION_VALUE
                        && cp.getDestination().getType() == EntityType.REGION_VALUE)
                .findFirst();
        if (!optRegion.isPresent()) {
            // No region provided, translate move to scale in that case.
            return true;
        }
        final ChangeProvider regionProvider = optRegion.get();
        // If regions are same, we need to translate move to scale. Otherwise (as in
        // cloud-to-cloud migration case, we would be moving b/w same compute tier types,
        // but would be b/w different regions, so in that case, we need to return false.
        return regionProvider.getSource().getId() == regionProvider.getDestination().getId();
    }

    /**
     * Determine if an entity type plays the role of primary tier for a certain consumer entity.
     * Some entities (Storage Tiers) play the role of primary tier for some entities (Volumes)
     * but not for others (VMs).
     *
     * @param consumerType type of entity consuming from tier
     * @param providerType type of entity to be checked
     * @return true if the checked entity plays the role of primary tier to the consumer entity.
     */
    public static boolean isPrimaryTierEntityType(int consumerType, int providerType) {
        return isPrimaryTierEntityType(providerType) ||
            PRIMARY_TIER_FOR_CONSUMER_TYPE.getOrDefault(consumerType, EntityType.UNKNOWN_VALUE) ==
                providerType;
    }

    /**
     * Is the entity type a tier entity type?
     * A tier entity type is a cloud tier type like a compute tier or storage tier or database tier or
     * database server tier.
     *
     * @param entityType the entity type to be checked
     * @return true if the entity type is a tier entity type. false otherwise.
     */
    public static boolean isTierEntityType(int entityType) {
        return TIER_VALUES.contains(entityType);
    }

    /**
     * Is the entity type either an on prem storage or cloud storage tier type?
     *
     * @param entityType the entity type to be checked.
     * @return true if the entity type is either an on prem storage type or a cloud storage tier
     * entity type. false otherwise.
     */
    public static boolean isStorageEntityType(int entityType) {
        return STORAGE_TYPES.contains(EntityType.forNumber(entityType));
    }

    /**
     * Returns the index of the primary provider from the list of providers.
     * If there is only one provider, then that is the primary provider.
     * If there are multiple providers,
     * 1. For a VirtualMachine, we find the PM/Compute Tier provider.
     * 2. For other entity types, we return 0 as the index of the primary provider. This might
     * need changes in the future.
     *
     * @param targetEntityType the entity type of the target entity
     * @param targetOid the target entity's oid
     * @param providerTypes the provider entity types
     * @return
     */
    public static Optional<Integer> getPrimaryProviderIndex(int targetEntityType, long targetOid,
                                                            @Nonnull List<Integer> providerTypes) {
        if (providerTypes.isEmpty()) {
            return Optional.empty();
        }
        if (providerTypes.size() == 1) {
            return Optional.of(0);
        }
        switch (targetEntityType) {
            case EntityType.VIRTUAL_MACHINE_VALUE:
                return IntStream.range(0, providerTypes.size())
                    .filter(i -> providerTypes.get(i) == EntityType.PHYSICAL_MACHINE_VALUE
                        || providerTypes.get(i) == EntityType.COMPUTE_TIER_VALUE)
                    .boxed()
                    .findFirst();
            default:
                return Optional.of(0);
        }
    }

    /**
     * Check if the key of storage cluster commodity is for real storage cluster.
     * <p>
     * Real storage cluster is a storage cluster that is physically exits in the data center.
     * </p>
     * @param storageClusterCommKey key of storage cluster commodity key
     * @return true if it is for real cluster
     */
    public static boolean isRealStorageClusterCommodityKey(String storageClusterCommKey) {
        if (storageClusterCommKey == null) {
            return false;
        }
        storageClusterCommKey = storageClusterCommKey.toLowerCase();
        return !storageClusterCommKey.startsWith(STORAGE_CLUSTER_WITH_GROUP)
            && !storageClusterCommKey.startsWith(STORAGE_CLUSTER_ISO)
            && !storageClusterCommKey.equals(FREE_STORAGE_CLUSTER);
    }

    /**
     * Checks if the marketTier is connected to the entity with the provided entityId.
     *
     * @param marketTier to check for connectedness.
     * @param entityId id to test for connectedness with marketTier.
     * @return true if connected, false otherwise.
     */
    public static boolean areEntitiesConnected(TopologyEntityDTO marketTier, long entityId) {
        return marketTier.getConnectedEntityListList().stream()
                .map(ConnectedEntity::getConnectedEntityId).anyMatch(id -> id == entityId);
    }

    /**
     * Create a label for a source topology given its {@link TopologyInfo}. Used for logging.
     *
     * @param topologyInfo topology info
     * @return label string
     */
    public static String getSourceTopologyLabel(@Nonnull final TopologyInfo topologyInfo) {
        return getTopologyInfoSummary(topologyInfo, false);
    }

    /**
     * Create a label for a projected topology given its {@link TopologyInfo}. Used for logging.
     *
     * @param topologyInfo topology info
     * @return label string
     */
    public static String getProjectedTopologyLabel(@Nonnull final TopologyInfo topologyInfo) {
        return getTopologyInfoSummary(topologyInfo, true);
    }

    private static String getTopologyInfoSummary(TopologyInfo topologyInfo, boolean projected) {
        String topologyType = (projected ? "PROJECTED" : "SOURCE") + " " + topologyInfo.getTopologyType().name();
        final String hms = hmsFormat.format(Instant.ofEpochMilli(topologyInfo.getCreationTime()));
        return String.format("%s Topology @%s[id: %s; ctx: %s]",
            topologyType, hms, topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());
    }

    /**
     * Create a {@link ActionEntityTypeSpecificInfo} from a {@link TopologyDTO.TypeSpecificInfo}.
     *
     * @param typeSpecificInfo The input type-specific-info (or builder).
     * @return An optional containing the {@link ActionEntityTypeSpecificInfo}, or an empty optional
     *         if there is no associated action-specific object.
     */
    public static Optional<ActionEntityTypeSpecificInfo.Builder> makeActionTypeSpecificInfo(@Nonnull final TypeSpecificInfoOrBuilder typeSpecificInfo) {
        switch (typeSpecificInfo.getTypeCase()) {
            case COMPUTE_TIER:
                ComputeTierInfo tierInfo = typeSpecificInfo.getComputeTier();
                return Optional.of(ActionEntityTypeSpecificInfo.newBuilder()
                    .setComputeTier(ActionComputeTierInfo.newBuilder()
                        .setFamily(tierInfo.getFamily())
                        .setQuotaFamily(tierInfo.getQuotaFamily())
                        .setNumCores(tierInfo.getNumCores())
                        .setSupportedCustomerInfo(tierInfo.getSupportedCustomerInfo())));
            case VIRTUAL_MACHINE:
                VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();
                return createActionVmInfo(vmInfo)
                        .map(actionVmInfo -> ActionEntityTypeSpecificInfo.newBuilder()
                            .setVirtualMachine(actionVmInfo));
            case PHYSICAL_MACHINE:
                if (typeSpecificInfo.getPhysicalMachine().hasCpuCoreMhz()) {
                    return Optional.of(ActionEntityTypeSpecificInfo.newBuilder()
                        .setPhysicalMachine(ActionPhysicalMachineInfo.newBuilder()
                            .setCpuCoreMhz(typeSpecificInfo.getPhysicalMachine().getCpuCoreMhz())));
                } else {
                    return Optional.empty();
                }
            case STORAGE:
                return Optional.of(ActionEntityTypeSpecificInfo.newBuilder()
                    .setStorage(ActionStorageInfo.newBuilder()
                            .setStorageType(typeSpecificInfo.getStorage().getStorageType())));
            default:
                // No other action-specific data.
                return Optional.empty();
        }
    }

    private static Optional<ActionVirtualMachineInfo.Builder> createActionVmInfo(@Nonnull final VirtualMachineInfo vmInfo) {
        // Avoid creating an object if the necessary properties are not set.
        // Most notably, none of these properties are set for on-prem VMs.
        if (!(vmInfo.hasArchitecture() || vmInfo.hasVirtualizationType()
                || (vmInfo.hasDriverInfo() && !vmInfo.getDriverInfo().equals(DriverInfo.getDefaultInstance()))
                || !StringUtils.isEmpty(vmInfo.getLocks()))) {
            return Optional.empty();
        } else {
            ActionVirtualMachineInfo.Builder actionVmInfo = ActionVirtualMachineInfo.newBuilder();
            if (vmInfo.hasArchitecture()) {
                actionVmInfo.setArchitecture(vmInfo.getArchitecture());
            }
            if (vmInfo.hasVirtualizationType()) {
                actionVmInfo.setVirtualizationType(vmInfo.getVirtualizationType());
            }
            if (vmInfo.hasDriverInfo() && !vmInfo.getDriverInfo().equals(DriverInfo.getDefaultInstance())) {
                actionVmInfo.setDriverInfo(vmInfo.getDriverInfo());
            }
            if (!StringUtils.isEmpty(vmInfo.getLocks())) {
                actionVmInfo.setLocks(vmInfo.getLocks());
            }
            return Optional.of(actionVmInfo);
        }
    }

    /**
     * Get volume provider. For on prem case, the result contains storage connected to the
     * volume. For cloud case, the result contains storage tier selling commodities to
     * the volume.
     *
     * @param volume Virtual volume.
     * @return optional OID of volume providers (Storage or Storage Tier).
     */
    public static Optional<Long> getVolumeProvider(@Nonnull final TopologyEntityDTO volume) {
        if (volume.getEnvironmentType() == EnvironmentType.CLOUD) {
            // Get storage tier selling commodities to the volume (cloud case)
            return volume.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(commBought -> commBought.getProviderEntityType()
                            == EntityType.STORAGE_TIER_VALUE)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .findFirst();
        } else {
            // Get storage connected to the volume (on prem case)
            return TopologyDTOUtil.getOidsOfConnectedEntityOfType(volume,
                    EntityType.STORAGE.getNumber()).findFirst();
        }
    }
}