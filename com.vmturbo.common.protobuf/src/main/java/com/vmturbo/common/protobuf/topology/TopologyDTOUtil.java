package com.vmturbo.common.protobuf.topology;

import static com.vmturbo.platform.common.builders.SDKConstants.FREE_STORAGE_CLUSTER;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utilities for dealing with protobuf messages in topology/TopologyDTO.proto.
 */
public final class TopologyDTOUtil {
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
     * The primary tiers entity types. Cloud consumers like VMs and DBs can only consume from one
     * primary tier like compute / database tier. But they can consume from multiple
     * secondary tiers like storage tiers.
     */
    public static final Set<Integer> PRIMARY_TIER_VALUES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE, EntityType.DATABASE_TIER_VALUE);

    public static final Set<Integer> TIER_VALUES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE, EntityType.STORAGE_TIER_VALUE);

    public static final Set<Integer> STORAGE_VALUES = ImmutableSet.of(EntityType.STORAGE_VALUE,
            EntityType.STORAGE_TIER_VALUE);

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
     * Gets the TopologyEntityDTOs of type connectedEntityType which are connected to entity
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
        return STORAGE_VALUES.contains(entityType);
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
}
