package com.vmturbo.common.protobuf;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utilities for dealing with protobuf messages in topology/TopologyDTO.proto.
 */
public final class TopologyDTOUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Name of alleviate pressure plan type.
     */
    private static final String ALLEVIATE_PRESSURE_PLAN_TYPE = "ALLEVIATE_PRESSURE";

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
}