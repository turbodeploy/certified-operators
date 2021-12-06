package com.vmturbo.common.protobuf.action;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utils to support expansion over involved entities.
 */
public class InvolvedEntityExpansionUtil {
    /**
     * These are non infrastructure entities that need to be included while expanding entities like namespace.
     */
    public static final Set<Integer> EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA = ImmutableSet.of(
            ApiEntityType.NAMESPACE.typeNumber(),
            ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber(),
            ApiEntityType.BUSINESS_APPLICATION.typeNumber(),
            ApiEntityType.BUSINESS_TRANSACTION.typeNumber(),
            ApiEntityType.SERVICE.typeNumber(),
            ApiEntityType.APPLICATION_COMPONENT.typeNumber(),
            ApiEntityType.CONTAINER.typeNumber(),
            ApiEntityType.CONTAINER_POD.typeNumber(),
            ApiEntityType.WORKLOAD_CONTROLLER.typeNumber(),
            ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
            ApiEntityType.DATABASE_SERVER.typeNumber());

    /**
     * These are infrastructure entities that need to be included while expanding entities like namespace.
     * Currently, the infrastructure entities are: Physical Machine, Storage, Virtual Volume.
     */
    public static final Set<Integer> EXPANSION_TOWARDS_ENTITY_TYPES_INFRA = ImmutableSet.of(
            ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
            ApiEntityType.STORAGE.typeNumber(),
            ApiEntityType.PHYSICAL_MACHINE.typeNumber());

    /**
     * The map includes all the entities required expansion (risk proporgation), and which entities it should expand to.
     * For Namespace Entity, we want to expand to the non infra entities only
     * For the rest (for now), we want to expand to both infra and non-infra entities
     * Key: EntityType the needs to be expanded
     * Value: EntityTypes that needs to be expanded to
     */
    public static final ImmutableMap<Integer, Set<Integer>> ENTITY_WITH_EXPAND_ENTITY_SET_MAP
            = ImmutableMap.of(ApiEntityType.NAMESPACE.typeNumber(), ImmutableSet.<Integer>builder()
                    .addAll(EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA).build(),
            ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber(), ImmutableSet.<Integer>builder()
                    .addAll(EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA).addAll(EXPANSION_TOWARDS_ENTITY_TYPES_INFRA).build(),
            ApiEntityType.BUSINESS_APPLICATION.typeNumber(), ImmutableSet.<Integer>builder()
                    .addAll(EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA).addAll(EXPANSION_TOWARDS_ENTITY_TYPES_INFRA).build(),
            ApiEntityType.BUSINESS_TRANSACTION.typeNumber(), ImmutableSet.<Integer>builder()
                    .addAll(EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA).addAll(EXPANSION_TOWARDS_ENTITY_TYPES_INFRA).build(),
            ApiEntityType.SERVICE.typeNumber(), ImmutableSet.<Integer>builder()
                    .addAll(EXPANSION_TOWARDS_ENTITY_TYPES_NON_INFRA).addAll(EXPANSION_TOWARDS_ENTITY_TYPES_INFRA).build());

    /**
     * Private constructor.
     */
    private InvolvedEntityExpansionUtil() {
    }

    /**
     * Check if the given entity type requires expansion.
     *
     * @param entityType the entity type.
     * @return true if this is an entity type that requires expansion.
     */
    public static boolean expansionRequiredEntityType(int entityType) {
        return ENTITY_WITH_EXPAND_ENTITY_SET_MAP.containsKey(entityType);
    }
}
