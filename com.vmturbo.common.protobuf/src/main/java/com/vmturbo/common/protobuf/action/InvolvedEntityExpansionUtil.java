package com.vmturbo.common.protobuf.action;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utils to support expansion over involved entities.
 */
public class InvolvedEntityExpansionUtil {

    /**
     * These are the entities that need to be included while expanding BusinessApp, BusinessTxn,
     * Service, Namespace and ContainerPlatformCluster.
     * <pre>
     * BApp -> BTxn -> Service -> AppComp -> Node
     *                                       VirtualMachine  --> VDC ----> Host  --------
     *                       DatabaseServer   \    \   \             ^                \
     *                                          \    \   \___________/                v
     *                                           \    -----> Volume   ------------->  Storage
     *                                            \                                   ^
     *                                             ----------------------------------/
     * </pre>
     */
    public static final List<Integer> EXPANSION_TOWARDS_ENTITY_TYPES = Arrays.asList(
            ApiEntityType.APPLICATION_COMPONENT.typeNumber(),
            ApiEntityType.CONTAINER.typeNumber(),
            ApiEntityType.CONTAINER_POD.typeNumber(),
            ApiEntityType.CONTAINER_SPEC.typeNumber(),
            ApiEntityType.WORKLOAD_CONTROLLER.typeNumber(),
            ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
            ApiEntityType.DATABASE_SERVER.typeNumber(),
            ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
            ApiEntityType.STORAGE.typeNumber(),
            ApiEntityType.PHYSICAL_MACHINE.typeNumber());

    /**
     * Entity types which are counted as entities that desire expansion.
     */
    public static final Set<Integer> EXPANSION_REQUIRED_ENTITY_TYPES = ImmutableSet.of(
            ApiEntityType.NAMESPACE.typeNumber(),
            ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber(),
            ApiEntityType.BUSINESS_APPLICATION.typeNumber(),
            ApiEntityType.BUSINESS_TRANSACTION.typeNumber(),
            ApiEntityType.SERVICE.typeNumber());

    /**
     * Entity types in supply chain that either require expansion or are involved in the expansion.
     */
    public static final Set<Integer> EXPANSION_ALL_ENTITY_TYPES = ImmutableSet.<Integer>builder()
            .addAll(EXPANSION_TOWARDS_ENTITY_TYPES)
            .addAll(EXPANSION_REQUIRED_ENTITY_TYPES)
            .build();

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
        return EXPANSION_REQUIRED_ENTITY_TYPES.contains(entityType);
    }
}
