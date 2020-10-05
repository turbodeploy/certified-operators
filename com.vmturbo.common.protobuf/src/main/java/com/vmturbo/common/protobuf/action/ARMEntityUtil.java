package com.vmturbo.common.protobuf.action;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utils for ARM entities.
 */
public class ARMEntityUtil {

    /**
     * These are the entities that need to be retrieved underneath BusinessApp, BusinessTxn, and
     * Service.
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
    public static final List<Integer> PROPAGATED_ARM_ENTITY_TYPES = Arrays.asList(
            ApiEntityType.APPLICATION_COMPONENT.typeNumber(),
            ApiEntityType.WORKLOAD_CONTROLLER.typeNumber(),
            ApiEntityType.CONTAINER_POD.typeNumber(),
            ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
            ApiEntityType.DATABASE_SERVER.typeNumber(),
            ApiEntityType.VIRTUAL_VOLUME.typeNumber(),
            ApiEntityType.STORAGE.typeNumber(),
            ApiEntityType.PHYSICAL_MACHINE.typeNumber());

    /**
     * Entity types which are counted as ARM entities.
     */
    public static final Set<Integer> ARM_ENTITY_TYPE = ImmutableSet.of(
            ApiEntityType.BUSINESS_APPLICATION.typeNumber(),
            ApiEntityType.BUSINESS_TRANSACTION.typeNumber(),
            ApiEntityType.SERVICE.typeNumber());

    /**
     * ARM entity types and those below them in supply chain.
     */
    public static final Set<Integer> ENTITY_TYPES_BELOW_ARM = ImmutableSet.<Integer>builder()
            .addAll(PROPAGATED_ARM_ENTITY_TYPES)
            .addAll(ARM_ENTITY_TYPE)
            .build();

    /**
     * Private constructor.
     */
    private ARMEntityUtil() {
    }

    /**
     * Check if the given entity type is an ARM entity type.
     *
     * @param entityType the entity type.
     * @return true if this is an ARM entity type.
     */
    public static boolean isARMEntityType(int entityType) {
        return ARM_ENTITY_TYPE.contains(entityType);
    }
}
