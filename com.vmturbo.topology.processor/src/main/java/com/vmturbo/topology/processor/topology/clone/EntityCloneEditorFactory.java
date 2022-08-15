package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;

import com.google.common.collect.MapMaker;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A factory to create topology entity clone function.
 */
public final class EntityCloneEditorFactory {
    /**
     * A cache for entity clone editor based on clone type.
     */
    private static final Map<Integer, DefaultEntityCloneEditor> entityCloneFunctionMap =
            new MapMaker().weakValues().makeMap();

    private EntityCloneEditorFactory() {}

    /**
     * Creates and caches topology entity clone function.
     *
     * @param entityType the type of the entity to clone
     * @return the topology entity clone function for the entity
     */
    public static DefaultEntityCloneEditor createEntityCloneFunction(final int entityType) {
        return entityCloneFunctionMap.computeIfAbsent(entityType, cloneType -> {
            switch (cloneType) {
                case EntityType.VIRTUAL_MACHINE_VALUE:
                    return new VirtualMachineCloneEditor();
                case EntityType.CONTAINER_POD_VALUE:
                    return new ContainerPodCloneEditor();
                case EntityType.CONTAINER_VALUE:
                    return new ContainerCloneEditor();
                case EntityType.WORKLOAD_CONTROLLER_VALUE:
                    return new WorkloadControllerCloneEditor();
                case EntityType.NAMESPACE_VALUE:
                    return new NamespaceCloneEditor();
                case EntityType.PHYSICAL_MACHINE_VALUE:
                case EntityType.STORAGE_VALUE:
                    return new DSPMCloneEditor();
                case EntityType.CONTAINER_SPEC_VALUE:
                default:
                    return new DefaultEntityCloneEditor();
            }
        });
    }
}
