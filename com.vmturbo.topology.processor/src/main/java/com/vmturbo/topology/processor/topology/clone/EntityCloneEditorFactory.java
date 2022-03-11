package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * A factory to create topology entity clone function.
 */
public class EntityCloneEditorFactory {

    private final IdentityProvider identityProvider;
    private final Map<CloneType, DefaultEntityCloneEditor> entityCloneFunctionMap = new HashMap<>();

    /**
     * Types of entity clone function.
     */
    private enum CloneType {
        Default,
        VirtualMachine,
        ContainerPod
    }

    /**
     * Map entity type to its corresponding clone type.
     */
    private static final Map<Integer, CloneType> entityTypeToCloneType = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE_VALUE, CloneType.VirtualMachine,
            EntityType.CONTAINER_POD_VALUE, CloneType.ContainerPod);

    /**
     * Construct an instance of {@link EntityCloneEditorFactory}.
     *
     * @param identityProvider the identity provider used to generate entity id
     */
    public EntityCloneEditorFactory(@Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = identityProvider;
    }

    /**
     * Creates and caches topology entity clone function.
     *
     * @param entity the builder of the source topology entity
     * @param topologyInfo the topology info associated with this clone function
     * @param topology the topology map
     * @param scope the plan scope
     *
     * @return the topology entity clone function for the entity
     */
    public DefaultEntityCloneEditor createEntityCloneFunction(
            @Nonnull final TopologyEntity.Builder entity,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nullable final PlanScope scope) {
        return entityCloneFunctionMap.computeIfAbsent(
                entityTypeToCloneType.getOrDefault(entity.getEntityType(), CloneType.Default),
                cloneType -> {
                    switch (cloneType) {
                        case VirtualMachine:
                            return new VirtualMachineCloneEditor(
                                    topologyInfo, identityProvider, topology, scope);
                        case ContainerPod:
                            return new ContainerPodCloneEditor(
                                    topologyInfo, identityProvider, topology, scope);
                        default:
                            return new DefaultEntityCloneEditor(
                                    topologyInfo, identityProvider, topology, scope);
                    }
                });
    }
}
