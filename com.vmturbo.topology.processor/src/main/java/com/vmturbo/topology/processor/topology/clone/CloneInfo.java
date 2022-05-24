package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * A {@link CloneInfo} object that stores the new information for a specific clone.
 */
public class CloneInfo {
    private long cloneCounter;
    @Nullable private final Integer changeReplicas;
    @Nullable private final TopologyEntity.Builder sourceCluster;

    private CloneInfo(@Nullable final Integer changeReplicas,
                      @Nullable final TopologyEntity.Builder sourceCluster) {
        this.changeReplicas = changeReplicas;
        this.sourceCluster = sourceCluster;
    }

    /**
     * Create a clone info object.
     *
     * @param entityBuilder the entity to be cloned
     * @param topologyGraph the topology graph
     * @param entitiesWithReplicas the change replicas map
     * @param cloneContext the clone context
     * @return the info that contains changes to this clone
     */
    public static CloneInfo createCloneInfo(@Nonnull TopologyEntity.Builder entityBuilder,
                                            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                            @Nonnull final Map<Long, Integer> entitiesWithReplicas,
                                            @Nonnull final CloneContext cloneContext) {
        // Locate the source cluster for entity
        final TopologyEntity.Builder sourceCluster = cloneContext.isMigrateContainerWorkloadPlan()
                ? TopologyEditorUtil.traverseSupplyChain(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE,
                                                         entityBuilder, topologyGraph, false)
                .stream()
                .findFirst()
                .orElse(null)
                : null;
        if (sourceCluster != null) {
            // Update cpuScalingFactor for entities in source cluster
            topologyGraph.getEntity(sourceCluster.getOid()).ifPresent(cloneContext::updateCPUScalingFactor);
        }
        return new CloneInfo(entitiesWithReplicas.get(entityBuilder.getOid()), sourceCluster);
    }

    /**
     * Associate a clone counter with the clone info.
     *
     * @param cloneCounter the clone counter of this clone
     * @return the current clone info
     */
    @Nonnull
    public CloneInfo withCloneCounter(final long cloneCounter) {
        this.cloneCounter = cloneCounter;
        return this;
    }

    /**
     * Get the clone counter associated with the clone info.
     *
     * @return the clone counter associated with the clone info
     */
    long getCloneCounter() {
        return cloneCounter;
    }

    /**
     * Get the changed replicas.
     *
     * @return an optional of changed replicas
     */
    @Nonnull
    Optional<Integer> getChangeReplicas() {
        return Optional.ofNullable(changeReplicas);
    }

    /**
     * Get the source cluster.
     *
     * @return an optional of source cluster
     */
    @Nonnull
    Optional<TopologyEntity.Builder> getSourceCluster() {
        return Optional.ofNullable(sourceCluster);
    }
}
