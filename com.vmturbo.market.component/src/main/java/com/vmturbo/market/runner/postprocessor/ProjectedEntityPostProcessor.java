package com.vmturbo.market.runner.postprocessor;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;

/**
 * Post processor to update corresponding projected topology entities.
 */
public abstract class ProjectedEntityPostProcessor {

    /**
     * Whether a projected entity post processor should be applied to given projected topology entities.
     *
     * @param topologyInfo                  Given {@link TopologyInfo}
     * @param entityTypeToProjectedEntities Map of entity types to list of {@link ProjectedTopologyEntity}.
     * @return True if {@link ProjectedEntityPostProcessor} should be applied.
     */
    public abstract boolean appliesTo(@Nonnull TopologyInfo topologyInfo,
                                      @Nonnull Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities);

    /**
     * Post process a type of projected topology entities based on other types of projected entities
     * or actions generated from market analysis.
     *
     * @param topologyInfo                  Given {@link TopologyInfo}.
     * @param projectedEntities             Map of entity OID to {@link ProjectedTopologyEntity}.
     * @param entityTypeToProjectedEntities Map of entity type to list of {@link ProjectedTopologyEntity}.
     * @param actionsList                   List of actions generated from market analysis.
     */
    public abstract void process(@Nonnull TopologyInfo topologyInfo,
                                 @Nonnull Map<Long, ProjectedTopologyEntity> projectedEntities,
                                 @Nonnull Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities,
                                 @Nonnull List<ActionTO> actionsList);
}
