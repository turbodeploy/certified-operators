package com.vmturbo.market.runner.wasted;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;

/**
 * Engine for identifying entities that are no longer needed.
 */
public interface WastedEntityAnalysisEngine {

    /**
     * Perform the analysis on entities and generate delete actions for those
     * that can be deleted since they provide no value to customers.
     *
     * @param topologyInfo Information about the topology this analysis applies to.
     * @param topologyEntities The entities in the topology.
     * @param topologyCostCalculator {@link TopologyCostCalculator} for calculating cost of entities
     * @param originalCloudTopology {@link CloudTopology} for calculating potential savings from deleting entities
     * @return The {@link WastedEntityResults} object.
     */
    @Nonnull
    WastedEntityResults analyze(@Nonnull TopologyInfo topologyInfo,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull TopologyCostCalculator topologyCostCalculator,
            @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology);
}
