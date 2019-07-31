package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.runner.cost.MarketPriceTable;

/**
 * A factory for creating WastedFilesAnalysis instances.
 */
public interface WastedFilesAnalysisFactory {

    /**
     * Create a new {@link WastedFilesAnalysis}.
     *
     * @param topologyInfo           Information about the topology this analysis applies to.
     * @param topologyEntities       The entities in the topology.
     * @param clock                  A {@link Clock} to calculate start and end times of analysis.
     * @param topologyCostCalculator {@link TopologyCostCalculator} for calculating cost of cloud
     *                               volumes.
     * @param marketPriceTable       {@link MarketPriceTable} for calculating potential savings from
     *                               deleting cloud volumes.
     * @return The {@link WastedFilesAnalysis} object.
     */
    @Nonnull
    WastedFilesAnalysis newWastedFilesAnalysis(@Nonnull final TopologyInfo topologyInfo,
                                               @Nonnull final Map<Long, TopologyEntityDTO> topologyEntities,
                                               @Nonnull final Clock clock,
                                               @Nonnull final TopologyCostCalculator topologyCostCalculator,
                                               @Nonnull final MarketPriceTable marketPriceTable);

    /**
     * The default implementation of {@link AnalysisFactory}.
     */
    class DefaultWastedFilesAnalysisFactory implements WastedFilesAnalysisFactory {
        @Nonnull
        @Override
        public WastedFilesAnalysis newWastedFilesAnalysis(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull final Clock clock,
            @Nonnull final TopologyCostCalculator topologyCostCalculator,
            @Nonnull final MarketPriceTable marketPriceTable) {
            return new WastedFilesAnalysis(topologyInfo, topologyEntities, clock,
                topologyCostCalculator, marketPriceTable);
        }
    }
}
