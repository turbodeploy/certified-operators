package com.vmturbo.repository.plan.db;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseTopology;

/**
 * Implementation of {@link BaseTopology} for plan supply chain calculation.
 */
public class RepoPlanTopology extends BaseTopology<RepoPlanGraphEntity> {

    private static final DataMetricSummary CONSTRUCTION_TIME_SUMMARY = DataMetricSummary.builder()
            .withName("repo_plan_topology_construction_seconds")
            .withHelp("Total time taken to build a plan repo topology.")
            .build()
            .register();

    protected RepoPlanTopology(@Nonnull final TopologyInfo topologyInfo,
                               @Nonnull final TopologyGraph<RepoPlanGraphEntity> entityGraph) {
        super(topologyInfo, entityGraph);
    }

    /**
     * Builder for the {@link RepoPlanTopology}.
     */
    public static class RepoPlanTopologyBuilder extends BaseTopologyBuilder<RepoPlanTopology, RepoPlanGraphEntity, RepoPlanGraphEntity.Builder> {

        RepoPlanTopologyBuilder(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final Consumer<RepoPlanTopology> onFinish) {
            super(CONSTRUCTION_TIME_SUMMARY, topologyInfo, onFinish);
        }

        @Override
        protected RepoPlanGraphEntity.Builder newBuilder(TopologyEntityDTO entity) {
            return new RepoPlanGraphEntity.Builder(entity);
        }

        @Override
        protected RepoPlanTopology newTopology(TopologyInfo topologyInfo,
                TopologyGraph<RepoPlanGraphEntity> graph) {
            return new RepoPlanTopology(topologyInfo, graph);
        }
    }
}
