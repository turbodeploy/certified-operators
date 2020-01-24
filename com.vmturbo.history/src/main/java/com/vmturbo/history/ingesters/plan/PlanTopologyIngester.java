package com.vmturbo.history.ingesters.plan;

import java.util.Collection;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;

/**
 * Ingester to process plan topologies broadcast by market component.
 */
public class PlanTopologyIngester extends TopologyIngesterBase<Topology.DataSegment> {

    private static final TopologyType TOPOLOGY_TYPE = TopologyType.PLAN;

    /**
     * Create a new ingester.
     *
     * @param iChunkProcessorFactories factories for participating writers
     * @param topologyIngesterConfig   ingester config
     * @param loaderFactorySupplier    supplier of bulk loader factories
     */
    public PlanTopologyIngester(
            @Nonnull final Collection<? extends IChunkProcessorFactory
                    <Topology.DataSegment, TopologyInfo, SimpleBulkLoaderFactory>>
                    iChunkProcessorFactories,
                @Nonnull final TopologyIngesterConfig topologyIngesterConfig,
            @Nonnull final Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier) {
        super(iChunkProcessorFactories, topologyIngesterConfig, loaderFactorySupplier,
                TOPOLOGY_TYPE);
    }

    /**
     * Create a summary string for this topology for use in logs.
     *
     * @param topologyInfo topology info
     * @return summary string
     */
    public static String getTopologyInfoSummary(TopologyInfo topologyInfo) {
        return getTopologyInfoSummary(topologyInfo, TOPOLOGY_TYPE.getReadableName());
    }

    /**
     * We only want to count the entities appearing in a chunk, not extension data.
     *
     * @param chunk chunk to be counted
     * @return number of entities in the chunk
     */
    @Override
    protected int getChunkObjectCount(final Collection<Topology.DataSegment> chunk) {
        final long count = chunk.stream()
                .filter(item -> item.hasEntity())
                .count();
        return (int)count;
    }
}
