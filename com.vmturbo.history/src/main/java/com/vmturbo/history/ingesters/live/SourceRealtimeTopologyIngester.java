package com.vmturbo.history.ingesters.live;

import java.util.Collection;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;

/**
 * Ingester for live topologies broadcast by topology processor.
 */
public class SourceRealtimeTopologyIngester extends TopologyIngesterBase<Topology.DataSegment> {

    private static final TopologyType TOPOLOGY_TYPE = TopologyType.LIVE;

    /**
     * Create a new instance.
     *
     * @param chunkProcessorFactories factories for participating writers
     * @param topologyIngesterConfig  ingester config
     * @param loaderFactorySupplier   supplier of new bulk loader factories
     */
    public SourceRealtimeTopologyIngester(
        @Nonnull final Collection<IChunkProcessorFactory
            <Topology.DataSegment, TopologyInfo, SimpleBulkLoaderFactory>>
            chunkProcessorFactories,
        @Nonnull final TopologyIngesterConfig topologyIngesterConfig,
        @Nonnull final Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier) {
        super(chunkProcessorFactories, topologyIngesterConfig, loaderFactorySupplier,
            TOPOLOGY_TYPE);
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
                .filter(DataSegment::hasEntity)
                .count();
        return (int)count;
    }
}
