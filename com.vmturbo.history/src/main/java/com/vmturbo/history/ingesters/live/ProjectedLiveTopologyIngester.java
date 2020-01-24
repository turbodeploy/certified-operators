package com.vmturbo.history.ingesters.live;

import java.util.Collection;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;
import com.vmturbo.history.ingesters.common.writers.ProjectedTopologyWriterBase.Factory;

/**
 * Ingester for projected live topologies, broadcast by market.
 */
public class ProjectedLiveTopologyIngester extends TopologyIngesterBase<ProjectedTopologyEntity> {

    private static final TopologyType TOPOLOGY_TYPE = TopologyType.PROJECTED_LIVE;

    /**
     * Create a new ingester instance.
     *
     * @param chunkProcessorFactories factories for participating writers
     * @param topologyIngesterConfig  ingester config
     * @param loaderFactorySupplier   supplier of a bulk loader factories
     */
    public ProjectedLiveTopologyIngester(
            @Nonnull final Collection<Factory> chunkProcessorFactories,
            @Nonnull final TopologyIngesterConfig topologyIngesterConfig,
            @Nonnull Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier) {
        super(chunkProcessorFactories, topologyIngesterConfig, loaderFactorySupplier,
                TOPOLOGY_TYPE);
    }

    /**
     * Summarize the topology being ingested.
     *
     * @param topologyInfo topology info object
     * @return topology summary string
     */
    public static String getTopologyInfoSummary(TopologyInfo topologyInfo) {
        return getTopologyInfoSummary(topologyInfo, TOPOLOGY_TYPE.getReadableName());
    }
}
