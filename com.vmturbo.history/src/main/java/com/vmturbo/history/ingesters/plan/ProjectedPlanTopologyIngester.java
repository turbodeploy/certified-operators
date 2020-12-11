package com.vmturbo.history.ingesters.plan;

import java.util.Collection;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;

/**
 * Ingester to process projected plan topologies broadcast by market.
 */
public class ProjectedPlanTopologyIngester extends TopologyIngesterBase<ProjectedTopologyEntity> {

    private static final TopologyType TOPOLOGY_TYPE = TopologyType.PROJECTED_PLAN;

    /**
     * Create a new ingester.
     *
     * @param chunkProcessorFactories factories to create participating writers
     * @param topologyIngesterConfig  ingester config
     * @param loaderFactorySupplier   supplier of bulk loader factories
     */
    public ProjectedPlanTopologyIngester(
            @Nonnull final Collection<? extends IChunkProcessorFactory<ProjectedTopologyEntity,
                    TopologyInfo, SimpleBulkLoaderFactory>> chunkProcessorFactories,
            @Nonnull final TopologyIngesterConfig topologyIngesterConfig,
            @Nonnull final Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier) {
        super(chunkProcessorFactories, topologyIngesterConfig, loaderFactorySupplier,
                TOPOLOGY_TYPE);
    }
}
