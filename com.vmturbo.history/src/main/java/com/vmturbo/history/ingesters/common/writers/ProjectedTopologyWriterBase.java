package com.vmturbo.history.ingesters.common.writers;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;

/**
 * Base class for projected topology writers.
 */
public abstract class ProjectedTopologyWriterBase
    implements IChunkProcessor<ProjectedTopologyEntity> {

    /**
     * Create a new writer instance.
     */
    public abstract static class Factory implements
            IChunkProcessorFactory<ProjectedTopologyEntity, TopologyInfo, SimpleBulkLoaderFactory> {
    }
}
