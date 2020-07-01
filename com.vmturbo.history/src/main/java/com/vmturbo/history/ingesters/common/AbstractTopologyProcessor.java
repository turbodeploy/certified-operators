package com.vmturbo.history.ingesters.common;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * An {@link AbstractChunkedBroadcastProcessor} specialized for processing topologies.
 *
 * <p>{@link TopologyInfo} is used for the broadcast info type.</p>
 *
 * @param <T>       topology entity type
 * @param <StateT>  shared state type
 * @param <ResultT> processing result type
 */
public abstract class AbstractTopologyProcessor<T, StateT, ResultT>
        extends AbstractChunkedBroadcastProcessor<T, TopologyInfo, StateT, ResultT> {

    private final boolean projectedTopology;

    /**
     * Create a new instance.
     *
     * @param chunkProcessorFactories factories to create chunk processors
     * @param config                  config values
     * @param projectedTopology       Whether or not this processor is for a projected topology.
     */
    public AbstractTopologyProcessor(
        @Nonnull final Collection<? extends IChunkProcessorFactory<T, TopologyInfo, StateT>>
                    chunkProcessorFactories, TopologyIngesterConfig config,
            boolean projectedTopology) {
        super(chunkProcessorFactories, config);
        this.projectedTopology = projectedTopology;
    }

    @Override
    protected String summarizeInfo(final TopologyInfo topologyInfo) {
        return projectedTopology ? TopologyDTOUtil.getProjectedTopologyLabel(topologyInfo)
            : TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
    }
}
