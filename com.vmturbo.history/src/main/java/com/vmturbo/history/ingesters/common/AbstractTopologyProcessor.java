package com.vmturbo.history.ingesters.common;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

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

    private final String topologyType;

    /**
     * Create a new instance.
     *
     * @param chunkProcessorFactories factories to create chunk processors
     * @param config                  config values
     * @param topologyType            a string identifying the type of this topology, for
     *                                inclusion in the info summary
     */
    public AbstractTopologyProcessor(
            @Nonnull final Collection<? extends IChunkProcessorFactory<T, TopologyInfo, StateT>>
                    chunkProcessorFactories, TopologyIngesterConfig config, String topologyType) {
        super(chunkProcessorFactories, config);
        this.topologyType = topologyType;
    }

    @Override
    protected String summarizeInfo(final TopologyInfo topologyInfo) {
        return getTopologyInfoSummary(topologyInfo, topologyType);
    }

    private static DateTimeFormatter hmsFormat = DateTimeFormatter
        .ofPattern("HH:mm:ss")
        .withZone(ZoneOffset.UTC);

    /**
     * Summarize a topology given its {@link TopologyInfo} and a type string.
     *
     * <p>This is primarily used for logging, and is made static so it can be used from other
     * classes.</p>
     *
     * @param topologyInfo topology info
     * @param topologyType topology type
     * @return summary string
     */
    public static String getTopologyInfoSummary(TopologyInfo topologyInfo, String topologyType) {
        final String hms = hmsFormat.format(Instant.ofEpochMilli(topologyInfo.getCreationTime()));
        return String.format("%s Topology @%s[id: %s; ctx: %s]",
            topologyType, hms, topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());
    }
}
