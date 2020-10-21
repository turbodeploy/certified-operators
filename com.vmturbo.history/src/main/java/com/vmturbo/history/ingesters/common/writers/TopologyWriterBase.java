package com.vmturbo.history.ingesters.common.writers;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;

/**
 * Base class for topology writers.
 */
public abstract class TopologyWriterBase implements IChunkProcessor<Topology.DataSegment> {
    private final Predicate<TopologyEntityDTO> entitiesFilter;

    /**
     * Default constructor.
     */
    public TopologyWriterBase() {
        this(e -> true);
    }

    /**
     * Constructor with entities filter.
     *
     * @param entitiesFilter entities filter
     */
    public TopologyWriterBase(Predicate<TopologyEntityDTO> entitiesFilter) {
        this.entitiesFilter = entitiesFilter;
    }

    @Override
    public ChunkDisposition processChunk(@Nonnull final Collection<Topology.DataSegment> chunk,
            @Nonnull final String infoSummary) throws InterruptedException {
        final List<TopologyEntityDTO> filteredChunk = chunk.stream()
                .filter(DataSegment::hasEntity)
                .map(DataSegment::getEntity)
                .filter(entitiesFilter)
                .collect(Collectors.toList());
        return processEntities(filteredChunk, infoSummary);
    }

    /**
     * Process the entities from a raw topology chunk, skipping the topology extension items.
     *
     * @param chunk       a raw chunk from the topology, which may contain a mixture of extension
     *                    and entity items.
     * @param infoSummary summary of topology
     * @return disposition indicating how to proceed with rest of topology
     * @throws InterruptedException if interrupted
     */
    protected abstract ChunkDisposition processEntities(@Nonnull Collection<TopologyEntityDTO> chunk,
            @Nonnull String infoSummary) throws InterruptedException;

    /**
     * Create a new writer instance.
     */
    public abstract static class Factory implements
            IChunkProcessorFactory<Topology.DataSegment, TopologyInfo, SimpleBulkLoaderFactory> {
    }
}
