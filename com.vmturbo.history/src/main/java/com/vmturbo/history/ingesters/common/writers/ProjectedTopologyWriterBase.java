package com.vmturbo.history.ingesters.common.writers;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;

/**
 * Base class for projected topology writers.
 */
public abstract class ProjectedTopologyWriterBase
    implements IChunkProcessor<ProjectedTopologyEntity> {
    private final Predicate<TopologyDTO.TopologyEntityDTO> entitiesFilter;

    /**
     * Default constructor.
     */
    public ProjectedTopologyWriterBase() {
        this(e -> true);
    }

    /**
     * Constructor with entities filter.
     *
     * @param entitiesFilter entities filter
     */
    public ProjectedTopologyWriterBase(@Nonnull Predicate<TopologyDTO.TopologyEntityDTO> entitiesFilter) {
        this.entitiesFilter = entitiesFilter;
    }

    @Override
    public ChunkDisposition processChunk(@Nonnull Collection<ProjectedTopologyEntity> chunk,
                                         @Nonnull String infoSummary) throws InterruptedException {
        final List<ProjectedTopologyEntity> filteredChunk = chunk.stream()
                .filter(ProjectedTopologyEntity::hasEntity)
                .filter(pe -> entitiesFilter.test(pe.getEntity()))
                .collect(Collectors.toList());
        return processEntities(filteredChunk, infoSummary);
    }

    /**
     * Process the filtered entities from a raw topology chunk.
     *
     * @param chunk       a filtered topology chunk
     * @param infoSummary summary of topology
     * @return disposition indicating how to proceed with rest of topology
     * @throws InterruptedException if interrupted
     */
    protected abstract ChunkDisposition processEntities(@Nonnull Collection<ProjectedTopologyEntity> chunk,
                                                        @Nonnull String infoSummary) throws InterruptedException;

    /**
     * Create a new writer instance.
     */
    public abstract static class Factory implements
            IChunkProcessorFactory<ProjectedTopologyEntity, TopologyInfo, IngesterState> {
    }
}
