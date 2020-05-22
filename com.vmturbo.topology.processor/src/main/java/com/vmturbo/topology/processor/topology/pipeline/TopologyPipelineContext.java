package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;

/**
 * The {@link TopologyPipelineContext} is information that's shared by all stages
 * in a pipeline.
 * <p>
 * This is the place to put generic info that applies to many stages, as well as utility objects
 * that have some state (e.g. caches like {@link GroupResolver}) which are shared across
 * the stages.
 * <p>
 * The context is immutable, but objects inside it may be mutable.
 */
public class TopologyPipelineContext {
    private final GroupResolver groupResolver;

    private final TopologyInfo.Builder topologyInfoBuilder;

    private final StitchingJournalContainer stitchingJournalContainer;
    private final ConsistentScalingManager consistentScalingManager;

    /**
     * Some plans (like MCP) clone source entity oids. The oids of these clones are stored here,
     * so that some of the rest of plan pipeline stages can use it.
     */
    private final Set<Long> cloneEntityOids;

    public TopologyPipelineContext(@Nonnull final GroupResolver groupResolver,
                                   @Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final ConsistentScalingManager consistentScalingManager) {
        this.groupResolver = Objects.requireNonNull(groupResolver);
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
        this.stitchingJournalContainer = new StitchingJournalContainer();
        this.consistentScalingManager = consistentScalingManager;
        cloneEntityOids = new HashSet<>();
    }

    @Nonnull
    public GroupResolver getGroupResolver() {
        return groupResolver;
    }

    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return topologyInfoBuilder.buildPartial();
    }

    public void editTopologyInfo(Consumer<Builder> editFunction) {
        editFunction.accept(topologyInfoBuilder);
    }

    /**
     * Sets the set of clone entity oids in this context.
     *
     * @param cloneOids Set of clone entity oids.
     */
    public void setCloneEntityOids(@Nonnull final Collection<Long> cloneOids) {
        cloneEntityOids.clear();
        cloneEntityOids.addAll(cloneOids);
    }

    /**
     * Gets the set of clone entity oids.
     *
     * @return Set of clone entity oids.
     */
    @Nonnull
    public Set<Long> getCloneEntityOids() {
        return Collections.unmodifiableSet(cloneEntityOids);
    }

    public String getTopologyTypeName() {
        return topologyInfoBuilder.getTopologyType().name();
    }

    @Nonnull
    public StitchingJournalContainer getStitchingJournalContainer() {
        return stitchingJournalContainer;
    }

    @Nonnull
    public ConsistentScalingManager getConsistentScalingManager() {
        return consistentScalingManager;
    }
}
