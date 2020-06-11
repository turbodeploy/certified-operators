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
     * For plans like MPC, this is the set of entities that are selected for migration to a
     * target CSP. This is used by various stages of the plan pipeline, thus stored here.
     */
    private final Set<Long> sourceEntityOids;

    public TopologyPipelineContext(@Nonnull final GroupResolver groupResolver,
                                   @Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final ConsistentScalingManager consistentScalingManager) {
        this.groupResolver = Objects.requireNonNull(groupResolver);
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
        this.stitchingJournalContainer = new StitchingJournalContainer();
        this.consistentScalingManager = consistentScalingManager;
        sourceEntityOids = new HashSet<>();
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
     * Sets the set of source entity oids in this context.
     *
     * @param sourceOids Set of source entity oids.
     */
    public void setSourceEntityOids(@Nonnull final Collection<Long> sourceOids) {
        sourceEntityOids.clear();
        sourceEntityOids.addAll(sourceOids);
    }

    /**
     * Gets the set of source entity oids.
     *
     * @return Set of source entity oids.
     */
    @Nonnull
    public Set<Long> getSourceEntityOids() {
        return Collections.unmodifiableSet(sourceEntityOids);
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
