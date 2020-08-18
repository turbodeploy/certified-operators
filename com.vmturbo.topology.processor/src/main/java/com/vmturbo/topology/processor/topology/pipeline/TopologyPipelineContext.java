package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.PipelineContext;
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
public class TopologyPipelineContext implements PipelineContext {
    private final GroupResolver groupResolver;

    private final TopologyInfo.Builder topologyInfoBuilder;

    private final StitchingJournalContainer stitchingJournalContainer;
    private final ConsistentScalingManager consistentScalingManager;

    public TopologyPipelineContext(@Nonnull final GroupResolver groupResolver,
                                   @Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final ConsistentScalingManager consistentScalingManager) {
        this.groupResolver = Objects.requireNonNull(groupResolver);
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
        this.stitchingJournalContainer = new StitchingJournalContainer();
        this.consistentScalingManager = consistentScalingManager;
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

    @NotNull
    @Override
    public String getPipelineName() {
        return FormattedString.format("Topology Pipeline (context : {}, id : {})",
            topologyInfoBuilder.getTopologyContextId(), topologyInfoBuilder.getTopologyId());
    }
}
