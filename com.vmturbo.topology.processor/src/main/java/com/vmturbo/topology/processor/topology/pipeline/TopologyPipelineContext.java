package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.topology.processor.group.GroupResolver;

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
public class TopologyPipelineContext extends PipelineContext {
    private final TopologyInfo.Builder topologyInfoBuilder;

    /**
     * Create a new {@link TopologyPipelineContext}.
     *
     * @param topologyInfo {@link TopologyInfo} describing the topology the pipeline is being run for.
     */
    public TopologyPipelineContext(@Nonnull final TopologyInfo topologyInfo) {
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
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

    @NotNull
    @Override
    public String getPipelineName() {
        return FormattedString.format("Topology Pipeline (context : {}, id : {})",
            topologyInfoBuilder.getTopologyContextId(), topologyInfoBuilder.getTopologyId());
    }
}
