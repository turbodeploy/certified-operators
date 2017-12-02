package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfoOrBuilder;
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
public class TopologyPipelineContext {
    private final GroupResolver groupResolver;

    private final TopologyInfo.Builder topologyInfoBuilder;

    public TopologyPipelineContext(@Nonnull final GroupResolver groupResolver,
                                   @Nonnull final TopologyInfo topologyInfo) {
        this.groupResolver = Objects.requireNonNull(groupResolver);
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
    }

    @Nonnull
    public GroupResolver getGroupResolver() {
        return groupResolver;
    }

    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return topologyInfoBuilder.buildPartial();
    }

    @Nonnull
    public void editTopologyInfo(Consumer<Builder> editFunction) {
        editFunction.accept(topologyInfoBuilder);
    }

    public String getTopologyTypeName() {
        return topologyInfoBuilder.getTopologyType().name();
    }
}
