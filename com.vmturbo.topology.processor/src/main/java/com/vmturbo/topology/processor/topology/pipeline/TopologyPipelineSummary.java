package com.vmturbo.topology.processor.topology.pipeline;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.components.common.pipeline.PipelineSummary;

/**
 * The summary of {@link TopologyPipeline}, intended to be a quick way to visualize what went
 * right/wrong in a pipeline execution.
 */
@ThreadSafe
public class TopologyPipelineSummary extends PipelineSummary {

    /**
     * The context of the {@link TopologyPipeline} this {@link TopologyPipelineSummary} describes.
     */
    private final TopologyPipelineContext context;

    TopologyPipelineSummary(@Nonnull final Clock clock,
                            @Nonnull final TopologyPipelineContext context,
                            @Nonnull final List<Pipeline.Stage> stages) {
        super(clock, stages);
        this.context = Objects.requireNonNull(context);
    }

    @Override
    protected String getPreamble() {
        final TopologyInfo topoInfo = context.getTopologyInfo();
        return FormattedString.format("Context ID: {}\n"
            + "Topology ID: {}\n"
            + "Topology Type: {}", topoInfo.getTopologyContextId(),
            topoInfo.getTopologyId(), context.getTopologyTypeName());
    }
}
