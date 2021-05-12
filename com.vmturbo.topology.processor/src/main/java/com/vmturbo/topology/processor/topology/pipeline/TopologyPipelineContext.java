package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
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

    private static final String PIPELINE_STAGE_LABEL = "stage";

    private static final DataMetricSummary TOPOLOGY_STAGE_SUMMARY = DataMetricSummary.builder()
        .withName("tp_broadcast_pipeline_duration_seconds")
        .withHelp("Duration of the individual stages in a topology broadcast.")
        .withLabelNames(TopologyPipeline.TOPOLOGY_TYPE_LABEL, PIPELINE_STAGE_LABEL)
        .build()
        .register();

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

    @Nonnull
    @Override
    public String getPipelineName() {
        return FormattedString.format("Topology Pipeline (context : {}, id : {})",
            topologyInfoBuilder.getTopologyContextId(), topologyInfoBuilder.getTopologyId());
    }

    @Override
    public DataMetricTimer startStageTimer(String stageName) {
        return TOPOLOGY_STAGE_SUMMARY.labels(getTopologyTypeName(), stageName).startTimer();
    }

    @Override
    public TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }
}
