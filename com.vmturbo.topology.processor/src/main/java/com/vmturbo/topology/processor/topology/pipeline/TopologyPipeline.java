package com.vmturbo.topology.processor.topology.pipeline;

import java.time.Clock;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.components.common.tracing.TracingManager;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link TopologyPipeline} captures the different stages required to build and broadcast
 * a topology out of the topology processor. {@link TopologyPipeline}s are built in the
 * {@link LivePipelineFactory} for realtime pipelines, and {@link PlanPipelineFactory} for
 * plan pipelines.
 *
 * <p>The pipeline consists of a set of {@link Stage}s. The output of one stage becomes the input
 * to the next {@link Stage}. There is some (minimal) shared state between the stages, represented
 * by the {@link TopologyPipelineContext}. The stages are executed one at a time. In the future
 * we can add parallel execution of certain subsets of the pipeline, but that's not necessary
 * at the time of this writing (Nov 2017).
 *
 * @param <I> The input to the pipeline. This is the input to the first stage.
 * @param <O> The output of the pipeline. This is the output of the last stage.
 */
public class TopologyPipeline<I, O> extends
        Pipeline<I, O, TopologyPipelineContext, TopologyPipelineSummary> {

    private static final Logger logger = LogManager.getLogger();

    private static final String TOPOLOGY_TYPE_LABEL = "topology_type";

    private static final String PIPELINE_STAGE_LABEL = "stage";

    /**
     * This metric tracks the total duration of a topology broadcast (i.e. all the stages
     * in the pipeline).
     */
    private static final DataMetricSummary TOPOLOGY_BROADCAST_SUMMARY = DataMetricSummary.builder()
            .withName("tp_broadcast_duration_seconds")
            .withHelp("Duration of a topology broadcast.")
            .withLabelNames(TOPOLOGY_TYPE_LABEL)
            .build()
            .register();

    private static final DataMetricSummary TOPOLOGY_STAGE_SUMMARY = DataMetricSummary.builder()
            .withName("tp_broadcast_pipeline_duration_seconds")
            .withHelp("Duration of the individual stages in a topology broadcast.")
            .withLabelNames(TOPOLOGY_TYPE_LABEL, PIPELINE_STAGE_LABEL)
            .build()
            .register();

    /**
     * Create a new {@link TopologyPipeline}.
     *
     * @param stages The {@link com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition}.
     */
    public TopologyPipeline(@Nonnull final PipelineDefinition<I, O, TopologyPipelineContext> stages) {
        super(stages, new TopologyPipelineSummary(Clock.systemUTC(), stages.getContext(), stages.getStages()));
    }

    /**
     * Get the {@link TopologyInfo} describing the topology being constructed by this pipeline.
     * This returns the current version of the {@link TopologyInfo}. Some stages may edit the
     * {@link TopologyInfo}, and those changes may or may not be present in the snapshot depending
     * on whether the stage has run or not. To get the guaranteed "final" version of the
     * {@link TopologyInfo}, call this method only after {@link TopologyPipeline#run(Object)}.
     *
     * @return The {@link TopologyInfo} describing the topology constructed by this pipeline.
     */
    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return getContext().getTopologyInfo();
    }

    @Override
    protected DataMetricTimer startPipelineTimer() {
        return TOPOLOGY_BROADCAST_SUMMARY.labels(getContext().getTopologyTypeName()).startTimer();
    }

    @Override
    protected DataMetricTimer startStageTimer(String stageName) {
        return TOPOLOGY_STAGE_SUMMARY.labels(getContext().getTopologyTypeName(),
                stageName).startTimer();
    }

    @Override
    protected TracingScope startPipelineTrace() {
        return Tracing.trace(getContext().getTopologyTypeName() + " Broadcast", TracingManager.alwaysOnTracer()).tag("context_id",
                getContext().getTopologyInfo().getTopologyContextId()).tag("topology_id",
                getContext().getTopologyInfo().getTopologyContextId())
            .baggageItem(Tracing.DISABLE_DB_TRACES_BAGGAGE_KEY, "");
    }

    protected TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }

    /**
     * A passthrough stage is a pipeline stage where the type of input and output is the same.
     * If the input is mutable, it may change during the stage. The stage may also not change
     * the input, but perform some operations based on the input (e.g. recording the input somewhere).
     *
     * @param <T> The type of the input.
     */
    public abstract static class PassthroughStage<T> extends Pipeline.PassthroughStage<T, TopologyPipelineContext> {
    }


    /**
     * A pipeline stage takes an input and produces an output that gets passed along to the
     * next stage.
     *
     * @param <I2> The type of the input.
     * @param <O2> The type of the output.
     */
    public abstract static class Stage<I2, O2> extends Pipeline.Stage<I2, O2, TopologyPipelineContext> {
    }
}
