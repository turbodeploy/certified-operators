package com.vmturbo.action.orchestrator.store.pipeline;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link ActionPipeline} captures the different stages required to process actions
 * in action plans sent to the ActionOrchestrator. {@link ActionPipeline}s are built
 * in the {@link LiveActionPipelineFactory} for realtime pipelines, and
 * {@link PlanActionPipelineFactory} for plan pipelines.
 *
 * <p>The pipeline consists of a set of {@link Stage}s. The output of one stage becomes
 * the input to the next {@link Stage}. State can be shared between stages by attaching
 * and dropping members on the {@link ActionPipelineContext}. Context state sharing is
 * configured when defining the "provides" and "requires" of individual stages in the
 * pipeline (see {@code Stage#requiresFromContext(PipelineContextMemberDefinition)} and
 * {@code Stage#providesToContext(PipelineContextMemberDefinition, Supplier)}.
 * The stages are executed one at a time. In the future we can add parallel execution of
 * certain subsets of the pipeline, but that's not necessary yet.
 *
 * @param <I> The input to the pipeline. This is the input to the first stage.
 * @param <O> The output of the pipeline. This is the output of the last stage.
 */
public class ActionPipeline<I, O> extends Pipeline<I, O, ActionPipelineContext, ActionPipelineSummary> {

    /**
     * Label for action plan type.
     */
    public static final String ACTION_PLAN_TYPE_LABEL = "action_plan_type";

    /**
     * This metric tracks the total duration of action processing (i.e. all the stages
     * in the pipeline).
     */
    private static final DataMetricSummary PROCESS_ACTIONS_SUMMARY = DataMetricSummary.builder()
        .withName("ao_process_action_plan_duration_seconds")
        .withHelp("Duration of action plan processing.")
        .withLabelNames(ACTION_PLAN_TYPE_LABEL)
        .build()
        .register();

    /**
     * Create a new {@link ActionPipeline}.
     *
     * @param stages The stages to be used in the pipeline.
     * @param clock Clock to track the time.
     */
    protected ActionPipeline(@Nonnull final PipelineDefinition<I, O, ActionPipelineContext> stages, @Nonnull Clock clock) {
        super(stages, new ActionPipelineSummary(clock, stages.getContext(), stages.getStages()));
    }

    /**
     * Get the {@link ActionPlanInfo} describing the actions being processed by the pipeline.
     * This returns the current version of the {@link ActionPlanInfo}.
     *
     * @return The {@link ActionPlanInfo} describing the action plan processed by the pipeline.
     */
    @Nonnull
    public ActionPlanInfo getActionPlanInfo() {
        return getContext().getActionPlanInfo();
    }

    @Override
    protected DataMetricTimer startPipelineTimer() {
        return PROCESS_ACTIONS_SUMMARY.labels(getContext().getActionPlanTopologyType().name()).startTimer();
    }

    @Override
    protected TracingScope startPipelineTrace() {
        return Tracing.trace("Process " + getContext().getActionPlanTopologyType() + " actions")
            .tag("context_id", getContext().getTopologyContextId())
            .baggageItem(Tracing.DISABLE_DB_TRACES_BAGGAGE_KEY, "");
    }

    /**
     * A passthrough stage is a pipeline stage where the type of input and output is the same.
     * If the input is mutable, it may change during the stage. The stage may also not change
     * the input, but perform some operations based on the input (e.g. recording the input somewhere).
     *
     * @param <T> The type of the input.
     */
    public abstract static class PassthroughStage<T> extends Pipeline.PassthroughStage<T, ActionPipelineContext> {
    }

    /**
     * A passthrough stage that is required (its failure will fail the overall pipeline).
     *
     * @param <T> The type of the input.
     */
    public abstract static class RequiredPassthroughStage<T> extends PassthroughStage<T> {
        @Override
        public boolean required() {
            return true;
        }
    }

    /**
     * A pipeline stage takes an input and produces an output that gets passed along to the
     * next stage.
     *
     * @param <I2> The type of the input.
     * @param <O2> The type of the output.
     */
    public abstract static class Stage<I2, O2> extends
        com.vmturbo.components.common.pipeline.Stage<I2, O2, ActionPipelineContext> {
    }
}
