package com.vmturbo.mediation.azure.pricing.pipeline;

import java.time.Clock;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link PricingPipeline} captures the different stages required to discover pricing
 * for a cloud account.
 *
 * <p>The pipeline consists of a set of {@link Stage}s. The output of one stage becomes
 * the input to the next {@link Stage}. State can be shared between stages by attaching
 * and dropping members on the {@link PricingPipelineContext}. Context state sharing is configured
 * when defining the "providesToContext" and "requiresFromContext" of individual stages in the pipeline (see
 * {@code Stage#requiresFromContext(PipelineContextMemberDefinition)} and
 * {@code Stage#providesToContext(PipelineContextMemberDefinition, Supplier)}.
 * The stages are executed one at a time.
 *
 * @param <I> The input to the pipeline. This is the input to the first stage
 * @param <O> The type output from the last stage of the pipeline
 */
public class PricingPipeline<I, O> extends
        Pipeline<I, O, PricingPipelineContext, PricingPipelineSummary> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Label for account name.
     */
    public static final String ACCOUNT_NAME_LABEL = "account_name";

    /**
     * This metric tracks the total duration of a topology broadcast (i.e. all the stages
     * in the pipeline).
     */
    private static final DataMetricSummary PRICING_DISCOVERY_SUMMARY = DataMetricSummary.builder()
            .withName("pricing_discovery")
            .withHelp("Live discovery of cloud pricing.")
            .withLabelNames(ACCOUNT_NAME_LABEL)
            .build()
            .register();

    /**
     * Create a new {@link PricingPipeline}.
     *
     * @param stages The {@link PipelineDefinition}.
     */
    public PricingPipeline(@Nonnull final PipelineDefinition<I, O, PricingPipelineContext> stages) {
        super(stages, new PricingPipelineSummary(Clock.systemUTC(), stages.getContext(), stages.getStages()));
    }

    @Override
    protected DataMetricTimer startPipelineTimer() {
        return PRICING_DISCOVERY_SUMMARY.labels(getContext().getAccountName()).startTimer();
    }

    @Override
    protected TracingScope startPipelineTrace() {
        return Tracing.trace("Pricing Discovery")
                .tag(ACCOUNT_NAME_LABEL, getContext().getAccountName())
                .baggageItem(Tracing.DISABLE_DB_TRACES_BAGGAGE_KEY, "");
    }

    /**
     * A passthrough stage is a pipeline stage where the type of input and output is the same.
     * If the input is mutable, it may change during the stage. The stage may also not change
     * the input, but perform some operations based on the input (e.g. recording the input somewhere).
     *
     * @param <T> The type of the input.
     */
    public abstract static class PassthroughStage<T> extends Pipeline.PassthroughStage<T, PricingPipelineContext> {
    }


    /**
     * A pipeline stage takes an input and produces an output that gets passed along to the
     * next stage.
     *
     * @param <I2> The type of the input.
     * @param <O2> The type of the output.
     * @param <C2> The type of the context.
     */
    public abstract static class Stage<I2, O2, C2 extends PipelineContext> extends
        com.vmturbo.components.common.pipeline.Stage<I2, O2, C2> {
    }
}
