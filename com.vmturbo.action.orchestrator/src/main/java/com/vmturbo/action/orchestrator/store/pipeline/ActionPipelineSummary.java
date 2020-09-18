package com.vmturbo.action.orchestrator.store.pipeline;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.components.common.pipeline.PipelineSummary;
import com.vmturbo.components.common.pipeline.Stage;

/**
 * The summary of {@link ActionPipeline}, intended to be a quick way to visualize what went
 * right/wrong in a pipeline execution.
 */
@ThreadSafe
public class ActionPipelineSummary extends PipelineSummary {

    /**
     * The context of the {@link ActionPipeline} this {@link ActionPipelineSummary} describes.
     */
    private final ActionPipelineContext context;

    ActionPipelineSummary(@Nonnull final Clock clock,
                            @Nonnull final ActionPipelineContext context,
                            @Nonnull final List<Stage> stages) {
        super(clock, stages);
        this.context = Objects.requireNonNull(context);
    }

    @Override
    protected String getPreamble() {
        return context.getPipelineName();
    }
}
