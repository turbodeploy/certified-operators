package com.vmturbo.mediation.azure.pricing.pipeline;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.PipelineSummary;
import com.vmturbo.components.common.pipeline.Stage;

/**
 * The summary of {@link PricingPipeline}, intended to be a quick way to visualize what went
 * right/wrong in a pipeline execution.
 */
@ThreadSafe
public class PricingPipelineSummary extends PipelineSummary {

    /**
     * The context of the {@link PricingPipeline} this {@link PricingPipelineSummary} describes.
     */
    private final PricingPipelineContext context;

    PricingPipelineSummary(@Nonnull final Clock clock,
                            @Nonnull final PricingPipelineContext context,
                            @Nonnull final List<Stage> stages) {
        super(clock, stages);
        this.context = Objects.requireNonNull(context);
    }

    @Override
    protected String getPreamble() {
        return FormattedString.format("Account: {}\n", context.getAccountName());
    }
}
