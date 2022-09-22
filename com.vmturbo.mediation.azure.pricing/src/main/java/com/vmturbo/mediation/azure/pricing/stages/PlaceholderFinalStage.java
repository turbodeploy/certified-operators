package com.vmturbo.mediation.azure.pricing.stages;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * A pipeline stage for use during development, to return the expected type before we have the
 * entire pipeline developed. TODO remove when no longer needed.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class PlaceholderFinalStage<E extends ProbeStageEnum>
        extends Stage<PricingWorkspace, DiscoveredPricing, E> {
    /**
     * Construct the placeholder stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public PlaceholderFinalStage(@Nonnull E probeStage) {
        super(probeStage);
    }

    @NotNull
    @Override
    protected StageResult executeStage(@NotNull PricingWorkspace input) {
        final String status = "This is just a placeholder during development";

        getStageInfo().ok(status);

        return StageResult.withResult(new DiscoveredPricing())
                .andStatus(Status.success(status));
    }
}
