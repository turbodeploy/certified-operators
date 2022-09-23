package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Abstract Meter Processing Stage for every Meter Processing Stage to extended.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of discovery.
 */
public abstract class AbstractMeterProcessingStage<E extends ProbeStageEnum> extends
        Stage<PricingWorkspace, PricingWorkspace, E> {

    /**
     * Logger from the class.
     */
    private final Logger logger;

    /**
     * {@link MeterType} of the processor.
     */
    public final MeterType meterType;

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     * @param meterType {@link MeterType} which the class is processing.
     */
    public AbstractMeterProcessingStage(@Nonnull E probeStage, @Nonnull MeterType meterType, @Nonnull Logger logger) {
        super(probeStage);
        this.meterType = meterType;
        this.logger = logger;
    }

    @Nonnull
    @Override
    protected StageResult<PricingWorkspace> executeStage(@Nonnull PricingWorkspace pricingWorkspace) throws PipelineStageException {

        try {
            final Collection<ResolvedMeter> resolvedMeters = pricingWorkspace.getAndRemoveResolvedMeterByMeterType(this.meterType);

            String status = "";
            if (resolvedMeters == null) {
                logger.warn("No meters found for {}", this.meterType);
                status = String.format("No meters found for %s.", this.meterType);
            } else {
                status = addPricingForResolvedMeters(pricingWorkspace, resolvedMeters);
            }

            getStageInfo().ok(status);
            return StageResult.withResult(pricingWorkspace).andStatus(Status.success(status));
        } catch (Exception ex) {
            getStageInfo().fail(ex).summary(ex.getMessage());
            throw new PipelineStageException("Failed to process " + meterType, ex);
        }
    }

    /**
     * Convert Resolved Meters to Price in the Workspace.
     *
     * @param pricingWorkspace {@link PricingWorkspace}
     * @param resolvedMeters collections of {@link ResolvedMeter}
     * @return status string
     */
    @Nonnull
    abstract String addPricingForResolvedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters);
}
