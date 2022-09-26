package com.vmturbo.mediation.azure.pricing.stages;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Pricing pipeline stage that filters AzureMeters of interest based on the effective start/end
 * date.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *         of discovery.
 */
public class DateFilterStage<E extends ProbeStageEnum>
        extends Stage<Stream<AzureMeter>, Stream<AzureMeter>, E> implements AutoCloseable {

    final AtomicInteger totalCount = new AtomicInteger();
    final AtomicInteger filteredCount = new AtomicInteger();

    /**
     * Create a date filtering stage for MCA meters.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public DateFilterStage(@Nonnull E probeStage) {
        super(probeStage);
    }

    @NotNull
    @Override
    protected StageResult<Stream<AzureMeter>> executeStage(
            @NotNull Stream<AzureMeter> azureMeterStream)
            throws PipelineStageException, InterruptedException {
        long currentTimeEpoch = getCurrentInstant().toEpochMilli();
        Stream<AzureMeter> filteredMeter = azureMeterStream.filter(meter -> {
            totalCount.incrementAndGet();
            if ((meter.getEffectiveStartDate().toInstant().toEpochMilli() <= currentTimeEpoch) && (
                    currentTimeEpoch <= meter.getEffectiveEndDate().toInstant().toEpochMilli())) {
                filteredCount.incrementAndGet();
                return true;
            }
            return false;
        });

        getContext().autoClose(this);

        return StageResult.withResult(filteredMeter).andStatus(Status.success("Filtered records by applicable effective date"));
    }

    /**
     * used to obtain the current datetime.
     *
     * @return current Instant.
     */
    @VisibleForTesting
    protected Instant getCurrentInstant() {
        return Instant.now();
    }

    @Override
    public void close() {
        final String status = String.format("removed %d filters from %d input filters",
                totalCount.get() - filteredCount.get(), totalCount.get());
        getStageInfo().ok(status);
    }
}
