package com.vmturbo.mediation.azure.pricing.stages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.resolver.MeterResolver;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Pricing pipeline stage that resolves AzureMeters of interest into a set of resolved meters.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class MeterResolverStage<E extends ProbeStageEnum> extends
        PricingPipeline.Stage<Stream<AzureMeter>, Collection<ResolvedMeter>, E> {
    private final List<MeterResolver> resolvers;
    private int resolved = 0;
    private int ignored = 0;

    /**
     * Create a builder for instances of AzureMeterResolverStage.
     *
     * @param <E> The enum for the probe discovery stages that apply to this particular kind
     *   of discovery.
     * @return a new Builder.
     */
    @Nonnull
    public static <E extends ProbeStageEnum> Builder<E> newBuilder() {
        return new Builder<>();
    }

    private MeterResolverStage(@Nonnull E probeStage, @Nonnull List<MeterResolver> resolvers) {
        super(probeStage);
        this.resolvers = resolvers;
    }

    @NotNull
    @Override
    protected StageResult<Collection<ResolvedMeter>> executeStage(@NotNull Stream<AzureMeter> input)
            throws PipelineStageException {
        try {
            final Map<String, ResolvedMeter> resolvedById = new CaseInsensitiveMap();

            input.forEach(meter -> {
                resolveMeter(meter).ifPresent(descriptor -> resolvedById
                    .computeIfAbsent(meter.getMeterId(), k -> new ResolvedMeter(descriptor))
                    .putPricing(meter));
            });

            int totalMeters = ignored + resolved;

            // If something goes wrong and we get no pricing, abort rather than creating
            // an empty pricetable that overwrites valid pricing with nothing.

            if (resolved == 0) {
                throw new PipelineStageException(String.format(
                    "0 out of %d meters resolved. Aborting, will not create an empty price table.",
                    totalMeters));
            }

            final String status = String.format("%d (%.1f%%) meters of interest (%d distinct IDs), %d (%.1f%%) ignored",
                    resolved, 100.0D * resolved / totalMeters, resolvedById.size(), ignored,
                    100.0D * ignored / totalMeters);

            getStageInfo().ok(status);
            return StageResult.withResult(resolvedById.values()).andStatus(Status.success(status));
        } catch (Exception ex) {
            getStageInfo().fail(ex);

            throw new PipelineStageException(ex);
        }
    }

    @Nonnull
    private Optional<AzureMeterDescriptor> resolveMeter(@Nonnull AzureMeter meter) {
        for (MeterResolver resolver : resolvers) {
            Optional<AzureMeterDescriptor> descriptor = resolver.resolveMeter(meter);
            if (descriptor.isPresent()) {
                resolved++;

                return descriptor;
            }
        }

        ignored++;

        return Optional.empty();
    }

    /**
     * Builder class for AzureMeterResolverStage.
     *
     * @param <E> The enum for the probe discovery stages that apply to this particular kind
     *   of discovery.
     */
    public static class Builder<E extends ProbeStageEnum> {
        private List<MeterResolver> resolvers = new ArrayList<>();

        private Builder() {
        }

        /**
         * Add another meter resolved. Resolvers will be tried in the order added.
         *
         * @param resolver The resolver to add
         * @return this builder, for chaining
         */
        public Builder addResolver(@Nonnull MeterResolver resolver) {
            resolvers.add(resolver);

            return this;
        }

        /**
         * Build the stage.
         *
         * @param probeStage The enum value for this probe stage in discovery
         * @return the new stage
         */
        public MeterResolverStage build(E probeStage) {
            return new MeterResolverStage(probeStage, resolvers);
        }
    }
}
