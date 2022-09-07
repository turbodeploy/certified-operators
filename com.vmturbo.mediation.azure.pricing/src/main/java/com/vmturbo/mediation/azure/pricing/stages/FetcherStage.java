package com.vmturbo.mediation.azure.pricing.stages;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.fetcher.PricingFileFetcher;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.ProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * A pipeline stage that fetches pricing data from the provider, eg via downloading a file.
 *
 * @param <A> The account to use for fetching the data, and to which the pricing data will
 *   apply.
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class FetcherStage<A extends ProxyAwareAccount, E extends ProbeStageEnum>
        extends Stage<A, Path, PricingPipelineContext<E>> {
    private final PricingFileFetcher<A> fetcher;
    private final E probeStage;

    /**
     * A path that if provided, will be returned instead of downloading to a new path.
     * This can be used for things like using a customer's pricing data file from diags
     * instead of downloading our own.
     */
    public static final IProbePropertySpec<String> OVERRIDE_PATH = new PropertySpec<>(
            "download.override", String::valueOf, "");

    private final IProbePropertySpec<String> overrideProperty;

    private FromContext<IPropertyProvider> propertyProvider =
            requiresFromContext(PricingPipelineContextMembers.PROPERTY_PROVIDER);

    /**
     * Construct a pricing data fetcher stage.
     *
     * @param fetcher the PricingFileFetcher instance this stage will use to fetch the data
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     * @param overrideProperty the property to check for, to use an override path for testing
     *   and with customer's downloads from diags.
     */
    public FetcherStage(@Nonnull PricingFileFetcher<A> fetcher,
            @Nonnull E probeStage,
            @Nonnull IProbePropertySpec<String> overrideProperty) {
        this.fetcher = fetcher;
        this.probeStage = probeStage;
        this.overrideProperty = overrideProperty;
    }

    /**
     * Construct a pricing data fetcher stage.
     *
     * @param fetcher the PricingFileFetcher instance this stage will use to fetch the data
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public FetcherStage(@Nonnull PricingFileFetcher<A> fetcher, @Nonnull E probeStage) {
        this(fetcher, probeStage, OVERRIDE_PATH);
    }

    @NotNull
    @Override
    protected StageResult<Path> executeStage(@NotNull A account)
            throws PipelineStageException {
        final ProbeStageTracker<E> tracker = getContext().getStageTracker();
        final String overridePath = propertyProvider.get().getProperty(overrideProperty);
        Pair<Path, String> result;

        try {
            if (StringUtils.isNotBlank(overridePath)) {
                result = new Pair<>(Paths.get(overridePath), "Probe configured with "
                        + overrideProperty.getPropertyKey() + " of " + overridePath
                        + ", will use instead of downloading.");
            } else {
                result = fetcher.fetchPricing(account, propertyProvider.get());
            }
            tracker.stage(probeStage).ok(result.getSecond());
        } catch (Exception ex) {
            tracker.stage(probeStage).fail(ex).summary(ex.getMessage());
            throw new PipelineStageException("Failed to fetch pricing", ex);
        }

        return StageResult.withResult(result.getFirst())
                .andStatus(Status.success(result.getSecond()));
    }
}
