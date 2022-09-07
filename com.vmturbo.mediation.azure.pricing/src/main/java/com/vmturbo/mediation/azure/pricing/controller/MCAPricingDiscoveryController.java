package com.vmturbo.mediation.azure.pricing.controller;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.AzurePricingAccount;
import com.vmturbo.mediation.azure.pricing.fetcher.CachingPricingFetcher;
import com.vmturbo.mediation.azure.pricing.fetcher.MCAPricesheetFetcher;
import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.stages.FetcherStage;
import com.vmturbo.mediation.azure.pricing.stages.PlaceholderFinalStage;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Discovery controller for Azure MCA pricing.
 */
public class MCAPricingDiscoveryController extends
        AbstractPricingDiscoveryController<AzurePricingAccount, MCAPricingProbeStage> {

    /**
     * Time since last download before which we'll always use the cached file, and
     * after which we'll try to download a new one, but continue with the cached one
     * if the download fails.
     */
    public static final IProbePropertySpec<Duration> MCA_REFRESH_TIME = new PropertySpec<>(
            "download.refresh.mca", Duration::parse, Duration.parse("P1D"));

    /**
     * Time since last download after which if we can't download a new file, we consider
     * the cached download to be too old to trust and will fail discovery. Should be
     * greater than MCA_REFRESH_TIME.
     */
    public static final IProbePropertySpec<Duration> MCA_EXPIRE_TIME = new PropertySpec<>(
            "download.expire.mca", Duration::parse, Duration.parse("P14D"));

    /**
     * A path that if provided, will be returned instead of downloading to a new path.
     * This can be used for things like using a customer's pricing data file from diags
     * instead of downloading our own.
     */

    public static final IProbePropertySpec<String> MCA_OVERRIDE_PATH = new PropertySpec<>(
            "download.override.mca", String::valueOf, "");

    private final Logger logger = LogManager.getLogger();
    private final MCAPricesheetFetcher mcaFetcher;
    private final CachingPricingFetcher<AzurePricingAccount> cacher;

    /**
     * Construct the controller for MCA pricing discovery.
     *
     * @param propertyProvider the property provider with configuration for this discovery.
     */
    public MCAPricingDiscoveryController(@Nonnull IPropertyProvider propertyProvider) {
        Path downloadDir = propertyProvider.getProperty(DOWNLOAD_DIRECTORY);

        this.mcaFetcher = new MCAPricesheetFetcher(downloadDir);
        this.cacher = CachingPricingFetcher.<AzurePricingAccount>newBuilder()
                .refreshAfter(propertyProvider.getProperty(MCA_REFRESH_TIME))
                .expireAfter(propertyProvider.getProperty(MCA_EXPIRE_TIME))
                .build(mcaFetcher);
    }

    @Override
    public boolean appliesToAccount(AzurePricingAccount account) {
        return StringUtils.isNotBlank(account.getMcaBillingAccountId())
            && StringUtils.isNotBlank(account.getMcaBillingProfileId());
    }

    @Nonnull
    @Override
    public DiscoveredPricing.Key getKey(@Nonnull AzurePricingAccount account) {
        return new MCAPricingKey(account.getMcaBillingAccountId(),
                account.getMcaBillingProfileId(), account.getPlanId());
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull AzurePricingAccount accountValues) {
        // TODO
        return ValidationResponse.newBuilder().build();
    }

    @Override
    @Nonnull
    public PricingPipeline<AzurePricingAccount, DiscoveredPricing> buildPipeline(
            @Nonnull PricingPipelineContext context,
            @Nonnull IPropertyProvider propertyProvider) {
        return new PricingPipeline<>(
            PipelineDefinition.<AzurePricingAccount, DiscoveredPricing,
                            PricingPipelineContext<MCAPricingProbeStage>>newBuilder(context)
                .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER,
                        () -> propertyProvider)
                .addStage(new FetcherStage<AzurePricingAccount, MCAPricingProbeStage>(
                    this.cacher, MCAPricingProbeStage.DOWNLOAD_PRICE_SHEET, MCA_OVERRIDE_PATH
                ))
                .finalStage(new PlaceholderFinalStage(MCAPricingProbeStage.PLACEHOLDER_FINAL)));
    }

    @NotNull
    @Override
    public List<MCAPricingProbeStage> getDiscoveryStages() {
        return MCAPricingProbeStage.DISCOVERY_STAGES;
    }

    /**
     * Implementation of a DiscoveredPricing key suitable for identifying MCA pricing.
     */
    private static class MCAPricingKey implements DiscoveredPricing.Key {
        private final String mcaAccountId;
        private final String mcaProfileId;
        private final String planId;

        MCAPricingKey(@Nonnull final String mcaAccountId, @Nonnull final String mcaProfileId,
                @Nonnull final String planId) {
            this.mcaAccountId = mcaAccountId;
            this.mcaProfileId = mcaProfileId;
            this.planId = planId;
        }

        @Nonnull
        @Override
        public List<PricingIdentifier> getPricingIdentifiers() {
            // TODO verify this is correct?

            return ImmutableList.of(
                pricingIdentifier(PricingIdentifierName.BILLING_ACCOUNT_NAME, mcaAccountId),
                pricingIdentifier(PricingIdentifierName.BILLING_PROFILE_ID, mcaProfileId),
                pricingIdentifier(PricingIdentifierName.AZURE_PLAN_ID, planId)
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MCAPricingKey that = (MCAPricingKey)o;
            return Objects.equals(mcaAccountId, that.mcaAccountId) && Objects.equals(mcaProfileId,
                    that.mcaProfileId) && Objects.equals(planId, that.planId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mcaAccountId, mcaProfileId, planId);
        }

        @Override
        public String toString() {
            return "MCA Account " + mcaAccountId + " Profile "
                    + mcaProfileId + " Plan " + planId;
        }
    }
}
