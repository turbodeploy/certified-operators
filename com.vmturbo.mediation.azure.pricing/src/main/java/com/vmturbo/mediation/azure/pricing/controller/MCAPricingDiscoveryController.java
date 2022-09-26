package com.vmturbo.mediation.azure.pricing.controller;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.AzurePricingAccount;
import com.vmturbo.mediation.azure.pricing.fetcher.CachingPricingFetcher;
import com.vmturbo.mediation.azure.pricing.fetcher.MCAPricesheetFetcher;
import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingKey;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.AssignPricingIdentifiersStage;
import com.vmturbo.mediation.azure.pricing.stages.BOMAwareReadersStage;
import com.vmturbo.mediation.azure.pricing.stages.ChainedCSVParserStage;
import com.vmturbo.mediation.azure.pricing.stages.DateFilterStage;
import com.vmturbo.mediation.azure.pricing.stages.FetcherStage;
import com.vmturbo.mediation.azure.pricing.stages.MCAMeterDeserializerStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStage;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.IPMeterProcessingStage;
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

    private static final Map<String, String> DEFAULT_MCA_PLANID_MAP = new CaseInsensitiveMap(
        ImmutableMap.of(
            "Azure plan", "0001",
            "Azure plan for DevTest", "0002"
        )
    );

    /**
     * The property to use for mapping MCA price sheet plan name to Turbo plan IDs.
     */
    public static final IProbePropertySpec<Map<String, String>> MCA_PLANID_MAP = new PropertySpec<>(
            "planid.map.mca", (rawMap) ->
            AssignPricingIdentifiersStage.parsePlanIdMap(rawMap, DEFAULT_MCA_PLANID_MAP),
            DEFAULT_MCA_PLANID_MAP);

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
    public PricingKey getKey(@Nonnull AzurePricingAccount account) {
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
            @Nonnull PricingKey pricingKey,
            @Nonnull IPropertyProvider propertyProvider) {
        return new PricingPipeline<>(
            PipelineDefinition.<AzurePricingAccount, DiscoveredPricing,
                            PricingPipelineContext<MCAPricingProbeStage>>newBuilder(context)
                .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER,
                        () -> propertyProvider)
                .initialContextMember(PricingPipelineContextMembers.PRICING_KEY, () -> pricingKey)
                .addStage(new FetcherStage<AzurePricingAccount, MCAPricingProbeStage>(
                    this.cacher, MCAPricingProbeStage.DOWNLOAD_PRICE_SHEET, MCA_OVERRIDE_PATH
                ))
                .addStage(new SelectZipEntriesStage("*.csv", MCAPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MCAPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage(MCAPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage(MCAPricingProbeStage.CHAINED_CSV_PARSERS))
                .addStage(new MCAMeterDeserializerStage(MCAPricingProbeStage.DESERIALIZE_METERS))
                .addStage(new DateFilterStage(MCAPricingProbeStage.EFFECTIVE_DATE_FILTER))
                .addStage(MeterResolverStage.newBuilder()
                    .addResolver(new MeterDescriptorsFileResolver())
                    .build(MCAPricingProbeStage.RESOLVE_METERS))
                .addStage(new RegroupByTypeStage(MCAPricingProbeStage.REGROUP_METERS))
                .addStage(new IPMeterProcessingStage(MCAPricingProbeStage.IP_PRICE_PROCESSOR))
                .finalStage(new AssignPricingIdentifiersStage(MCAPricingProbeStage.ASSIGN_IDENTIFIERS,
                        MCA_PLANID_MAP)));
    }

    @NotNull
    @Override
    public List<MCAPricingProbeStage> getDiscoveryStages() {
        return MCAPricingProbeStage.DISCOVERY_STAGES;
    }

    /**
     * Implementation of a DiscoveredPricing key suitable for identifying MCA pricing.
     */
    private static class MCAPricingKey implements PricingKey {
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

        @NotNull
        @Override
        public PricingKey withPlanId(@NotNull String planId) {
            return new MCAPricingKey(this.mcaAccountId, this.mcaProfileId, planId);
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
