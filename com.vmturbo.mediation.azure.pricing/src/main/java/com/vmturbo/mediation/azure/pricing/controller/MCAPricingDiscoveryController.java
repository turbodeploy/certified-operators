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
import com.vmturbo.mediation.azure.pricing.stages.LicenseOverridesStage;
import com.vmturbo.mediation.azure.pricing.stages.MCAMeterDeserializerStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStage;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.PlanFallbackStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.DBStorageMeterProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.FixedSizeStorageTierProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.IPMeterProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.IndividuallyPricedDbTierProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.InstanceTypeProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.LicensePriceProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.LinearSizeStorageTierProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.PricedPerDTUDatabaseTierProcessingStage;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.UltraDiskStorageTierMeterProcessingStage;
import com.vmturbo.mediation.azure.pricing.util.PriceConverter;
import com.vmturbo.mediation.azure.pricing.util.VmSizeParser;
import com.vmturbo.mediation.azure.pricing.util.VmSizeParserImpl;
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
     * The property to use for mapping MCA price sheet plan name to Turbo plan IDs. The format is
     * Plan being processed -> Plan from which to copy pricing if the plan being processed doesn't
     * have pricing for a specific resolved meter.
     */
    public static final IProbePropertySpec<Map<String, String>> MCA_PLANID_MAP = new PropertySpec<>(
            "plan.idmap.mca", (rawMap) ->
            AssignPricingIdentifiersStage.parsePlanIdMap(rawMap, DEFAULT_MCA_PLANID_MAP),
            DEFAULT_MCA_PLANID_MAP);

    private static final Map<String, String> DEFAULT_MCA_PLAN_FALLBACK_MAP = new CaseInsensitiveMap(
            ImmutableMap.of(
                    "Azure plan for DevTest", "Azure plan"
            )
    );

    /**
     * The property to use for providing fallback from one plan's pricing to another when
     * one plan contains only partial pricing.
     */
    public static final IProbePropertySpec<Map<String, String>> MCA_PLAN_FALLBACK_MAP = new PropertySpec<>(
            "plan.fallback.mca", (rawMap) ->
            PlanFallbackStage.parsePlanFallbackMap(rawMap, DEFAULT_MCA_PLAN_FALLBACK_MAP),
            DEFAULT_MCA_PLAN_FALLBACK_MAP);


    private final Logger logger = LogManager.getLogger();
    private final MCAPricesheetFetcher mcaFetcher;
    private final CachingPricingFetcher<AzurePricingAccount> cacher;
    private final VmSizeParser parser;

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
        this.parser = new VmSizeParserImpl();
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
                .initialContextMember(PricingPipelineContextMembers.PRICE_CONVERTER,
                    () -> new PriceConverter())
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
                .addStage(new PlanFallbackStage(MCAPricingProbeStage.PLAN_FALLBACK, MCA_PLAN_FALLBACK_MAP))
                .addStage(new RegroupByTypeStage(MCAPricingProbeStage.REGROUP_METERS))
                .addStage(new IPMeterProcessingStage(MCAPricingProbeStage.IP_PRICE_PROCESSOR))
                .addStage(new InstanceTypeProcessingStage(MCAPricingProbeStage.INSTANCE_TYPE_PROCESSOR))
                .addStage(new LicenseOverridesStage(MCAPricingProbeStage.LICENSE_OVERRIDES, this.parser, propertyProvider))
                .addStage(new LicensePriceProcessingStage(MCAPricingProbeStage.LICENSE_PRICE_PROCESSOR))
                // Fixed Size is the first Storage Tier Processing stage as it will process most of the STs.
                .addStage(new FixedSizeStorageTierProcessingStage(MCAPricingProbeStage.FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR))
                .addStage(new LinearSizeStorageTierProcessingStage(MCAPricingProbeStage.LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR))
                .addStage(new UltraDiskStorageTierMeterProcessingStage(MCAPricingProbeStage.ULTRA_DISK_STORAGE_TIER))
                // This must come before other database pricing processors, which will consume
                // the discovered storage pricing and include it in their DTOs.
                .addStage(new DBStorageMeterProcessingStage(MCAPricingProbeStage.DB_STORAGE_METER_PROCESSOR))
                // Individually Priced DBTier is the first DBTier Processing stage.
                .addStage(new IndividuallyPricedDbTierProcessingStage(MCAPricingProbeStage.INDIVIDUALLY_PRICED_DB_TIER_PROCESSOR))
                // Price-per-DTU comes after individually priced, so that it can check for and not
                // overwrite any individual prices.
                .addStage(new PricedPerDTUDatabaseTierProcessingStage(MCAPricingProbeStage.PER_DTU_DATABASE_TIER_PROCESSOR))
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
                // TODO: use only mcaProfileId
                pricingIdentifier(PricingIdentifierName.BILLING_PROFILE_ID,
                        String.format("/providers/Microsoft.Billing/billingAccounts/%s/billingProfiles/%s", mcaAccountId, mcaProfileId)),
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
