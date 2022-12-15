package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.enums.DeploymentType;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.azure.pricing.util.PriceConversionException;
import com.vmturbo.mediation.azure.pricing.util.PriceConverter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.DatabasePriceTableByTierEntry;

/**
 * Meter Processing Stage for Individually Priced DBTier.
 *
 * @param <E> ProbeStageEnum
 */
public class IndividuallyPricedDbTierProcessingStage<E extends ProbeStageEnum> extends AbstractDbTierMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private FromContext<PriceConverter> priceConverterFromContext
            = requiresFromContext(PricingPipelineContextMembers.PRICE_CONVERTER);

    /**
     * Constructor.
     *
     * @param probeStage probe Stage.
     */
    public IndividuallyPricedDbTierProcessingStage(@NotNull E probeStage) {
        super(probeStage, MeterType.DB, LOGGER);
    }

    @NotNull
    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @NotNull Collection<ResolvedMeter> resolvedMeters) {
        final PriceConverter priceConverter = priceConverterFromContext.get();

        // Group by Plan, Region, Tier -> Price
        TreeMap<String, TreeMap<String, TreeMap<String, Price>>> planPrices = new TreeMap<>();

        Set<String> pricedSizes = new HashSet<>();

        int priceCount = 0;
        int errors = 0;

        for (ResolvedMeter resolvedMeter : resolvedMeters) {
            AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            for (Entry<String, TreeMap<Double, AzureMeter>> byPlan : resolvedMeter.getPricing().entrySet()) {
                TreeMap<String, TreeMap<String, Price>> regionPrices = planPrices.computeIfAbsent(
                        byPlan.getKey(), k -> new TreeMap<>());

                for (String region : descriptor.getRegions()) {
                    TreeMap<String, Price> tierPrices = regionPrices.computeIfAbsent(region,
                        k -> new TreeMap<>());

                    try {
                        final Price tierPrice = priceConverter.getPrice(Unit.DAYS, byPlan.getValue());

                        for (String sku : descriptor.getSkus()) {
                            tierPrices.put(sku, tierPrice);
                            pricedSizes.add(sku);
                            priceCount++;
                        }
                    } catch (PriceConversionException ex) {
                        LOGGER.warn("Plan {}" + byPlan.getKey() + " meter "
                                + resolvedMeter.getDescriptor() + ": " + ex.getMessage(), ex);
                        errors += descriptor.getSkus().size();
                    }
                }
            }
        }

        // Generating DTOs
        for (Entry<String, TreeMap<String, TreeMap<String, Price>>> planEntry : planPrices.entrySet()) {
            final String planId = planEntry.getKey();

            for (Entry<String, TreeMap<String, Price>> regionEntry : planEntry.getValue().entrySet()) {
                final String regionId = regionEntry.getKey();

                OnDemandPriceTableByRegionEntry.Builder priceTable = pricingWorkspace.getOnDemandBuilder(planId, regionId);

                for (Entry<String, Price> tierEntry : regionEntry.getValue().entrySet()) {
                    DatabaseTierConfigPrice.Builder basePrice = createBasePrice(tierEntry.getValue());
                    final List<Price> dependentPrices = getDbStoragePriceList(planId, regionId,
                            tierEntry.getKey().toLowerCase(), DeploymentType.NONE);

                    DatabaseTierPriceList.Builder dbTierPriceListBuilder = DatabaseTierPriceList.newBuilder()
                            .setBasePrice(basePrice)
                            .addAllDependentPrices(dependentPrices);

                    DatabasePriceTableByTierEntry.Builder dbPriceTableByTierEntryBuilder = DatabasePriceTableByTierEntry.newBuilder()
                            .setRelatedDatabaseTier(EntityDTO.newBuilder()
                                    .setEntityType(EntityType.DATABASE_TIER)
                                    .setId(AZURE_DBPROFILE_ID_PREFIX + tierEntry.getKey()))
                            .addDatabaseTierPriceList(dbTierPriceListBuilder);

                    priceTable.addDatabasePriceTable(dbPriceTableByTierEntryBuilder);
                }
            }
        }

        getStageInfo().longExplanation("Sizes with discovered pricing across all plans/regions:\n\n"
            + pricedSizes.stream().sorted().collect(Collectors.joining(", ")));

        return String.format("%s DB Tiers prices added, %d errors.", priceCount, errors);
    }
}
