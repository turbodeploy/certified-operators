package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.azure.pricing.util.PriceConversionException;
import com.vmturbo.mediation.azure.pricing.util.PriceConverter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.StoragePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

/**
 * Storage Tier Processing for Fixed Size.
 *
 * @param <E> {@link ProbeStageEnum}
 */
public class LinearSizeStorageTierProcessingStage<E extends ProbeStageEnum>
    extends AbstractStorageTierMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private FromContext<PriceConverter> priceConverterFromContext
        = requiresFromContext(PricingPipelineContextMembers.PRICE_CONVERTER);

    private static final Map<String, ImmutablePair<String, RedundancyType>> SKUS = ImmutableMap.of(
        "UNMANAGED_STANDARD_ZRS", new ImmutablePair<>("UNMANAGED_STANDARD", RedundancyType.ZRS),
        "UNMANAGED_STANDARD_LRS", new ImmutablePair<>("UNMANAGED_STANDARD", RedundancyType.LRS),
        "UNMANAGED_STANDARD_GRS", new ImmutablePair<>("UNMANAGED_STANDARD", RedundancyType.GRS),
        "UNMANAGED_STANDARD_RAGRS", new ImmutablePair<>("UNMANAGED_STANDARD", RedundancyType.RAGRS)
    );

    /**
     * Constructor.
     *
     * @param probeStage a {@link ProbeStageEnum}
     */
    public LinearSizeStorageTierProcessingStage(@NotNull E probeStage) {
        super(probeStage, SKUS.keySet(), LOGGER);
    }

    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
                                 @NotNull List<ResolvedMeter> resolvedMeters) {
        final PriceConverter priceConverter = priceConverterFromContext.get();

        // Group by Plan, Region, Tier, Redundancy -> Prices list by max quantity

        TreeMap<String, TreeMap<String, TreeMap<String, TreeMap<RedundancyType, List<Price>>>>>
            planPrices = new TreeMap<>();

        int errors = 0;

        for (ResolvedMeter resolvedMeter : resolvedMeters) {
            AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            for (Entry<String, TreeMap<Double, AzureMeter>> byPlan : resolvedMeter.getPricing().entrySet()) {
                TreeMap<String, TreeMap<String, TreeMap<RedundancyType, List<Price>>>> regionPrices
                    = planPrices.computeIfAbsent(byPlan.getKey(), k -> new TreeMap<>());

                for (String region : descriptor.getRegions()) {
                    TreeMap<String, TreeMap<RedundancyType, List<Price>>> tierPrices =
                        regionPrices.computeIfAbsent(region, k -> new TreeMap<>());

                    for (String sku : descriptor.getSkus()) {
                        final ImmutablePair<String, RedundancyType> skuInfo = SKUS.get(sku);
                        if (skuInfo == null) {
                            LOGGER.warn("Ignoring unsupported/unrecognized storage SKU {}", sku);
                            errors++;
                        } else {
                            List<Price> prices = tierPrices
                                .computeIfAbsent(skuInfo.getLeft(), k -> new TreeMap<>())
                                .computeIfAbsent(skuInfo.getRight(), k -> new ArrayList<>());

                            try {
                                prices.addAll(priceConverter.getPrices(Unit.GB_MONTH, byPlan.getValue()));
                            } catch (PriceConversionException ex) {
                                LOGGER.warn("Plan {}" + byPlan.getKey() + " meter "
                                        + resolvedMeter.getDescriptor() + ": " + ex.getMessage(), ex);
                                errors++;
                            }
                        }
                    }
                }
            }
        }

        // Generate DTOs

        int totalCount = 0;
        Map<String, Map<String, Integer>> countsByPlanAndRegion = new TreeMap<>();

        for (Entry<String, TreeMap<String, TreeMap<String, TreeMap<RedundancyType, List<Price>>>>> planEntry
            : planPrices.entrySet()) {
            String planId = planEntry.getKey();
            Map<String, Integer> countsByRegion = countsByPlanAndRegion.computeIfAbsent(planId, k -> new TreeMap<>());

            for (Entry<String, TreeMap<String, TreeMap<RedundancyType, List<Price>>>> regionEntry
                : planEntry.getValue().entrySet()) {
                final String regionId = regionEntry.getKey();

                int regionCount = 0;

                OnDemandPriceTableByRegionEntry.Builder priceTable = pricingWorkspace.getOnDemandBuilder(planId, regionId);

                for (Entry<String, TreeMap<RedundancyType, List<Price>>> tierEntry
                    : regionEntry.getValue().entrySet()) {

                    StorageTierPriceList.Builder stPriceListBuilder = StorageTierPriceList.newBuilder();

                    for (Entry<RedundancyType, List<Price>> redundancyEntry : tierEntry.getValue().entrySet()) {
                        stPriceListBuilder.addCloudStoragePrice(StorageTierPrice.newBuilder()
                            .setRedundancyType(redundancyEntry.getKey())
                            .addAllPrices(redundancyEntry.getValue())
                            .build()
                        );

                        regionCount++;
                    }

                    priceTable.addStoragePriceTable(StoragePriceTableByTierEntry.newBuilder()
                        .setRelatedStorageTier(EntityDTO.newBuilder()
                            .setEntityType(EntityType.STORAGE_TIER)
                            .setId(formatStorageTierId(tierEntry.getKey()))
                            .build())
                        .setStorageTierPriceList(stPriceListBuilder.build())
                        .build());
                }

                totalCount += regionCount;
                final int finalCount = regionCount;

                countsByRegion.compute(regionId, (k, v) -> (v == null) ? finalCount : v + finalCount);
            }
        }

        // Build a long explanation string with counts of prices by plan/region

        StringBuilder sb = new StringBuilder("Prices added by plan and region:\n");
        for (Entry<String, Map<String, Integer>> planEntry : countsByPlanAndRegion.entrySet()) {
            sb.append("\n");
            sb.append(WordUtils.capitalizeFully(planEntry.getKey()));
            sb.append(":\n\n");

            for (Entry<String, Integer> regionCount : planEntry.getValue().entrySet()) {
                sb.append(regionCount.getValue());
                sb.append("\t");
                sb.append(regionCount.getKey());
                sb.append("\n");
            }
        }

        getStageInfo().longExplanation(sb.toString());

        return String.format("%d Linear-priced Storage Tier prices processed, %d errors", totalCount, errors);
    }
}
