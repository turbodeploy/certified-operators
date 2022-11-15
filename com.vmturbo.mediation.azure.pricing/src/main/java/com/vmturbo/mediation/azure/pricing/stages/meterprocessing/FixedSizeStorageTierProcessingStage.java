package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.common.storage.StorageTier;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.hybrid.cloud.common.azure.AzureStorageSizeType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
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
public class FixedSizeStorageTierProcessingStage<E extends ProbeStageEnum> extends AbstractStorageTierMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Set<StorageTier> SKU_IDS = Sets.newHashSet(
            StorageTier.UNMANAGED_PREMIUM,
            StorageTier.MANAGED_PREMIUM,
            StorageTier.MANAGED_STANDARD,
            StorageTier.MANAGED_STANDARD_SSD);

    /**
     * Constructor.
     *
     * @param probeStage a {@link ProbeStageEnum}
     */
    public FixedSizeStorageTierProcessingStage(@NotNull E probeStage) {
        super(probeStage, SKU_IDS.stream().map(Objects::toString).collect(Collectors.toSet()), LOGGER);
    }

    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
                                 @NotNull List<ResolvedMeter> resolvedMeters) {
        // grouped by PlanId, RegionId, StorageTier
        final TreeMap<String, TreeMap<String, TreeMap<StorageTier, HashSet<ResolvedMeter>>>>
                azureMetersByPlanIdRegionIdStorageTier = new TreeMap<>();

        resolvedMeters.stream().forEach(resolvedMeter -> {
            final AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            final Map<String, TreeMap<Double, AzureMeter>> pricing = resolvedMeter.getPricing();

            for (final String storageTierSku : descriptor.getSkus()) {
                try {
                    final StorageTier storageTier = StorageTier.valueOf(storageTierSku);
                    for (final String planId : pricing.keySet()) {
                        for (String region : descriptor.getRegions()) {
                            azureMetersByPlanIdRegionIdStorageTier.computeIfAbsent(planId, p -> new TreeMap<>())
                                    .computeIfAbsent(region, r -> new TreeMap<>())
                                    .computeIfAbsent(storageTier, st -> new HashSet<>())
                                    .add(resolvedMeter);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    LOGGER.warn("Invalid Storage Tier Sku found: {}", storageTierSku);
                }
            }
        });

        Map<String, Map<String, Map<String, Integer>>> statusesByPlanAndRegion = new TreeMap<>();
        azureMetersByPlanIdRegionIdStorageTier.forEach((planId, azureMetersByRegionIdStorageTier) -> {
            azureMetersByRegionIdStorageTier.forEach((regionId, azureMetersByStorageTier) -> {
               azureMetersByStorageTier.forEach((storageTier, rms) -> {
                   OnDemandPriceTableByRegionEntry.Builder onDemandPriceTableForRegion = pricingWorkspace.getOnDemandBuilder(planId, regionId);

                   final EntityDTO storageTierEntity = EntityDTO.newBuilder()
                           .setEntityType(EntityType.STORAGE_TIER)
                           .setId(formatStorageTierId(storageTier.name())).build();
                   final StorageTierPriceList storageTierPriceList = createStorageTierPriceList(planId, storageTier, rms);
                   onDemandPriceTableForRegion.addStoragePriceTable(
                           StoragePriceTableByTierEntry.newBuilder()
                                   .setRelatedStorageTier(storageTierEntity)
                                   .setStorageTierPriceList(storageTierPriceList)
                                   .build());
                   statusesByPlanAndRegion.computeIfAbsent(planId, p -> new TreeMap<>())
                           .computeIfAbsent(regionId, r -> new TreeMap<>())
                           .put(storageTier.name(), rms.size());
               });
            });
        });

        getStageInfo().longExplanation(generateStatus(statusesByPlanAndRegion));

        return String.format("Processed %s storage tiers", SKU_IDS.toString());
    }

    private String generateStatus(@NotNull final Map<String, Map<String, Map<String, Integer>>> statusesByPlanAndRegion) {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage Pricing captured:\n");
        statusesByPlanAndRegion.forEach((planId, statusByRegion) -> {
            sb.append("Plan: ").append(planId).append(":\n");
            statusByRegion.forEach((region, statusByStorageTier) -> {
                sb.append("\t").append(region).append(": ");
                statusByStorageTier.forEach((storageTier, numberOfPrice) ->
                    sb.append(numberOfPrice).append(" ").append(storageTier).append("; ")
                );
                sb.append("\n");
            });
        });
        return sb.toString();
    }

    @NotNull
    private StorageTierPriceList createStorageTierPriceList(
            @NotNull final String planId,
            @NotNull final StorageTier storageTier,
            @NotNull final HashSet<ResolvedMeter> resolvedMetersByStorageTier) {

        final Set<Price> priceList = new HashSet<>();
        resolvedMetersByStorageTier.forEach(resolvedMeter -> {
            final AzureMeterDescriptor azureMeterDescriptor = resolvedMeter.getDescriptor();
            final Map<Double, AzureMeter> azureMeterMap = resolvedMeter.getPricing().get(planId);

            // E.g. E50
            final String azureStorageSizeTypeStr = azureMeterDescriptor.getSubType();
            try {
                final AzureStorageSizeType azureStorageSizeType = AzureStorageSizeType
                        .valueOf(azureStorageSizeTypeStr);

                azureMeterMap.values().forEach(azureMeter -> {
                    // TODO change to Using Unit Converter
                    Price.Builder priceBuilder = Price.newBuilder()
                            .setEndRangeInUnits(azureStorageSizeType.getSize())
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                                    .setAmount(azureMeter.getUnitPrice())
                                                    .build())
                            .setUnit(Unit.MONTH);
                    priceList.add(priceBuilder.build());
                });
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Error when trying to get AzureStorageSizeType for {}, {}", azureStorageSizeTypeStr, e.getMessage());
            }
        });

        final RedundancyType redundancyType = getRedundancyType(storageTier);
        final StorageTierPriceList storageTierPriceList =
                StorageTierPriceList.newBuilder().addAllCloudStoragePrice(
                        Collections.singleton(StorageTierPrice.newBuilder()
                                .addAllPrices(priceList)
                                .setRedundancyType(redundancyType)
                                .build())).build();
        return storageTierPriceList;
    }

    @NotNull
    private RedundancyType getRedundancyType(@NotNull final StorageTier storageTier) {
        RedundancyType redundancyType;
        try {
            redundancyType = RedundancyType.valueOf(com.vmturbo.mediation.cost.dto.StorageTier.valueOf(storageTier.name()).getRedundancy());
        } catch (Exception e) {
            // Default to LRS
            redundancyType = RedundancyType.LRS;
        }
        return redundancyType;
    }
}
