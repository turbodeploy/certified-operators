package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static com.vmturbo.mediation.cost.dto.StorageTier.MANAGED_ULTRA_SSD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.common.storage.StorageTier;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.azure.pricing.util.UnitConverter;
import com.vmturbo.mediation.cost.common.CostUtils;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.hybrid.cloud.common.azure.AzureStorageSizeType;
import com.vmturbo.mediation.hybrid.cloud.common.azure.AzureStorageTier;
import com.vmturbo.mediation.hybrid.cloud.common.azure.AzureStorageTypeException;
import com.vmturbo.mediation.hybrid.cloud.utils.TimeRange;
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
 * Stage to process StorageTier and build price table for Ultra Disks.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of
 *         discovery.
 */

public class UltraDiskStorageTierMeterProcessingStage<E extends ProbeStageEnum>
        extends AbstractStorageTierMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final HashMap<String, HashMap<String, Set<ResolvedMeter>>>
            azureMeterByPlanIdAndRegionId = new HashMap<>();

    /**
     * Create a UltraDiskStorageTierMeterProcessingStage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for
     *         reporting
     *         detailed discovery status.
     */
    public UltraDiskStorageTierMeterProcessingStage(@Nonnull E probeStage) {
        super(probeStage, Collections.singleton(StorageTier.MANAGED_ULTRA_SSD.name()), LOGGER);
    }

    @Override
    String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @NotNull List<ResolvedMeter> resolvedMeters) {
        Map<String, Integer> statuses = new HashMap<>();
        resolvedMeters.forEach(resolvedMeter -> {
            final AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            final Map<String, TreeMap<Double, AzureMeter>> pricing = resolvedMeter.getPricing();
            for (String planId : pricing.keySet()) {
                for (String region : descriptor.getRegions()) {
                    azureMeterByPlanIdAndRegionId.computeIfAbsent(planId, k -> new HashMap<>())
                            .computeIfAbsent(region, k -> new HashSet<>())
                            .add(resolvedMeter);
                }
            }
        });
        azureMeterByPlanIdAndRegionId.forEach((planId, value) -> {
            final AtomicInteger addedCount = new AtomicInteger(0);
            value.forEach((region, value1) -> {

                final Set<Price> priceList = new HashSet<>();
                value1.forEach(meter -> {
                    final AzureMeterDescriptor meterDescriptor = meter.getDescriptor();
                    String subType = meterDescriptor.getSubType();
                    final Map<String, TreeMap<Double, AzureMeter>> pricing = meter.getPricing();
                    final TreeMap<Double, AzureMeter> pricingForPlan = pricing.get(planId);
                    List<AzureMeter> storageMeters = new ArrayList<>(pricingForPlan.values());
                    if (storageMeters.isEmpty()) {
                        LOGGER.warn("No Storage pricing found for planId={}, SKU={}", planId,
                                meterDescriptor.getSkus());
                    } else {
                        AzureMeter storageMeter = storageMeters.get(0);
                        if (CostUtils.COST_TYPE_STORAGE_GB_PER_MONTH.equals(subType)) {
                            priceList.addAll(
                                    processFlatRateToFixedSizes(planId, storageMeter.getUnitPrice(),
                                            meterDescriptor));
                        } else if (CostUtils.STORAGE_COST_TYPE_TO_UNIT.containsKey(subType)) {
                            Price fixedPrice = getPrice(
                                    storageMeter.getUnitPrice() * TimeRange.HOURS_PER_MONTH, null,
                                    CostUtils.STORAGE_COST_TYPE_TO_UNIT.get(subType));
                            if (fixedPrice != null) {
                                priceList.add(fixedPrice);
                            }
                        }
                    }
                });

                final EntityDTO tierEntity = EntityDTO.newBuilder().setEntityType(
                        EntityType.STORAGE_TIER).setId(formatStorageTierId(MANAGED_ULTRA_SSD.getSdkName())).build();
                final StorageTierPriceList priceSet =
                        StorageTierPriceList.newBuilder().addAllCloudStoragePrice(
                                Collections.singleton(StorageTierPrice.newBuilder()
                                        .addAllPrices(priceList)
                                        .setRedundancyType(RedundancyType.valueOf(
                                                MANAGED_ULTRA_SSD.getRedundancy()))
                                        .build())).build();
                OnDemandPriceTableByRegionEntry.Builder onDemand =
                        pricingWorkspace.getOnDemandBuilder(planId, region);
                onDemand.addStoragePriceTable(StoragePriceTableByTierEntry.newBuilder()
                        .setRelatedStorageTier(tierEntity)
                        .setStorageTierPriceList(priceSet)
                        .build());
                addedCount.incrementAndGet();
            });
            statuses.put(planId, addedCount.get());
        });
        return String.format("Managed Ultra SSD prices added for [PlanId, Region]: %s",
                statuses);
    }

    private Set<Price> processFlatRateToFixedSizes(String planId, double rate,
            AzureMeterDescriptor meterDescriptor) {
        Set<Integer> duplicateSize = new HashSet<>();
        Set<Price> priceWithRange = new HashSet<>();
        for (String sku : meterDescriptor.getSkus()) {
            Collection<AzureStorageSizeType> availableSizes;
            try {
                availableSizes = AzureStorageSizeType.getStorageMap(
                        AzureStorageTier.fromString(sku)).values();
            } catch (AzureStorageTypeException | IllegalArgumentException ex) {
                LOGGER.debug("Unrecognized volume tier {} in plan {}", sku, planId);
                continue;
            }

            for (AzureStorageSizeType size : availableSizes) {
                if (duplicateSize.add(size.getSize())) {
                    Price price = getPrice(rate * size.getSize() * TimeRange.HOURS_PER_MONTH,
                            size.getSize(), CostUtils.COST_UNIT_MONTHLY);
                    if (price != null) {
                        priceWithRange.add(price);
                    }
                }
            }
        }
        return priceWithRange;
    }

    private Price getPrice(Double value, Integer endRange, String costUnitMonthly) {
        Price.Builder priceBuilder = Price.newBuilder();
        if (endRange != null) {
            priceBuilder.setEndRangeInUnits(endRange);
        }
        Unit unit = UnitConverter.priceUnitMap.get(costUnitMonthly);
        if (unit == null) {
            LOGGER.warn("Unknown unit: " + costUnitMonthly);
        } else if (value != null) {
            // leaving the default currency, which is USD
            CurrencyAmount.Builder currencyAmountBuilder = CurrencyAmount.newBuilder().setAmount(
                    value);
            return priceBuilder.setPriceAmount(currencyAmountBuilder.build()).setUnit(unit).build();
        }
        return null;
    }
}
