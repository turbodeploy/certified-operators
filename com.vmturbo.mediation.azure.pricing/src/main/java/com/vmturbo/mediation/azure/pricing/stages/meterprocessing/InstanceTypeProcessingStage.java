package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.ComputePriceTableByTierEntry;

/**
 * Stage to process MeterType VMProfile.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of discovery.
 */
public class InstanceTypeProcessingStage<E extends ProbeStageEnum> extends AbstractMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String ID_FORMAT = "azure::VMPROFILE::%s";

    private FromContext<PriceConverter> priceConverterFromContext
        = requiresFromContext(PricingPipelineContextMembers.PRICE_CONVERTER);

    /**
     * Create a VM Instance Type processor stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public InstanceTypeProcessingStage(@Nonnull E probeStage) {
        super(probeStage, MeterType.VMProfile, LOGGER);
    }

    @Override
    @Nonnull
    protected String addPricingForResolvedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters) {
        final PriceConverter priceConverter = priceConverterFromContext.get();
        int errors = 0;

        // We need to compare Linux vs Windows pricing for the same plan/region/instance type,
        // and find the least expensive OS to make the base price.
        // So regroup as plan, region, size -> TreeMap<Price, OS>

        Map<String, Map<String, Map<String, Map<OSType, Double>>>> instanceTypes = new TreeMap<>();

        for (ResolvedMeter resolvedMeter : resolvedMeters) {
            final AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();

            for (Entry<String, TreeMap<Double, AzureMeter>> pricingEntry : resolvedMeter.getPricing().entrySet()) {
                final String planId = pricingEntry.getKey();
                try {
                    double price = priceConverter.getPriceAmount(Unit.HOURS, pricingEntry.getValue());

                    Map<String, Map<String, Map<OSType, Double>>> regionMap = instanceTypes
                            .computeIfAbsent(planId, k -> new TreeMap<>());

                    for (String region : descriptor.getRegions()) {
                        Map<String, Map<OSType, Double>> skuMap = regionMap
                            .computeIfAbsent(region, k -> new TreeMap<>());

                        for (String vmSize : descriptor.getSkus()) {
                            Map<OSType, Double> osPrices = skuMap.computeIfAbsent(vmSize, k -> new TreeMap<>());
                            OSType osType = OSType.valueOf(descriptor.getSubType().toUpperCase());
                            Double curPrice = osPrices.get(osType);

                            if (curPrice == null) {
                                osPrices.put(osType, price);
                            } else if (curPrice != price) {
                                LOGGER.warn("Already have price of {} for {}, {}, {}, {} while adding price {}",
                                    curPrice, planId, region, vmSize, osType, price);
                            } else {
                                LOGGER.debug("Duplicate price of {} for {}, {}, {}, {}",
                                    curPrice, planId, region, vmSize, osType);
                            }
                        }
                    }
                } catch (IllegalArgumentException ex) {
                    LOGGER.warn("Unknown OSType " + descriptor.getSubType(), ex);
                    errors++;
                } catch (PriceConversionException ex) {
                    LOGGER.warn("Plan " + planId + " meter "
                        + resolvedMeter.getDescriptor() + ": " + ex.getMessage(), ex);
                    errors++;
                }
            }
        }

        int totalCount = 0;
        Map<String, Map<String, Integer>> countsByPlanAndRegion = new TreeMap<>();

        for (Entry<String, Map<String, Map<String, Map<OSType, Double>>>> planEntry : instanceTypes.entrySet()) {
            Map<String, Integer> countsByRegion = countsByPlanAndRegion.computeIfAbsent(planEntry.getKey(), k -> new TreeMap<>());

            for (Entry<String, Map<String, Map<OSType, Double>>> regionEntry : planEntry.getValue().entrySet()) {
                OnDemandPriceTableByRegionEntry.Builder builder =
                    pricingWorkspace.getOnDemandBuilder(planEntry.getKey(), regionEntry.getKey());

                boolean hasWindows;

                for (Entry<String, Map<OSType, Double>> instanceEntry : regionEntry.getValue().entrySet())  {
                    final String instanceType = instanceEntry.getKey();
                    Map<OSType, Double> byPriceMap = instanceEntry.getValue();
                    int count = 0;

                    List<ImmutablePair<Double, OSType>> pricesAscending =
                        byPriceMap.entrySet().stream()
                            .map(entry -> new ImmutablePair<>(entry.getValue(), entry.getKey()))
                            .sorted()
                            .collect(Collectors.toList());

                    // The lowest-priced option will be first. This is the base price.

                    ImmutablePair<Double, OSType> basePriceEntry = pricesAscending.remove(0);

                    double basePrice = basePriceEntry.getKey();
                    final ComputeTierConfigPrice baseConfigPrice = makePrice(basePriceEntry.getValue(),
                            priceConverter.amountToPrice(Unit.HOURS, basePrice));

                    count++;
                    hasWindows = basePriceEntry.getValue() == OSType.WINDOWS;

                    ComputeTierPriceList.Builder priceListBuilder =
                            ComputeTierPriceList.newBuilder().setBasePrice(baseConfigPrice);

                    // For each additional price, add a config price with the difference

                    while (!pricesAscending.isEmpty()) {
                        ImmutablePair<Double, OSType> priceEntry = pricesAscending.remove(0);

                        priceListBuilder.addPerConfigurationPriceAdjustments(makePrice(priceEntry.getValue(),
                                priceConverter.amountToPrice(Unit.HOURS, priceEntry.getKey() - basePrice)));

                        count++;
                        hasWindows = hasWindows || (priceEntry.getValue() == OSType.WINDOWS);
                    }

                    // Add the special WINDOWS_BYOL price that is always the same as the base price

                    if (hasWindows) {
                        priceListBuilder.addPerConfigurationPriceAdjustments(
                            makePrice(OSType.WINDOWS_BYOL, priceConverter.amountToPrice(Unit.HOURS, 0)));

                        count++;
                    }

                    // Build and add the compute price table entry

                    builder.addComputePriceTable(ComputePriceTableByTierEntry.newBuilder()
                        .setRelatedComputeTier(EntityDTO.newBuilder()
                            .setEntityType(EntityType.COMPUTE_TIER)
                            .setId(String.format(ID_FORMAT, instanceType)))
                        .setComputeTierPriceList(priceListBuilder));

                    totalCount += count;
                    int finalCount = count;
                    countsByRegion.compute(regionEntry.getKey(), (k, v) -> (v == null) ? finalCount
                        : v + finalCount);
                }
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

        return String.format("%d VM Instance Type prices processed, %d errors", totalCount, errors);
    }

    @Nonnull
    private ComputeTierConfigPrice makePrice(@Nonnull OSType osType, Price price) {
        return ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(osType)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(price)
            .build();
    }
}
