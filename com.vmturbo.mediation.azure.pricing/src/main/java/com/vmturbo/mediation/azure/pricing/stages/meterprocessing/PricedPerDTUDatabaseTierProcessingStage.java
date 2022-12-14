package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.DatabasePriceTableByTierEntry;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Database Tier processor for tiers priced by a per-DTU cost in each region.
 *
 * @param <E> {@link ProbeStageEnum}
 */
public class PricedPerDTUDatabaseTierProcessingStage<E extends ProbeStageEnum>
    extends AbstractDbTierMeterProcessingStage<E> {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Gson GSON = new Gson();
    private static final String READING_DTU_MAP_ERROR =
            "There was an error reading the Size to DTU count map from properties. "
                    + "The default map will be used";

    /**
     * The list of sizes that are priced per DTU, and the corresponding
     * number of DTUs for that size.
     */
    private static final Map<String, Double> DEFAULT_DTUS_BY_SIZE = sortMapByDtus(ImmutableMap.of(
        "Standard_S1", 20.0,
        "Standard_S2", 50.0,
        "Standard_S3", 100.0,
        "Standard_S4", 200.0,
        "Standard_S6", 400.0,
        "Standard_S7", 800.0,
        "Standard_S9", 1600.0,
        "Standard_S12", 3000.0
    ));

    private static final IProbePropertySpec<Map<String, Double>>
            DTUS_BY_SIZE_MAP = new PropertySpec<>(
            "db.dtu.sizes", (rawMap) -> parsePlanIdMap(rawMap, DEFAULT_DTUS_BY_SIZE),
            DEFAULT_DTUS_BY_SIZE);

    private FromContext<PriceConverter> priceConverterFromContext
            = requiresFromContext(PricingPipelineContextMembers.PRICE_CONVERTER);

    private FromContext<IPropertyProvider> propertyProvider
            = requiresFromContext(PricingPipelineContextMembers.PROPERTY_PROVIDER);

    /**
     * Constructor.
     *
     * @param probeStage a {@link ProbeStageEnum}
     */
    public PricedPerDTUDatabaseTierProcessingStage(@NotNull E probeStage) {
        super(probeStage, MeterType.DTU, LOGGER);
    }

    @Override
    protected String processSelectedMeters(@NotNull PricingWorkspace pricingWorkspace,
            @Nonnull Collection<ResolvedMeter> resolvedMeters) {
        final PriceConverter priceConverter = priceConverterFromContext.get();
        final Map<String, Double> dtusBySize = propertyProvider.get().getProperty(DTUS_BY_SIZE_MAP);

        // Group by Plan, Region -> Price per DTU

        NavigableMap<String, NavigableMap<String, Double>> planPrices = new TreeMap<>();

        int errors = 0;

        for (ResolvedMeter resolvedMeter : resolvedMeters) {
            AzureMeterDescriptor descriptor = resolvedMeter.getDescriptor();
            for (Entry<String, TreeMap<Double, AzureMeter>> byPlan : resolvedMeter.getPricing().entrySet()) {
                NavigableMap<String, Double> regionPrices
                    = planPrices.computeIfAbsent(byPlan.getKey(), k -> new TreeMap<>());

                for (String region : descriptor.getRegions()) {
                    try {
                        /*
                         * We need to do an extra division by 10 here, outside the generic unit
                         * conversion code, because of the weird way that DTUs are priced.
                         * You might expect that there would be a "DTU" product with, say
                         * unitPrice=12.345 and unitOfMeasure="1/Day", or perhaps
                         * unitPrice=123.45 and unitOfMeasure="10/Day", or similar.
                         * Instead, there is essentially a "10 DTUs" product with
                         * unitPrice=123.45 and unitOfMeasure="1/Day". If the unitOfMeasure was
                         * "10/Day", that would actually be a price per 100 DTUs, and so on.
                         */

                        regionPrices.put(region,
                            priceConverter.getPriceAmount(Unit.DAYS, byPlan.getValue()) / 10.0D);
                    } catch (PriceConversionException ex) {
                        LOGGER.warn("Plan {}" + byPlan.getKey() + " meter "
                                + resolvedMeter.getDescriptor() + ": " + ex.getMessage(), ex);
                        errors++;
                    }
                }
            }
        }

        // Generate DTOs

        int totalCount = 0;
        int totalSkipped = 0;

        Set<String> pricedSizes = new HashSet<>();
        Set<String> skippedSizes = new HashSet<>();

        // region -> count, skipped
        Map<String, Map<String, Pair<Integer, Integer>>> countsByPlanAndRegion = new TreeMap<>();

        for (Entry<String, NavigableMap<String, Double>> planEntry : planPrices.entrySet()) {
            String planId = planEntry.getKey();
            Map<String, Pair<Integer, Integer>> countsByRegion =
                countsByPlanAndRegion.computeIfAbsent(planId, k -> new TreeMap<>());

            for (Entry<String, Double> regionEntry : planEntry.getValue().entrySet()) {
                final String regionId = regionEntry.getKey();
                final double regionPrice = regionEntry.getValue();

                int regionCount = 0;
                int regionSkipped = 0;

                OnDemandPriceTableByRegionEntry.Builder priceTable = pricingWorkspace.getOnDemandBuilder(planId, regionId);

                Set<String> existingIds = priceTable.getDatabasePriceTableList().stream()
                    .map(DatabasePriceTableByTierEntry::getRelatedDatabaseTier)
                    .map(EntityDTO::getId)
                    .collect(Collectors.toSet());

                for (Entry<String, Double> sizeEntry : dtusBySize.entrySet()) {
                    final String id = AZURE_DBPROFILE_ID_PREFIX + sizeEntry.getKey();

                    // If the stage for individually priced database tiers already provided
                    // a price, don't override it.

                    if (existingIds.contains(id)) {
                        LOGGER.debug("DB tier {} already found in region {}, skipped", id, regionId);
                        skippedSizes.add(sizeEntry.getKey());
                        regionSkipped++;

                        continue;
                    }

                    final Price price = priceConverter.amountToPrice(Unit.DAYS, sizeEntry.getValue() * regionPrice);
                    final List<Price> dependentPrices = getDbStoragePriceList(planId, regionId,
                            sizeEntry.getKey().toLowerCase(), DeploymentType.NONE);

                    priceTable.addDatabasePriceTable(DatabasePriceTableByTierEntry.newBuilder()
                        .setRelatedDatabaseTier(EntityDTO.newBuilder()
                            .setEntityType(EntityType.DATABASE_TIER)
                            .setId(id))
                        .addDatabaseTierPriceList(DatabaseTierPriceList.newBuilder()
                            .setBasePrice(createBasePrice(price))
                            .addAllDependentPrices(dependentPrices)
                        ).build());

                    pricedSizes.add(sizeEntry.getKey());
                    regionCount++;
                }

                totalCount += regionCount;
                totalSkipped += regionSkipped;

                final int finalCount = regionCount;
                final int finalSkipped = regionSkipped;

                countsByRegion.compute(regionId, (k, v) -> (v == null)
                    ? new ImmutablePair<>(finalCount, finalSkipped)
                    : new ImmutablePair<>(v.getLeft() + finalCount, v.getRight() + finalSkipped));
            }
        }

        // Build a long explanation string with counts of prices by plan/region

        StringBuilder sb = new StringBuilder("Prices added and skipped by plan and region:\n");
        for (Entry<String, Map<String, Pair<Integer, Integer>>> planEntry : countsByPlanAndRegion.entrySet()) {
            sb.append("\n");
            sb.append(WordUtils.capitalizeFully(planEntry.getKey()));
            sb.append(":\n\n");

            for (Entry<String, Pair<Integer, Integer>> regionCount : planEntry.getValue().entrySet()) {
                sb.append(regionCount.getValue().getLeft());
                sb.append("\t");
                sb.append(regionCount.getValue().getRight());
                sb.append("\t");
                sb.append(regionCount.getKey());
                sb.append("\n");
            }
        }

        sb.append("\nSizes with pricing discovered, across all plans/regions:\n\n");
        sb.append(pricedSizes.stream().sorted().collect(Collectors.joining(", ")));
        sb.append("\n\nSizes skipped due to individual pricing being present, across all plans/regions:\n\n");
        sb.append(skippedSizes.stream().sorted().collect(Collectors.joining(", ")));

        return String.format("%d Per-DTU-priced Database Tier prices processed, %d skipped (already discovered), %d errors",
            totalCount, totalSkipped, errors);
    }

    /**
     * Utility function for parsing a string into a DTU sizes map, for use in property
     * definitions.
     *
     * @param rawMap the raw string to parse
     * @param defaultValue the default value to use if the parse fails
     * @return the resulting size to DTU count map
     */
    public static Map<String, Double> parsePlanIdMap(
            @Nullable final String rawMap,
            @Nonnull final Map<String, Double> defaultValue) {
        if (StringUtils.isBlank(rawMap)) {
            return defaultValue;
        }

        final Type itemsMapType = new TypeToken<Map<String, Double>>() {}.getType();
        try {
            return sortMapByDtus(GSON.fromJson(rawMap, itemsMapType));
        } catch (JsonParseException e) {
            LOGGER.error(READING_DTU_MAP_ERROR, e);
            return defaultValue;
        }
    }

    @Nonnull
    private static Map<String, Double> sortMapByDtus(@Nonnull Map<String, Double> map) {
        return map.entrySet().stream().sorted(Entry.comparingByValue())
            .collect(Collectors.toMap(
                Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}
