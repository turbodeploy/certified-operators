package com.vmturbo.repository.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRICE_INDEX;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_DISK;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.repository.topology.util.PlanEntityStatsExtractorUtil;

/**
 * A utility to convert extract requested stats from {@link TopologyEntityDTO}.
 * Split apart mostly for unit testing purposes, so that methods relying on this extraction
 * can be tested separately.
 */
@FunctionalInterface
interface PlanEntityStatsExtractor {

    /**
     * Extract the stats values from a given {@link ProjectedTopologyEntity} and add them to a new
     * {@link EntityStats} object.
     *
     * @param projectedEntity the {@link ProjectedTopologyEntity} to transform
     * @param statsFilter the stats filter to use to build the stat snapshot
     * @param statEpoch the type of epoch to set on the stat snapshot
     * @param snapshotDate the snapshot date to use for the stat snapshot
     * @return an {@link EntityStats} object populated from the current stats for the
     * given {@link ProjectedTopologyEntity}
     */
    @Nonnull
    EntityStats.Builder extractStats(@Nonnull ProjectedTopologyEntity projectedEntity,
                                     @Nonnull StatsFilter statsFilter,
                                     @Nullable StatEpoch statEpoch,
                                     long snapshotDate);

    /**
     * The default implementation of {@link PlanEntityStatsExtractor} for use in production.
     */
    class DefaultPlanEntityStatsExtractor implements PlanEntityStatsExtractor {

        private static final Logger logger = LogManager.getLogger();

        @Nonnull
        @Override
        public EntityStats.Builder extractStats(@Nonnull final ProjectedTopologyEntity projectedEntity,
                                                @Nonnull final StatsFilter statsFilter,
                                                @Nullable final StatEpoch statEpoch,
                                                final long snapshotDate) {
            Set<String> commodityNames = StatsUtils.collectCommodityNames(statsFilter);
            logger.debug("Extracting stats for commodities: {}", commodityNames);
            StatSnapshot.Builder snapshot = StatSnapshot.newBuilder();
            if (statEpoch != null) {
                snapshot.setStatEpoch(statEpoch);
            }
            snapshot.setSnapshotDate(snapshotDate);

            // Collect groupBy fields for different commodity types and store them in a map for
            // easy lookup.
            final Map<String, Set<String>> commodityNameToGroupByFields =
                statsFilter.getCommodityRequestsList().stream()
                    .filter(CommodityRequest::hasCommodityName)
                    .collect(Collectors.toMap(CommodityRequest::getCommodityName,
                        commodityRequest -> commodityRequest.getGroupByList().stream()
                            .filter(Objects::nonNull)
                            // Only support "key" and "virtualDisk" group by for now
                            // Both equate to grouping by the key (matches existing logic in
                            // History component).
                            .filter(groupBy -> KEY.equalsIgnoreCase(groupBy)
                                || VIRTUAL_DISK.equalsIgnoreCase(groupBy))
                            .collect(Collectors.toSet())
                    ));

            // commodities bought - TODO: compute capacity of commodities bought = seller capacity
            for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                projectedEntity.getEntity().getCommoditiesBoughtFromProvidersList()) {
                String providerOidString = Long.toString(commoditiesBoughtFromProvider.getProviderId());
                Multimap<String, CommodityBoughtDTO> commoditiesBoughtToAggregate = HashMultimap.create();
                commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(commodityBoughtDTO -> {
                    final CommodityType commodityType = commodityBoughtDTO.getCommodityType();
                    final String commodityName = getCommodityName(commodityType);
                    if (shouldIncludeCommodity(commodityName, commodityNames)) {
                        // Will either be empty, or contain a concatenated list of group by fields
                        // that apply to this commodity.
                        final String groupByKey =
                            getGroupByStringForCommodity(commodityNameToGroupByFields, commodityType);
                        String aggregationKey = commodityName + groupByKey;
                        // If multiple commodities are added to the multimap with the same
                        // aggregationKey, then they will be aggregated together in the code below.
                        commoditiesBoughtToAggregate.put(aggregationKey, commodityBoughtDTO);
                    }
                });
                // Each entry in the multimap is a list of commodities that should be aggregated
                commoditiesBoughtToAggregate.asMap().values().stream().forEach(commodityBoughtDTOS -> {
                    // The commodity type for all commodities being aggregated must be the same
                    CommodityType commodityType = commodityBoughtDTOS.iterator().next().getCommodityType();
                    final String commodityName = getCommodityName(commodityType);
                    // Set the key only if there are not multiple commodities being aggregated
                    final String key = commodityBoughtDTOS.size() == 1 ? commodityType.getKey() : "";
                    StatsAccumulator accumulator = new StatsAccumulator();
                    commodityBoughtDTOS.stream()
                        .forEach(commodityBoughtDTO -> accumulator.record(
                            commodityBoughtDTO.getUsed(),
                            commodityBoughtDTO.getPeak()));
                    final StatValue usedValues = accumulator.toStatValue();
                    // Currently, there are no requirements to set percentile for commodity bought
                    // stats. So setting histUtilizationValue to null.
                    final StatRecord statRecord =
                        buildStatRecord(commodityName, key, usedValues,
                                PlanEntityStatsExtractorUtil.buildStatValue(0), providerOidString,
                                StringConstants.RELATION_BOUGHT, null);
                    snapshot.addStatRecords(statRecord);
                });
            }
            // commodities sold
            String entityOidString = Long.toString(projectedEntity.getEntity().getOid());
            final List<CommoditySoldDTO> commoditySoldList = projectedEntity.getEntity().getCommoditySoldListList();
            Multimap<String, CommoditySoldDTO> commoditiesSoldToAggregate = HashMultimap.create();
            commoditySoldList.forEach(commoditySoldDTO -> {
                final CommodityType commodityType = commoditySoldDTO.getCommodityType();
                final String commodityName = getCommodityName(commodityType);
                if (shouldIncludeCommodity(commodityName, commodityNames)) {
                    final String groupByKey =
                        getGroupByStringForCommodity(commodityNameToGroupByFields, commodityType);
                    String aggregationKey = commodityName + groupByKey;
                    commoditiesSoldToAggregate.put(aggregationKey, commoditySoldDTO);
                }
            });
            commoditiesSoldToAggregate.asMap().values().stream().forEach(commoditySoldDTOS -> {
                // The commodity type for all commodities being aggregated must be the same
                CommodityType commodityType = commoditySoldDTOS.iterator().next().getCommodityType();
                final String commodityName = getCommodityName(commodityType);
                // Set the key only if there are not multiple commodities being aggregated
                final String key = commoditySoldDTOS.size() == 1 ? commodityType.getKey() : "";
                StatsAccumulator accumulator = new StatsAccumulator();
                StatsAccumulator capacityAccumulator = new StatsAccumulator();
                commoditySoldDTOS.forEach(commoditySoldDTO -> {
                        accumulator.record(
                                commoditySoldDTO.getUsed(),
                                commoditySoldDTO.getPeak());
                        if (commoditySoldDTO.hasCapacity()) {
                            capacityAccumulator.record(commoditySoldDTO.getCapacity());
                        }
                    });
                final StatValue usedValues = accumulator.toStatValue();
                final StatValue capacityValue = capacityAccumulator.toStatValue();

                // If the percentile value is available and only 1 commodity exists for this
                // commodity type (i.e. no aggregation is needed), include the percentile value in
                // the stat record. We cannot aggregate percentile values of different commodities.
                HistUtilizationValue percentileValue = null;
                if (commoditySoldDTOS.size() == 1) {
                    final CommoditySoldDTO commoditySoldDTO = commoditySoldDTOS.iterator().next();
                    final HistoricalValues historicalValues = commoditySoldDTO.getHistoricalUsed();
                    if (historicalValues != null && historicalValues.hasPercentile()) {
                        final double percentile = historicalValues.getPercentile();
                        final double capacity = commoditySoldDTO.getCapacity();
                        final StatValue percentileUsage = StatValue.newBuilder()
                                .setAvg((float)(capacity * percentile))
                                .build();
                        percentileValue = HistUtilizationValue.newBuilder()
                                .setType(StringConstants.PERCENTILE)
                                .setUsage(percentileUsage)
                                .setCapacity(capacityValue)
                                .build();
                    }
                }

                final StatRecord statRecord =
                    buildStatRecord(commodityName, key, usedValues, capacityValue, entityOidString,
                        StringConstants.RELATION_SOLD, percentileValue);
                snapshot.addStatRecords(statRecord);
            });

            if (commodityNames.contains(PRICE_INDEX)) {
                final float projectedPriceIdx = (float)projectedEntity.getProjectedPriceIndex();
                final StatValue statValue = PlanEntityStatsExtractorUtil.buildStatValue(projectedPriceIdx);
                final StatRecord priceIdxStatRecord = StatRecord.newBuilder()
                    .setName(PRICE_INDEX)
                    .setCurrentValue(projectedPriceIdx)
                    .setUsed(statValue)
                    .setPeak(statValue)
                    .setCapacity(statValue)
                    .build();
                snapshot.addStatRecords(priceIdxStatRecord);
            }

            return EntityStats.newBuilder()
                .setOid(projectedEntity.getEntity().getOid())
                .addStatSnapshots(snapshot);
        }

        private String getCommodityName(final CommodityType commodityType) {
            return UICommodityType.fromType(commodityType.getType()).apiStr();
        }

        private boolean shouldIncludeCommodity(final String commodityName,
                                               final Set<String> commoditiesToInclude) {
            return commoditiesToInclude.isEmpty() || commoditiesToInclude.contains(commodityName);
        }

        /**
         * Build a String containing a concatenation of all the group by values applicable to the
         * provided commodity.
         *
         * <p>Note: Currently, we only support grouping by commodity key (or not grouping at all).</p>
         *
         * @param commodityNameToGroupByFields a map from commodity name to the fields to group by.
         * @param commodityType the commodity type for which to generate the group by string.
         * @return a String containing a concatenation of all the group by values applicable to the
         *         provided commodity.
         */
        private String getGroupByStringForCommodity(
                final Map<String, Set<String>> commodityNameToGroupByFields,
                final CommodityType commodityType) {
            return commodityNameToGroupByFields
                .getOrDefault(getCommodityName(commodityType), Collections.emptySet()).stream()
                .map(groupByMapper(commodityType))
                .distinct()
                .collect(Collectors.joining());
        }

        private Function<String, String> groupByMapper(final CommodityType commodityType) {
            return groupByField -> {
                switch (groupByField) {
                    // Only support "key" and "virtualDisk" group by for now
                    // Both equate to grouping by the key (matches existing logic in
                    // History component).
                    case KEY:
                    case VIRTUAL_DISK:
                        return commodityType.getKey();
                    default:
                        return StringConstants.EMPTY_STRING;
                }
            };
        }

        /**
         * Create a new StatRecord with values populated.
         *
         * @param commodityName the name of the commodity
         * @param key the key associate with the commodity, or empty if no key
         * @param used used (or current) value recorded for one sample
         * @param capacity the total capacity for the commodity
         * @param providerOidString the OID for the provider - either this SE for sold, or the 'other'
         *                          SE for bought commodities
         * @param relation the relation ("bought" or "sold") of the commodity to the entity
         * @param histUtilizationValue historical utilization value
         * @return a new StatRecord initialized from the given values
         */
        private StatRecord buildStatRecord(@Nonnull final String commodityName,
                                           @Nonnull final String key,
                                           @Nonnull final StatValue used,
                                           @Nonnull final StatValue capacity,
                                           @Nonnull final String providerOidString,
                                           @Nonnull final String relation,
                                           @Nullable final HistUtilizationValue histUtilizationValue) {
            final String units = CommodityTypeUnits.fromString(commodityName).getUnits();
            StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                .setName(commodityName)
                .setUnits(units)
                .setCurrentValue(used.getAvg())
                .setUsed(used)
                .setPeak(used)
                .setCapacity(capacity)
                .setStatKey(key)
                .setProviderUuid(providerOidString)
                .setRelation(relation);
            if (histUtilizationValue != null) {
                statRecordBuilder.addHistUtilizationValue(histUtilizationValue);
            }
            return statRecordBuilder.build();
        }
    }
}
