package com.vmturbo.repository.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.VCPU_OVERCOMMITMENT;
import static com.vmturbo.common.protobuf.utils.StringConstants.KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.VMEM_OVERCOMMITMENT;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRICE_INDEX;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_DISK;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.repository.service.AbridgedSoldCommoditiesForProvider.AbridgedSoldCommodity;
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
     * @param statEpoch the type of epoch to set on the stat snapshot
     * @param providerIdToSoldCommodities oid to AbridgedSoldCommoditiesForProvider
     * @param commodityNameToProviderType commodities requested for specific provider types
     * @param commodityNameToGroupByFields groupBy fields for requested commodities
     * @param snapshotDate the snapshot date to use for the stat snapshot
     * @return an {@link EntityStats} object populated from the current stats for the
     * given {@link ProjectedTopologyEntity}
     */
    @Nonnull
    EntityStats.Builder extractStats(@Nonnull ProjectedTopologyEntity projectedEntity,
                                     @Nullable StatEpoch statEpoch,
                                     @Nonnull Map<Long, AbridgedSoldCommoditiesForProvider>
                                         providerIdToSoldCommodities,
                                     @Nonnull Map<String, Set<Integer>>
                                         commodityNameToProviderType,
                                     @Nonnull Map<String, Set<String>>
                                         commodityNameToGroupByFields,
                                     long snapshotDate);

    /**
     * The default implementation of {@link PlanEntityStatsExtractor} for use in production.
     */
    class DefaultPlanEntityStatsExtractor implements PlanEntityStatsExtractor {

        private static final Logger logger = LogManager.getLogger();

        /**
         * Map of entity type to list of attribute types. This map is used to extract plan entity
         * attributes and return these attributes as stats records.
         */
        private static final Map<TypeCase, List<String>> ENTITY_TYPE_TO_ATTRIBUTE_MAP =
                ImmutableMap.of(TypeCase.CONTAINER_PLATFORM_CLUSTER,
                        ImmutableList.of(VCPU_OVERCOMMITMENT, VMEM_OVERCOMMITMENT));

        @Nonnull
        @Override
        public EntityStats.Builder extractStats(@Nonnull final ProjectedTopologyEntity projectedEntity,
                                                @Nullable final StatEpoch statEpoch,
                                                @Nonnull Map<Long,
                                                    AbridgedSoldCommoditiesForProvider>
                                                            providerIdToSoldCommodities,
                                                @Nonnull Map<String, Set<Integer>>
                                                            commodityNameToProviderType,
                                                @Nonnull Map<String, Set<String>>
                                                            commodityNameToGroupByFields,
                                                final long snapshotDate) {
            final Set<String> commodityNames = commodityNameToProviderType.keySet();
            logger.debug("Extracting stats for commodities: {}", commodityNames);
            StatSnapshot.Builder snapshot = StatSnapshot.newBuilder();
            if (statEpoch != null) {
                snapshot.setStatEpoch(statEpoch);
            }
            snapshot.setSnapshotDate(snapshotDate);

            for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                projectedEntity.getEntity().getCommoditiesBoughtFromProvidersList()) {
                final long providerId = commoditiesBoughtFromProvider.getProviderId();
                final String providerOidString = Long.toString(providerId);
                final AbridgedSoldCommoditiesForProvider soldCommodities = providerIdToSoldCommodities
                    .get(providerId);
                Multimap<String, CommodityBoughtDTO> commoditiesBoughtToAggregate = HashMultimap.create();
                commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(commodityBoughtDTO -> {
                    final CommodityType commodityType = commodityBoughtDTO.getCommodityType();
                    final String commodityName = getCommodityName(commodityType);
                    // Return bought commodities only for provider types specified in the filter. If
                    // provider type not specified, return all commodities.
                    final Set<Integer> requestedProviderTypes =
                        commodityNameToProviderType.get(commodityName);
                    final boolean providerTypeMatches = requestedProviderTypes == null
                        || requestedProviderTypes.isEmpty()
                        || requestedProviderTypes.contains(soldCommodities.getProviderType());
                    if (shouldIncludeCommodity(commodityName, commodityNames)
                        && providerTypeMatches) {
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
                commoditiesBoughtToAggregate.asMap().values().forEach(commodityBoughtDTOS -> {
                    // The commodity type for all commodities being aggregated must be the same
                    CommodityType commodityType = commodityBoughtDTOS.iterator().next().getCommodityType();
                    // Set the key only if there are not multiple commodities being aggregated
                    final String key = commodityBoughtDTOS.size() == 1 ? commodityType.getKey() : "";
                    final Optional<Double> capacity = extractCapacityFromSoldCommodities(
                        soldCommodities, commodityType);
                    final StatsAccumulator capacityAccumulator = new StatsAccumulator();
                    capacity.ifPresent(capacityAccumulator::record);
                    final StatValue capacityValues = capacityAccumulator.toStatValue();
                    StatsAccumulator accumulator = new StatsAccumulator();
                    commodityBoughtDTOS.forEach(commodityBoughtDTO -> accumulator.record(
                            commodityBoughtDTO.getUsed(),
                            commodityBoughtDTO.getPeak()));
                    final StatValue usedValues = accumulator.toStatValue();
                    final HistUtilizationValue percentileValue =
                        createPercentileUtilization(commodityBoughtDTOS, capacityValues);
                    final StatRecord statRecord =
                        buildStatRecord(commodityType, key, usedValues, capacityValues,
                            providerOidString, StringConstants.RELATION_BOUGHT, percentileValue);
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
            commoditiesSoldToAggregate.asMap().values().forEach(commoditySoldDTOS -> {
                // The commodity type for all commodities being aggregated must be the same
                CommodityType commodityType = commoditySoldDTOS.iterator().next().getCommodityType();
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

                final HistUtilizationValue percentileValue =
                    createPercentileUtilization(commoditySoldDTOS, capacityValue);

                final StatRecord statRecord =
                    buildStatRecord(commodityType, key, usedValues, capacityValue, entityOidString,
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

            List<StatRecord> attributeStatsRecords =
                    extractEntityAttributeStatsRecords(projectedEntity.getEntity());
            snapshot.addAllStatRecords(attributeStatsRecords);

            return EntityStats.newBuilder()
                .setOid(projectedEntity.getEntity().getOid())
                .addStatSnapshots(snapshot);
        }

        @Nullable
        private <T> HistUtilizationValue createPercentileUtilization(
            final Collection<T> commodityDTOs, final StatValue capacityStat) {
            // If the percentile value is available and only 1 commodity exists for this
            // commodity type (i.e. no aggregation is needed), include the percentile value in
            // the stat record. We cannot aggregate percentile values of different commodities.
            if (commodityDTOs.size() == 1) {
                final T commodityDTO = commodityDTOs.iterator().next();
                final HistoricalValues historicalValues = getHistoricalUsedValue(commodityDTO);
                if (historicalValues != null && historicalValues.hasPercentile()) {
                    final double percentile = historicalValues.getPercentile();
                    final StatValue percentileUsage = StatValue.newBuilder()
                        .setAvg((float)(capacityStat.getAvg() * percentile))
                        .build();
                    return HistUtilizationValue.newBuilder()
                        .setType(StringConstants.PERCENTILE)
                        .setUsage(percentileUsage)
                        .setCapacity(capacityStat)
                        .build();
                }
            }
            return null;
        }

        @Nullable
        private static <T> HistoricalValues getHistoricalUsedValue(T commodityDTO) {
            if (commodityDTO instanceof CommodityBoughtDTO) {
                return ((CommodityBoughtDTO)commodityDTO).getHistoricalUsed();
            } else if (commodityDTO instanceof CommoditySoldDTO) {
                return ((CommoditySoldDTO)commodityDTO).getHistoricalUsed();
            } else {
                return null;
            }
        }

        private Optional<Double> extractCapacityFromSoldCommodities(
            @Nullable final AbridgedSoldCommoditiesForProvider soldCommodities,
            @Nonnull final CommodityType commodityType) {
            return Optional.ofNullable(soldCommodities)
                .flatMap(commodities -> commodities.getSoldCommodityList().stream()
                .filter(commodity -> commodity.getCommodityType()
                    == commodityType.getType())
                .findAny()
                .map(AbridgedSoldCommodity::getCapacity));
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
         * Extract attributes from given TopologyEntityDTO and create stats records for the attributes.
         *
         * @param entityDTO Given plan TopologyEntityDTO.
         * @return List of {@link StatRecord} created from entity attributes.
         */
        private List<StatRecord> extractEntityAttributeStatsRecords(@Nonnull final TopologyEntityDTO entityDTO) {
            List<StatRecord> statRecords = new ArrayList<>();
            if (!entityDTO.hasTypeSpecificInfo()) {
                return statRecords;
            }
            TypeSpecificInfo entityInfo = entityDTO.getTypeSpecificInfo();
            List<String> attributeNames = ENTITY_TYPE_TO_ATTRIBUTE_MAP.get(entityInfo.getTypeCase());
            if (attributeNames == null) {
                return statRecords;
            }
            for (String attributeName : attributeNames) {
                Optional<Double> attributeValue = extractAttributeValue(entityInfo, attributeName);
                attributeValue.ifPresent(value -> {
                    final StatValue statValue = PlanEntityStatsExtractorUtil.buildStatValue(value.floatValue());
                    final StatRecord attributeStatRecord = StatRecord.newBuilder()
                            .setName(attributeName)
                            .setCurrentValue(value.floatValue())
                            .setUsed(statValue)
                            .setPeak(statValue)
                            .setCapacity(statValue)
                            .build();
                    statRecords.add(attributeStatRecord);
                });
            }
            return statRecords;
        }

        private Optional<Double> extractAttributeValue(@Nonnull TypeSpecificInfo entityInfo,
                                                       @Nonnull String attributeName) {
            TypeCase entityTypeCase = entityInfo.getTypeCase();
            if (entityTypeCase == TypeCase.CONTAINER_PLATFORM_CLUSTER) {
                ContainerPlatformClusterInfo containerPlatformCluster = entityInfo.getContainerPlatformCluster();
                switch (attributeName) {
                    case VCPU_OVERCOMMITMENT:
                        return containerPlatformCluster.hasVcpuOvercommitment()
                                ? Optional.of(containerPlatformCluster.getVcpuOvercommitment())
                                : Optional.empty();
                    case VMEM_OVERCOMMITMENT:
                        return containerPlatformCluster.hasVmemOvercommitment()
                                ? Optional.of(containerPlatformCluster.getVmemOvercommitment())
                                : Optional.empty();
                    default:
                        logger.error("Unsupported attribute {} to extract for {}.", attributeName,
                                entityInfo.getTypeCase());
                }
            }
            return Optional.empty();
        }

        /**
         * Create a new StatRecord with values populated.
         *
         * @param commodityType the name of the commodity
         * @param key the key associate with the commodity, or empty if no key
         * @param used used (or current) value recorded for one sample
         * @param capacity the total capacity for the commodity
         * @param providerOidString the OID for the provider - either this SE for sold, or the 'other'
         *                          SE for bought commodities
         * @param relation the relation ("bought" or "sold") of the commodity to the entity
         * @param histUtilizationValue historical utilization value
         * @return a new StatRecord initialized from the given values
         */
        private StatRecord buildStatRecord(@Nonnull final CommodityType commodityType,
                                           @Nonnull final String key,
                                           @Nonnull final StatValue used,
                                           @Nonnull final StatValue capacity,
                                           @Nonnull final String providerOidString,
                                           @Nonnull final String relation,
                                           @Nullable final HistUtilizationValue histUtilizationValue) {
            StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                .setName(CommodityTypeMapping.getApiCommodityType(commodityType.getType()))
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
            final String typeUnits = CommodityTypeMapping.getUnitForCommodityType(commodityType.getType());
            if (typeUnits != null) {
                statRecordBuilder.setUnits(typeUnits);
            }
            return statRecordBuilder.build();
        }
    }
}
