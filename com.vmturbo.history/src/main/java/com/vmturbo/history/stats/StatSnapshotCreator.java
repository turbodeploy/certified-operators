package com.vmturbo.history.stats;

import static com.vmturbo.components.common.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.CAPACITY;
import static com.vmturbo.components.common.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.components.common.utils.StringConstants.EFFECTIVE_CAPACITY;
import static com.vmturbo.components.common.utils.StringConstants.ENTITY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.KEY;
import static com.vmturbo.components.common.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.MIN_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.PRODUCER_UUID;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.RELATED_ENTITY;
import static com.vmturbo.components.common.utils.StringConstants.RELATION;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.UTILIZATION;
import static com.vmturbo.components.common.utils.StringConstants.VIRTUAL_DISK;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.jooq.Record;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * A helper class to assemble database {@link Record}s into a {@link StatSnapshot}
 * that can be returned to clients.
 */
@FunctionalInterface
public interface StatSnapshotCreator {

    /**
     * Process the given DB Stats records, organizing into {@link StatSnapshot} by time and commodity,
     * and then invoking the handler ({@link Consumer}) from the caller on each StatsSnapshot that is built.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @param fullMarket is this a query against the full market table vs. individual SE type
     * @param commodityRequests a list of {@link CommodityRequest} being satifisfied in this query
     */
    Stream<Builder> createStatSnapshots(@Nonnull final List<Record> statDBRecords,
                                        final boolean fullMarket,
                                        @Nonnull final List<CommodityRequest> commodityRequests);

    /**
     * The default implementation of {@link StatSnapshotCreator}, for production use.
     */
    class DefaultStatSnapshotCreator implements StatSnapshotCreator {

        // map from groupBy string provided by API to the corresponding table field name in DB
        // virtualDisk id is saved as commodity key in db, thus mapped to commodity key column
        private static final Map<String, String> API_GROUP_BY_STR_TO_TABLE_FIELD = ImmutableMap.of(
            KEY, COMMODITY_KEY,
            RELATED_ENTITY, PRODUCER_UUID,
            VIRTUAL_DISK, COMMODITY_KEY
        );

        private StatRecordBuilder statRecordBuilder;

        public DefaultStatSnapshotCreator(@Nonnull final StatRecordBuilder snapshotBuilder) {
            this.statRecordBuilder = Objects.requireNonNull(snapshotBuilder);
        }

        @Override
        public Stream<Builder> createStatSnapshots(
                @Nonnull final List<Record> statDBRecords,
                final boolean fullMarket,
                @Nonnull final List<CommodityRequest> commodityRequests) {
            // Process all the DB records grouped by, and ordered by, snapshot_time
            TreeMap<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity =
                    organizeStatsRecordsByTime(statDBRecords, commodityRequests);
            // For each snapshot_time, create a {@link StatSnapshot} and handle as it is constructed
            return statRecordsByTimeByCommodity.entrySet().stream().map(entry -> {
                final Timestamp timestamp = entry.getKey();
                final Multimap<String, Record> commodityMap = entry.getValue();
                Builder snapshotBuilder = StatSnapshot.newBuilder();
                snapshotBuilder.setSnapshotDate(timestamp.getTime());

                // process all the stats records for a given commodity for the current snapshot_time
                // - might be 1, many for group, or none if time range didn't overlap recorded stats
                commodityMap.asMap().values().forEach(dbStatRecordList -> {
                    final StatsAccumulator percentileUtilization = new StatsAccumulator();
                    dbStatRecordList.removeIf(r -> {
                        if (StringConstants.PROPERTY_SUBTYPE_PERCENTILE_UTILIZATION.equalsIgnoreCase(
                                r.getValue(PROPERTY_SUBTYPE, String.class))) {
                            final Double value = r.getValue(AVG_VALUE, Double.class);
                            if (value != null) {
                                percentileUtilization.record(value);
                            }
                            return true;
                        }
                        return false;
                    });
                    if (!dbStatRecordList.isEmpty()) {
                        // use the first element as the core of the group value
                        Record dbFirstStatRecord = dbStatRecordList.iterator().next();
                        final String propertyType = dbFirstStatRecord.getValue(PROPERTY_TYPE, String.class);
                        String propertySubtype = dbFirstStatRecord.getValue(PROPERTY_SUBTYPE, String.class);
                        String relation = dbFirstStatRecord.getValue(RELATION, String.class);

                        // In the full-market request we return aggregate stats with no commodity_key
                        // or producer_uuid.
                        final String commodityKey = fullMarket ? null :
                                dbFirstStatRecord.getValue(COMMODITY_KEY, String.class);
                        final String producerIdString = fullMarket ? null :
                                dbFirstStatRecord.getValue(PRODUCER_UUID, String.class);
                        final String relatedEntityType = dbFirstStatRecord.field(ENTITY_TYPE) != null
                            ? dbFirstStatRecord.getValue(ENTITY_TYPE, String.class)
                            : null;
                        final StatsAccumulator capacityValue = new StatsAccumulator();
                        final StatsAccumulator usageValue = new StatsAccumulator();
                        final StatsAccumulator effectiveCapacityValue = new StatsAccumulator();
                        Long producerId = null;
                        if (StringUtils.isNotEmpty(producerIdString)) {
                            producerId = Long.valueOf(producerIdString);
                        }

                        // calculate totals
                        for (Record dbStatRecord : dbStatRecordList) {
                            Float oneAvgValue = dbStatRecord.getValue(AVG_VALUE, Float.class);
                            Float oneMinValue = dbStatRecord.getValue(MIN_VALUE, Float.class);
                            Float oneMaxValue = dbStatRecord.getValue(MAX_VALUE, Float.class);

                            float avgValue = oneAvgValue == null ? 0 : oneAvgValue;
                            float minValue = oneMinValue == null ? avgValue : oneMinValue;
                            float maxValue = oneMaxValue == null ? avgValue : oneMaxValue;

                            usageValue.record(minValue, avgValue, maxValue);

                            Float oneCapacityValue = dbStatRecord.getValue(CAPACITY, Float.class);
                            if (oneCapacityValue != null) {
                                capacityValue.record(oneCapacityValue.doubleValue());

                                // effective capacity really only makes sense in the context of an
                                // actual capacity, so we'll handle it within the capacity clause.
                                Float oneEffectiveCapacityValue = dbStatRecord.getValue(EFFECTIVE_CAPACITY, Float.class);
                                // a null effective capacity should be treated as full "capacity".
                                if (oneEffectiveCapacityValue == null) {
                                    // add one full capacity to the effective capacity total.
                                    effectiveCapacityValue.record(oneCapacityValue);
                                } else { // o/w add the effective capacity.
                                    effectiveCapacityValue.record(oneEffectiveCapacityValue);
                                }
                            }
                        }

                        final StatRecord statRecord;
                        float reserved = (float)(capacityValue.getTotal()
                            - effectiveCapacityValue.getTotal());
                        StatValue usageStat = usageValue.toStatValue();
                        if (fullMarket) {
                            // For the full market, we don't divide the values by the number of
                            // records. This is because we know all records for this commodity at
                            // this time refer to the same "entity" (i.e. the market). The total
                            // value for this entity got "split" according to certain properties
                            // when we saved it, and we add up all the matching rows to "re-unite"
                            // it at query-time.
                            //
                            // For example, if a certain commodity "amtConsumed" has value 2 for
                            // environment_type ON_PREM and value 3 for environment_type CLOUD,
                            // then when we search for commodity "amtConsumed" with no environment
                            // type filter (i.e. we want the amount consumed across environments),
                            // the returned value should be 5 (total), not 2.5 (average)
                            //
                            // Note (roman, Feb 6 2019): It's not clear what the different expected
                            // records are here. It's possible that there are stats for which we
                            // DO want the amount to be averaged across records.
                            usageStat = usageStat.toBuilder()
                                    .setAvg(usageStat.getTotal())
                                    .setMax(usageStat.getTotalMax())
                                    .setMin(usageStat.getTotalMin())
                                    .build();
                        }
                        statRecord = statRecordBuilder.buildStatRecord(propertyType,
                            propertySubtype, capacityValue.toStatValue(), reserved,
                            relatedEntityType, producerId, usageStat,
                            commodityKey, relation, percentileUtilization.getCount() > 0 ?
                                        percentileUtilization.toStatValue() : null);

                        // return add this record to the snapshot for this timestamp
                        snapshotBuilder.addStatRecords(statRecord);
                    }
                });
                return snapshotBuilder;
            });
        }

        /**
         * Process a list of DB stats reecords and organize into a {@link TreeMap} ordered by Timestamp
         * and then commodity.
         * <p>
         * Note that properties bought and sold may have the same name, so we need to distinguish
         * those by appending PROPERTY_TYPE with PROPERTY_SUBTYPE for the key for the commodity map.
         *
         * Similarly, a single commodity may be sold by more than one entity type, and so the
         * entity_type field must also be included in the commodity map key. Note that this only
         * applies to market_stats_xxx rows, not individual entity stats rows.
         *
         * @param statDBRecords the list of DB stats records to organize
         * @param commodityRequests a list of {@link CommodityRequest} being satisfied in this query. We
         *                          will check if there is a groupBy parameter in the request and
         *                          implement it here, where we are aggregating results.
         *                          Currently XL supports following grouping by: <em>key</em>
         *                          (i.e. commodity keys), <em>relatedEntity</em> (producer uuid),
         *                          and <em>virtualDisk</em> (commodity key)
         * @return a map from each unique Timestamp to the map of properties to DB stats records for
         * that property and timestamp
         */
        private TreeMap<Timestamp, Multimap<String, Record>> organizeStatsRecordsByTime(
                @Nonnull final List<Record> statDBRecords,
                @Nonnull final List<CommodityRequest> commodityRequests) {
            // collect groupBy fields for different commodity type and convert to table field names
            final Map<String, Set<String>> commodityNameToGroupByFields = commodityRequests.stream()
                .filter(CommodityRequest::hasCommodityName)
                .collect(Collectors.toMap(CommodityRequest::getCommodityName,
                    commodityRequest -> commodityRequest.getGroupByList().stream()
                        .map(API_GROUP_BY_STR_TO_TABLE_FIELD::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet())
                ));

            // organize the statRecords by SNAPSHOT_TIME and then by PROPERTY_TYPE + PROPERTY_SUBTYPE
            TreeMap<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity = new TreeMap<>();
            for (Record dbStatRecord : statDBRecords) {
                // Filter out the utilization as we are interested in the used values
                String propertySubType = dbStatRecord.getValue(PROPERTY_SUBTYPE, String.class);
                if (UTILIZATION.equals(propertySubType)) {
                    continue;
                }

                Timestamp snapshotTime = dbStatRecord.getValue(SNAPSHOT_TIME, Timestamp.class);
                Multimap<String, Record> snapshotMap = statRecordsByTimeByCommodity.computeIfAbsent(
                        snapshotTime, k -> HashMultimap.create()
                );

                final String commodityName = dbStatRecord.getValue(PROPERTY_TYPE, String.class);
                final String groupByKey = commodityNameToGroupByFields.getOrDefault(commodityName,
                    Collections.emptySet()).stream()
                        .filter(fieldName -> dbStatRecord.field(fieldName) != null)
                        .map(fieldName -> dbStatRecord.getValue(fieldName, String.class))
                        .collect(Collectors.joining());

                // See the enum RelationType in com.vmturbo.history.db.
                // Commodities, CommoditiesBought, and CommoditiesFromAttributes
                // (e.g., priceIndex, numVCPUs, etc.)
                String relation = dbStatRecord.getValue(RELATION, String.class);

                // Need to separate commodity bought and sold as some commodities are both bought
                // and sold in the same entity, e.g., StorageAccess in Storage entity.
                String recordKey = commodityName + relation + groupByKey;

                // Need to separate commodities by entity_type if the field exists, as some
                // commodities (e.g. CPUAllocation) are sold by more than one entity type
                // (VirtualDataCenter and PhysicalMachine)
                if (dbStatRecord.field(ENTITY_TYPE) != null) {
                    recordKey += dbStatRecord.getValue(ENTITY_TYPE, String.class);
                }

                snapshotMap.put(recordKey, dbStatRecord);
            }
            return statRecordsByTimeByCommodity;
        }
    }

}
