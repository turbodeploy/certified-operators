package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.StringConstants.AVG_VALUE;
import static com.vmturbo.history.schema.StringConstants.CAPACITY;
import static com.vmturbo.history.schema.StringConstants.COMMODITY_KEY;
import static com.vmturbo.history.schema.StringConstants.EFFECTIVE_CAPACITY;
import static com.vmturbo.history.schema.StringConstants.MAX_VALUE;
import static com.vmturbo.history.schema.StringConstants.MIN_VALUE;
import static com.vmturbo.history.schema.StringConstants.PRODUCER_UUID;
import static com.vmturbo.history.schema.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.history.schema.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.history.schema.StringConstants.RELATION;
import static com.vmturbo.history.schema.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.history.schema.StringConstants.UTILIZATION;

import java.sql.Timestamp;
import java.util.List;
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
import com.google.common.collect.Multimap;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.schema.StringConstants;

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
                snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(timestamp.getTime()));

                // process all the stats records for a given commodity for the current snapshot_time
                // - might be 1, many for group, or none if time range didn't overlap recorded stats
                commodityMap.asMap().values().forEach(dbStatRecordList -> {
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
                        final StatsAccumulator capacityValue = new StatsAccumulator();
                        Long producerId = null;
                        if (StringUtils.isNotEmpty(producerIdString)) {
                            producerId = Long.valueOf(producerIdString);
                        }
                        float avgTotal = 0.0f;
                        float minTotal = 0.0f;
                        float maxTotal = 0.0f;
                        final StatsAccumulator effectiveCapacityValue = new StatsAccumulator();

                        // calculate totals
                        for (Record dbStatRecord : dbStatRecordList) {
                            Float oneAvgValue = dbStatRecord.getValue(AVG_VALUE, Float.class);
                            if (oneAvgValue != null) {
                                avgTotal += oneAvgValue;
                            }
                            Float oneMinValue = dbStatRecord.getValue(MIN_VALUE, Float.class);
                            if (oneMinValue != null) {
                                minTotal += oneMinValue;
                            }
                            Float oneMaxValue = dbStatRecord.getValue(MAX_VALUE, Float.class);
                            if (oneMaxValue != null) {
                                maxTotal += oneMaxValue;
                            }
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

                        // calculate the averages
                        final int numStatRecords = dbStatRecordList.size();
                        float avgValueAvg = avgTotal / numStatRecords;
                        float minValueAvg = minTotal / numStatRecords;
                        float maxValueAvg = maxTotal / numStatRecords;

                        // calculate the "reserved" amount. This is the gap between capacity and
                        // "effective capacity".
                        float reserved = (float) (capacityValue.getAvg() - effectiveCapacityValue.getAvg());

                        // build the record for this stat (commodity type)
                        final StatRecord statRecord = statRecordBuilder.buildStatRecord(propertyType, propertySubtype,
                                capacityValue.toStatValue(), reserved, producerId, avgValueAvg, minValueAvg, maxValueAvg,
                                commodityKey, avgTotal, relation);

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
         * @param statDBRecords the list of DB stats records to organize
         * @param commodityRequests a list of {@link CommodityRequest} being satisfied in this query. We
         *                          will check if there is a groupBy parameter in the request and
         *                          implement it here, where we are aggregating results.
         *                          <b>NOTE:</b> XL only supports grouping by <em>key</em> (i.e.
         *                          commodity keys), but not <em>relatedEntity</em> or <em>virtualDisk</em>.
         *                          TODO: those options will be covered in OM-36453.
         * @return a map from each unique Timestamp to the map of properties to DB stats records for
         * that property and timestamp
         */
        private TreeMap<Timestamp, Multimap<String, Record>> organizeStatsRecordsByTime(
                @Nonnull final List<Record> statDBRecords,
                @Nonnull final List<CommodityRequest> commodityRequests) {
            // Figure out which stats are getting grouped by "key". This will be the set of commodity names
            // that have "key" in their groupBy field list.
            final Set<String> statsGroupedByKey = commodityRequests.stream()
                    .filter(CommodityRequest::hasCommodityName)
                    .filter(cr -> cr.getGroupByList().contains(StringConstants.KEY))
                    .map(CommodityRequest::getCommodityName)
                    .collect(Collectors.toSet());

            // organize the statRecords by SNAPSHOT_TIME and then by PROPERTY_TYPE + PROPERTY_SUBTYPE
            TreeMap<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity = new TreeMap<>();
            for (Record dbStatRecord : statDBRecords) {
                Timestamp snapshotTime = dbStatRecord.getValue(SNAPSHOT_TIME, Timestamp.class);
                Multimap<String, Record> snapshotMap = statRecordsByTimeByCommodity.computeIfAbsent(
                        snapshotTime, k -> HashMultimap.create()
                );
                String commodityName = dbStatRecord.getValue(PROPERTY_TYPE, String.class);
                // if this commodity is grouped by key, and the commodity key field is set, use the key
                // in the record key.
                String commodityKey = ((statsGroupedByKey.contains(commodityName)
                        && dbStatRecord.field(COMMODITY_KEY) != null ))
                        ? dbStatRecord.getValue(COMMODITY_KEY, String.class)
                        : "";
                String propertySubType = dbStatRecord.getValue(PROPERTY_SUBTYPE, String.class);
                // See the enum RelationType in com.vmturbo.history.db.
                // Commodities, CommoditiesBought, and CommoditiesFromAttributes
                // (e.g., priceIndex, numVCPUs, etc.)
                String relation = dbStatRecord.getValue(RELATION, String.class);

                // Need to separate commodity bought and sold as some commodities are both bought
                // and sold in the same entity, e.g., StorageAccess in Storage entity.
                String recordKey = commodityName + commodityKey + relation;

                // Filter out the utilization as we are interested in the used values
                if (!UTILIZATION.equals(propertySubType)) {
                    snapshotMap.put(recordKey, dbStatRecord);
                }
            }
            return statRecordsByTimeByCommodity;
        }
    }

}
