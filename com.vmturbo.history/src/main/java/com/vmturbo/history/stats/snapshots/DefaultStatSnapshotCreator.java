/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProducerIdPopulator;

/**
 * The default implementation of {@link StatSnapshotCreator}, for production use.
 */
public class DefaultStatSnapshotCreator implements StatSnapshotCreator {
    private static final Map<Class<? extends Record>, RecordsAggregator<?>>
                    RECORD_TYPE_TO_RECORD_AGGREGATOR = ImmutableMap.of(HistUtilizationRecord.class,
                    new HistUtilizationRecordRecordsAggregator());
    private static final RecordsAggregator<Record> DEFAULT_RECORDS_AGGREGATOR =
                    new StatsRecordsAggregator(RECORD_TYPE_TO_RECORD_AGGREGATOR.keySet());

    private final ProducerIdPopulator producerIdPopulator;

    /**
     * Creates {@link DefaultStatSnapshotCreator} instance.
     *
     * @param producerIdPopulator populator for producer identifier, doing set only
     *                 in case value have not been initialized
     */
    public DefaultStatSnapshotCreator(@Nonnull ProducerIdPopulator producerIdPopulator) {
        this.producerIdPopulator = producerIdPopulator;
    }

    @Nonnull
    @Override
    public Stream<Builder> createStatSnapshots(@Nonnull final List<Record> statDBRecords,
                    final boolean fullMarket,
                    @Nonnull final List<CommodityRequest> commodityRequests) {
        // Process all the DB records grouped by, and ordered by, snapshot_time
        final Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity =
                        organizeStatsRecordsByTime(statDBRecords, commodityRequests);
        // For each snapshot_time, create a {@link StatSnapshot} and handle as it is constructed
        return statRecordsByTimeByCommodity.entrySet().stream()
                        .map(new SnapshotCreator(fullMarket, producerIdPopulator));
    }

    /**
     * Process a list of DB stats reecords and organize into a {@link TreeMap} ordered by Timestamp
     * and then commodity.
     *
     * <p>Note that properties bought and sold may have the same name, so we need to distinguish
     * those by appending PROPERTY_TYPE with PROPERTY_SUBTYPE for the key for the commodity map.
     * Similarly, a single commodity may be sold by more than one entity type, and so the
     * entity_type field must also be included in the commodity map key. Note that this only applies
     * to market_stats_xxx rows, not individual entity stats rows.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @param commodityRequests a list of {@link CommodityRequest} being satisfied
     *                 in this query. We will check if there is a groupBy parameter in the request
     *                 and implement it here, where we are aggregating results. Currently XL
     *                 supports following grouping by: <em>key</em> (i.e. commodity keys),
     *                 <em>relatedEntity</em> (producer uuid), and <em>virtualDisk</em> (commodity
     *                 key)
     * @return a map from each unique Timestamp to the map of properties to DB stats records
     *                 for that property and timestamp
     */
    private static Map<Timestamp, Multimap<String, Record>> organizeStatsRecordsByTime(
                    @Nonnull final List<Record> statDBRecords,
                    @Nonnull final List<CommodityRequest> commodityRequests) {
        final Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity =
                        new TreeMap<>();
        final Multimap<RecordsAggregator<?>, Record> specificAggregatorToRecords =
                        HashMultimap.create();
        for (Record record : statDBRecords) {
            final RecordsAggregator<? extends Record> specificRecordsAggregator =
                            RECORD_TYPE_TO_RECORD_AGGREGATOR.get(record.getClass());
            if (specificRecordsAggregator != null) {
                specificAggregatorToRecords.put(specificRecordsAggregator, record);
            } else {
                DEFAULT_RECORDS_AGGREGATOR
                                .aggregate(record, commodityRequests, statRecordsByTimeByCommodity);
            }
        }
        for (Entry<RecordsAggregator<? extends Record>, Record> recordsAggregatorRecordEntry : specificAggregatorToRecords
                        .entries()) {
            aggregate(commodityRequests, statRecordsByTimeByCommodity,
                            recordsAggregatorRecordEntry);
        }
        return statRecordsByTimeByCommodity;
    }

    private static <T extends Record> void aggregate(
                    @Nonnull List<CommodityRequest> commodityRequests,
                    @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity,
                    @Nonnull Entry<RecordsAggregator<?>, Record> recordsAggregatorRecordEntry) {
        @SuppressWarnings("unchecked")
        final RecordsAggregator<T> aggregator =
                        (RecordsAggregator<T>)recordsAggregatorRecordEntry.getKey();
        @SuppressWarnings("unchecked")
        final T record = (T)recordsAggregatorRecordEntry.getValue();
        aggregator.aggregate(record, commodityRequests, statRecordsByTimeByCommodity);
    }

}
