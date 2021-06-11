/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.Collection;
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
     * Process a list of DB stats records and organize into a {@link TreeMap} ordered by Timestamp
     * and then a commodity key.
     *
     * <p>Note that properties bought and sold may have the same name, and a single commodity may be
     * sold by more than one entity type. The key to the stats record map must include attributes
     * to uniquely identify the record. See logic in {@link AbstractRecordsAggregator#createRecordKey}
     * for the logic that generates the key.
     * Note that this only applies to market_stats_xxx rows, not individual entity stats rows.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @param commodityRequests which contains information that manages aggregation process.
     * @return a map from each unique Timestamp to the map of properties to DB stats records
     *                 for that property and timestamp
     */
    private static Map<Timestamp, Multimap<String, Record>> organizeStatsRecordsByTime(
                    @Nonnull final Iterable<Record> statDBRecords,
                    @Nonnull final Collection<CommodityRequest> commodityRequests) {
        final Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity =
                        new TreeMap<>();
        final Map<Class<? extends Record>, RecordsAggregator<? extends Record>>
                        specificRecordAggregators =
                        createSpecificRecordAggregators(commodityRequests);
        final RecordsAggregator<? extends Record> defaultRecordsAggregator =
                            new StatsRecordsAggregator(commodityRequests, specificRecordAggregators
                                            .keySet());
        final Multimap<RecordsAggregator<?>, Record> specificAggregatorToRecords =
                        HashMultimap.create();
        for (Record record : statDBRecords) {
            final RecordsAggregator<? extends Record> specificRecordsAggregator =
                            specificRecordAggregators.get(record.getClass());
            if (specificRecordsAggregator != null) {
                specificAggregatorToRecords.put(specificRecordsAggregator, record);
            } else {
                aggregate(statRecordsByTimeByCommodity, record, defaultRecordsAggregator);
            }
        }
        for (Entry<RecordsAggregator<? extends Record>, Record> recordsAggregatorRecordEntry : specificAggregatorToRecords
                        .entries()) {
            aggregate(statRecordsByTimeByCommodity, recordsAggregatorRecordEntry.getValue(),
                            recordsAggregatorRecordEntry.getKey());
        }
        return statRecordsByTimeByCommodity;
    }

    private static Map<Class<? extends Record>, RecordsAggregator<? extends Record>> createSpecificRecordAggregators(
                    @Nonnull Collection<CommodityRequest> commodityRequests) {
        final RecordsAggregator<HistUtilizationRecord> histUtilizationRecordsAggregator =
                        new HistUtilizationRecordRecordsAggregator(commodityRequests);
        return ImmutableMap.of(HistUtilizationRecord.class, histUtilizationRecordsAggregator);
    }

    private static <T extends Record> void aggregate(
                    @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity,
                    T record,
                    @Nonnull RecordsAggregator<?> rawAggregator) {
        @SuppressWarnings("unchecked")
        final RecordsAggregator<T> aggregator = (RecordsAggregator<T>)rawAggregator;
        aggregator.aggregate(record, statRecordsByTimeByCommodity);
    }

}
