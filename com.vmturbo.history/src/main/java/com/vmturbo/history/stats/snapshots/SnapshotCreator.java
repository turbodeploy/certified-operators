/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.history.stats.StatsConfig;

/**
 * {@link SnapshotCreator} creates one {@link StatSnapshot.Builder} instance from timestamp and
 * grouped DB records corresponding to that timestamp.
 */
public class SnapshotCreator
                implements Function<Entry<Timestamp, Multimap<String, Record>>, Builder> {
    private static final BiFunction<Record, String, String> BI_FUNCTION_IDENTITY = (record, s) -> s;
    private final Function<Collection<Record>, StatRecord> dbRecordsProcessor;

    /**
     * Creates {@link SnapshotCreator} instance.
     *
     * @param fullMarket whether we want to get stat record about full market or
     *                 not.
     * @param longProducerIdPopulator populator for producer identifier,
     *                 doing set only in case value have not been initialized
     */
    public SnapshotCreator(boolean fullMarket,
                    @Nonnull SharedPropertyPopulator<Long> longProducerIdPopulator) {
        final Collection<RecordVisitor<?>> statsVisitors =
                createRequestStatsVisitors(fullMarket, longProducerIdPopulator);
        final Multimap<Class<? extends Record>, RecordVisitor<?>> specialVisitors =
                createRequestSpecialVisitors(fullMarket, longProducerIdPopulator);
        this.dbRecordsProcessor = new DbRecordsProcessor(statsVisitors, specialVisitors);
    }

    private static Collection<RecordVisitor<?>> createRequestStatsVisitors(boolean fullMarket,
            @Nonnull SharedPropertyPopulator<Long> producerIdPopulator) {
        final ImmutableCollection.Builder<RecordVisitor<?>> builder = ImmutableList.builder();
        builder.add(new UsageRecordVisitor(fullMarket, StatsConfig.USAGE_POPULATOR),
                        new PropertyTypeVisitor<>(fullMarket, StringConstants.PROPERTY_TYPE,
                                        String.class, Function.identity(),
                                        StatsConfig.PROPERTY_TYPE_POPULATOR),
                        new ProducerIdVisitor(fullMarket, StringConstants.PRODUCER_UUID,
                                        producerIdPopulator),
                        new BasePropertyVisitor<>(StringConstants.ENTITY_TYPE, BI_FUNCTION_IDENTITY,
                                        StatsConfig.RELATED_ENTITY_TYPE_POPULATOR, String.class),
                        new BasePropertyVisitor<>(StringConstants.RELATION, BI_FUNCTION_IDENTITY,
                                        StatsConfig.RELATION_POPULATOR, String.class),
                        new CapacityRecordVisitor(StatsConfig.CAPACITY_POPULATOR));
        return builder.build();
    }

    private static Multimap<Class<? extends Record>, RecordVisitor<?>> createRequestSpecialVisitors(
            boolean fullMarket, @Nonnull SharedPropertyPopulator<Long> producerIdPopulator) {
        final ImmutableMultimap.Builder<Class<? extends Record>, RecordVisitor<?>> builder =
                        ImmutableMultimap.builder();
        builder.putAll(HistUtilizationRecord.class, Arrays.stream(HistoryUtilizationType.values())
                        .map(HistUtilizationRecordVisitor::new).collect(Collectors.toSet()));
        builder.putAll(HistUtilizationRecord.class, new PropertyTypeVisitor<>(fullMarket,
                        HistUtilization.HIST_UTILIZATION.PROPERTY_TYPE_ID.getName(), Long.class,
                        dbValue -> dbValue == null ?
                                        null :
                                        UICommodityType.fromType(dbValue.intValue()).apiStr(),
                        StatsConfig.PROPERTY_TYPE_POPULATOR), new ProducerIdVisitor(fullMarket,
                        HistUtilization.HIST_UTILIZATION.PRODUCER_OID.getName(),
                        producerIdPopulator));
        builder.put(HistUtilizationRecord.class,
                new CapacityRecordVisitor(StatsConfig.CAPACITY_POPULATOR));
        return builder.build();
    }

    @Override
    public StatSnapshot.Builder apply(Entry<Timestamp, Multimap<String, Record>> entry) {
        final Timestamp timestamp = entry.getKey();
        final Multimap<String, Record> commodityMap = entry.getValue();
        final Builder snapshotBuilder = StatSnapshot.newBuilder();
        snapshotBuilder.setSnapshotDate(timestamp.getTime());
        // process all the records for a given commodity for the current snapshot_time
        // - might be 1, many for group, or none if time range didn't overlap recorded stats
        final List<StatRecord> statRecords =
                        commodityMap.asMap().values().stream().map(dbRecordsProcessor)
                                        .collect(Collectors.toList());
        snapshotBuilder.addAllStatRecords(statRecords);
        return snapshotBuilder;
    }

    /**
     * {@link DbRecordsProcessor} process DB records that should be converted into a single {@link
     * StatRecord} instance.
     */
    private static class DbRecordsProcessor implements Function<Collection<Record>, StatRecord> {
        private final Collection<RecordVisitor<?>> statsVisitors;
        private final Multimap<Class<? extends Record>, RecordVisitor<?>>
                        recordTypeToSpecialVisitors;

        private DbRecordsProcessor(Collection<RecordVisitor<?>> statsVisitors,
                        Multimap<Class<? extends Record>, RecordVisitor<?>> recordTypeToSpecialVisitors) {
            this.statsVisitors = statsVisitors;
            this.recordTypeToSpecialVisitors = recordTypeToSpecialVisitors;
        }

        @Override
        public StatRecord apply(Collection<Record> records) {
            final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder();
            final Collection<RecordVisitor<?>> passedVisitors = new HashSet<>();
            records.forEach(record -> {
                final Collection<RecordVisitor<? extends Record>> specialVisitors =
                                recordTypeToSpecialVisitors.get(record.getClass());
                final Collection<RecordVisitor<? extends Record>> currentVisitors =
                                specialVisitors.isEmpty() ? statsVisitors : specialVisitors;
                currentVisitors.forEach(visitor -> visit(record, visitor));
                passedVisitors.addAll(currentVisitors);
            });
            passedVisitors.forEach(visitor -> visitor.build(statRecordBuilder));
            return statRecordBuilder.build();
        }

        private static <R extends Record> void visit(R record,
                        RecordVisitor<? extends Record> rawVisitor) {
            @SuppressWarnings("unchecked")
            final RecordVisitor<R> typedVisitor = (RecordVisitor<R>)rawVisitor;
            typedVisitor.visit(record);
        }

    }
}
