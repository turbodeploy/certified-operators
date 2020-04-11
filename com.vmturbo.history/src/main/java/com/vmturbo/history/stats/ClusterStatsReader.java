package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.common.protobuf.utils.StringConstants.SAMPLES;
import static com.vmturbo.common.protobuf.utils.StringConstants.VALUE;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ClusterRecordsProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.TimeRange;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.ClusterTimeRangeFactory;

/**
 * This class retrieves records from cluster_stats_* tables to satisfy API requests.
 *
 * <p>A timeframe is computed from the start and end times, and that determines which table is
 * queried.</p>
 */
public class ClusterStatsReader {
    private final HistorydbIO historydbIO;
    private final ClusterTimeRangeFactory timeRangeFactory;
    private final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory;

    /**
     * Create a new instance.
     *
     * @param historydbIO                        Access to some DB utilities
     * @param timeRangeFactory                   an instance of ClusterTimeRangeFactory used to
     *                                           determine time frame for query results
     * @param computedPropertiesProcessorFactory factory for processors tohandle computed properties
     */
    ClusterStatsReader(HistorydbIO historydbIO, ClusterTimeRangeFactory timeRangeFactory,
            ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory) {
        this.historydbIO = historydbIO;
        this.timeRangeFactory = timeRangeFactory;
        this.computedPropertiesProcessorFactory = computedPropertiesProcessorFactory;
    }

    /**
     * Obtain records from the appropriate cluster stats table to satisfy an API request.
     *
     * <p>Records are returned in a wrapper that provides uniform access to record data regardless
     * of the specific cluster_stats_* table from which the records were retrieved.</p>
     *
     * @param clusterUuid   cluster ID
     * @param startTime     beginning of time range of interest
     * @param endTime       end of time range of interest
     * @param propertyTypes property types of interest
     * @return retrieved records, each wrapped in a {@link ClusterStatsRecordReader} instance.
     * @throws VmtDbException if retrieval fails
     */
    List<ClusterStatsRecordReader> getStatsRecords(long clusterUuid,
            long startTime, long endTime, Optional<TimeFrame> timeFrame,
            @Nonnull Set<String> propertyTypes) throws VmtDbException {
        Objects.requireNonNull(propertyTypes);
        StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startTime)
                .setEndDate(endTime)
                .addAllCommodityRequests(propertyTypes.stream()
                        .map(prop -> CommodityRequest.newBuilder().setCommodityName(prop).build())
                        .collect(Collectors.toList()))
                .build();
        final ComputedPropertiesProcessor computedPropertiesProcessor =
                computedPropertiesProcessorFactory.getProcessor(statsFilter, new ClusterRecordsProcessor());
        final StatsFilter augmentedFilter = computedPropertiesProcessor.getAugmentedFilter();
        Optional<TimeRange> timeRange = timeRangeFactory.resolveTimeRange(augmentedFilter,
                Optional.of(Collections.singletonList(Long.toString(clusterUuid))),
                Optional.empty(), Optional.empty(), timeFrame);
        if (timeRange.isPresent()) {
            final Set<String> augmentedPropertyTypes = augmentedFilter.getCommodityRequestsList().stream()
                    .map(CommodityRequest::getCommodityName)
                    .collect(Collectors.toSet());
            final Result<? extends Record> records = getStatsRecords(clusterUuid, augmentedPropertyTypes, timeRange.get());
            final Timestamp defaultTimestamp = timeRange.map(TimeRange::getMostRecentSnapshotTime).orElse(null);
            final List<Record> result = computedPropertiesProcessor.processResults(records, defaultTimestamp);
            return result.stream().map(ClusterStatsRecordReader::new).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Obtain records from the appropriate cluster stats table to satisfy an API request.
     *
     * <p>Records are returned in a wrapper that provides uniform access to record data regardless
     * of the specific cluster_stats_* table from which the records were retrieved.</p>
     *
     * @param clusterUuid   cluster id
     * @param propertyTypes property types of interest
     * @param timeRange     time range determined from start end end times
     * @return retrieved records, each wrapped in a {@link ClusterStatsRecordReader} instance
     * @throws VmtDbException if retrieval fails
     */
    Result<? extends Record> getStatsRecords(long clusterUuid, Set<String> propertyTypes, TimeRange timeRange)
            throws VmtDbException {
        Table<?> table = getStatsTable(timeRange.getTimeFrame());
        List<Condition> conditions = new ArrayList<>();
        conditions.add(getStringField(table, INTERNAL_NAME).eq(Long.toString(clusterUuid)));
        // Don't add inCommodityNames condition if commodityNames is empty.
        if (!propertyTypes.isEmpty()) {
            conditions.add(getStringField(table, PROPERTY_TYPE).in(propertyTypes));
        }
        conditions.add(getTimestampField(table, RECORDED_ON).between(
                new Timestamp(timeRange.getStartTime()), new Timestamp(timeRange.getEndTime())));

        final Result<? extends Record> results = historydbIO.execute(Style.FORCED,
                historydbIO.JooqBuilder().selectFrom(table)
                        .where(conditions).getQuery());
        return results;
    }

    /**
     * Get the cluster_stats table for the given time frame.
     *
     * @param timeFrame time frame
     * @return corresponding cluster_stats table
     */
    public static Table<?> getStatsTable(TimeFrame timeFrame) {
        switch (timeFrame) {
            case LATEST:
                return CLUSTER_STATS_LATEST;
            case HOUR:
                return CLUSTER_STATS_BY_HOUR;
            case DAY:
                return Tables.CLUSTER_STATS_BY_DAY;
            case MONTH:
                return Tables.CLUSTER_STATS_BY_MONTH;
            default:
                throw new IllegalArgumentException(
                        "Illegal time frame for cluster stats: " + timeFrame.name());
        }
    }

    /**
     * Wrapper for any of the various cluster_stats_* tables, providing uniform access to the
     * column values.
     */
    public static class ClusterStatsRecordReader {

        private final Record record;

        /**
         * Wrap a cluster stats record.
         *
         * @param record record to be wrapped
         */
        ClusterStatsRecordReader(Record record) {
            this.record = record;
        }

        /**
         * Get the id of the cluster to which this record applies.
         *
         * @return cluster id
         */
        public String getInternalName() {
            return record.get(INTERNAL_NAME, String.class);
        }

        /**
         * Get the timestamp for this record.
         *
         * @return record timestamp
         */
        public Timestamp getRecordedOn() {
            return record.get(RECORDED_ON, Timestamp.class);
        }

        /**
         * Get this record's property type.
         *
         * @return property type
         */
        public String getPropertyType() {
            return record.get(PROPERTY_TYPE, String.class);
        }

        /**
         * Get this record's property subtype.
         *
         * @return the property subtype
         */
        public String getPropertySubtype() {
            return record.get(PROPERTY_SUBTYPE, String.class);
        }

        /**
         * Get this record's value.
         *
         * @return the value
         */
        public Float getValue() {
            Double value = record.get(VALUE, Double.class);
            return value != null ? value.floatValue() : null;
        }

        /**
         * Get the number of samples averaged into this record's value.
         *
         * @return sample count
         */
        public Integer getSamples() {
            return record instanceof ClusterStatsLatestRecord ? 1
                    : record.get(SAMPLES, Integer.class);
        }
    }
}
