/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * {@link StatsRecordsAggregator} aggregates records from stats tables, basing on API request
 * parameters.
 */
public class StatsRecordsAggregator extends AbstractRecordsAggregator<Record> {
    private final Collection<Class<? extends Record>> excludingRecordTypes;

    /**
     * Creates {@link StatsRecordsAggregator} instance.
     *
     * @param excludingRecordTypes types of DB records that need to be excluded from
     *                 stats processing.
     * @param commodityRequests which contains information that manages aggregation process.
     */
    public StatsRecordsAggregator(@Nonnull Collection<CommodityRequest> commodityRequests,
                    @Nonnull Collection<Class<? extends Record>> excludingRecordTypes) {
        super(commodityRequests);
        this.excludingRecordTypes = excludingRecordTypes;
    }

    @Override
    public void aggregate(@Nonnull Record record, @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity) {
        // Filter out the utilization as we are interested in the used values
        if (excludingRecordTypes.stream().anyMatch(type -> type.isInstance(record))) {
            return;
        }
        final Timestamp snapshotTime =
                        record.getValue(StringConstants.SNAPSHOT_TIME, Timestamp.class);
        final Multimap<String, Record> snapshotMap = statRecordsByTimeByCommodity
                        .computeIfAbsent(snapshotTime, k -> HashMultimap.create());
        snapshotMap.put(createRecordKey(record), record);
    }

    @Nonnull
    @Override
    protected String getRelation(@Nonnull Record record) {
        return record.getValue(StringConstants.RELATION, String.class);
    }

    @Nonnull
    @Override
    protected String getPropertyType(@Nonnull Record record) {
        return record.getValue(StringConstants.PROPERTY_TYPE, String.class);
    }
}
