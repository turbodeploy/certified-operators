/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import org.jooq.Record;

/**
 * {@link RecordsAggregator} aggregate record into map by timestamp and record keys. Aggregation
 * based on API parameters taken from the request.
 *
 * @param <R> type of the record that is going to be aggregated.
 */
public interface RecordsAggregator<R extends Record> {

    /**
     * Aggregates record into statRecordsByTimeByCommodity map.
     *
     * @param record record that needs to be aggregated.
     * @param statRecordsByTimeByCommodity map with aggregation results.
     */
    void aggregate(@Nonnull R record,
                    @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity);
}
