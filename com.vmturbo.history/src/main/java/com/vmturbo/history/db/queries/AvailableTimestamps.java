package com.vmturbo.history.db.queries;

import static com.vmturbo.history.schema.abstraction.tables.AvailableTimestamps.AVAILABLE_TIMESTAMPS;

import java.sql.Timestamp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.SortOrder;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.QueryBase;
import com.vmturbo.history.schema.HistoryVariety;

/**
 * This class creates a query to retrieve available timestamps for a given variety of history data in a given
 * timeframe, in reverse chronological order.
 */
public class AvailableTimestamps extends QueryBase {

    /**
     * Create a new query.
     *
     * @param timeFrame      timeframe (latest, hourly, etc.) for the query
     * @param historyVariety variety of history data (entity stats, market stats, etc.) for the query
     * @param limit          max # of results to return, or zero for no limit
     * @param fromInclusive  inclusive lower bound on results
     * @param toExclusive    exclusive upper bound on results
     */
    public AvailableTimestamps(@Nonnull TimeFrame timeFrame,
            @Nonnull HistoryVariety historyVariety,
            int limit,
            @Nullable Timestamp fromInclusive,
            @Nullable Timestamp toExclusive) {
        if (limit != 1) {
            // if we're getting more than one timestamp, omit duplicates
            setDistinct();
        }
        addSelectFields(AVAILABLE_TIMESTAMPS.TIME_STAMP);
        addTable(AVAILABLE_TIMESTAMPS);
        addConditions(AVAILABLE_TIMESTAMPS.TIME_FRAME.eq(timeFrame.name()));
        addConditions(AVAILABLE_TIMESTAMPS.HISTORY_VARIETY.eq(historyVariety.name()));
        addFieldRangeCondition(AVAILABLE_TIMESTAMPS.TIME_STAMP, fromInclusive, toExclusive);
        orderBy(AVAILABLE_TIMESTAMPS.TIME_STAMP, SortOrder.DESC);
        limit(limit);
    }
}
