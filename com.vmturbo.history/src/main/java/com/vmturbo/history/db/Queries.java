package com.vmturbo.history.db;

import java.sql.Timestamp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Query;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.queries.AvailableEntityTimestamps;
import com.vmturbo.history.db.queries.AvailableTimestamps;
import com.vmturbo.history.schema.HistoryVariety;

/**
 * This class simply provides static functions to build query objects from the various query classes.
 */
public class Queries {
    private Queries() {
    }

    /**
     * Build an {@link AvailableTimestamps} query, which retrieves available timestamps for a given variety of
     * history data in a given timeframe.
     *
     * @param timeFrame      timeframe (latest, hourly, etc.) for the query
     * @param historyVariety variety of history data (entity stats, market stats, etc.) for the query
     * @param limit          max # of results to return, or zero for no limit
     * @param fromInclusive  inclusive lower bound on results
     * @param toExclusive    exclusive upper bound on results
     * @return query to retrieve matching snapshots, in reverse chronological order
     */
    public static Query getAvailableSnapshotTimesQuery(@Nonnull TimeFrame timeFrame,
            @Nonnull HistoryVariety historyVariety,
            int limit,
            @Nullable Timestamp fromInclusive,
            @Nullable Timestamp toExclusive) {
        return new AvailableTimestamps(timeFrame, historyVariety, limit, fromInclusive, toExclusive)
                .getQuery();
    }

    /**
     * Build an {@link AvailableEntityTimestamps} query, which retrieves available entity stats snapshot_time values.
     *
     * @param timeFrame         required {@link TimeFrame}
     * @param entityType        required {@link EntityType}
     * @param entityOid         required entity OID (only if entityType is provided)
     * @param limit             max number of returned values, or 0 for no limit
     * @param fromInclusive     inclusive lower bound on returned timestamps
     * @param toExclusive       exclusive upper bound on returned timestamps
     * @param excludeProperties true if listed properties must not appear, else they must appear
     * @param propertyTypes     property types that must/must not appear in stats records considered
     * @return query to retrieve matching snapshot times in reverse chronological order
     */
    public static Query getAvailableEntityTimestampsQuery(@Nonnull TimeFrame timeFrame,
            @Nullable EntityType entityType,
            @Nullable String entityOid,
            int limit,
            @Nullable Timestamp fromInclusive,
            @Nullable Timestamp toExclusive,
            boolean excludeProperties,
            String... propertyTypes) {
        return new AvailableEntityTimestamps(timeFrame, entityType, entityOid, limit,
                fromInclusive, toExclusive, excludeProperties, propertyTypes)
                .getQuery();
    }
}
