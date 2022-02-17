package com.vmturbo.cost.component.savings;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.rollup.RollupDurationType;

/**
 * Interface for read/write of entity savings/investments related hourly/daily/monthly statistics.
 *
 * @param <T> object used for transaction management (e.g. jooq DSLContext)
 */
public interface EntitySavingsStore<T> {

    /**
     * Stores records to hourly stats table.
     *
     * @param hourlyStat Set of stats to store. Each is of a particular stats type, has a timestamp,
     *  and the entityId that the stats belongs to.
     * @param transaction object used for transaction management
     * @throws EntitySavingsException Thrown on write error.
     */
    void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStat, T transaction)
            throws EntitySavingsException;

    /**
     * Stores records to daily stats table.
     *
     * @param dailyStats Set of stats to store. Each is of a particular stats type, has a timestamp,
     *  and the entityId that the stats belongs to.
     * @param transaction object used for transaction management
     * @throws EntitySavingsException Thrown on write error.
     */
    void addDailyStats(@Nonnull Set<EntitySavingsStats> dailyStats, T transaction)
            throws EntitySavingsException;

    /**
     * Gets stats related to savings and investments, based on the given time frame.
     *
     * @param timeFrame Info about what time range to fetch stats for.
     * @param statsTypes Type of stats to query for.
     * @param startTime Start time (epoch millis) to fetch stats for. Inclusive.
     * @param endTime End time (epoch millis) to fetch stats for. Exclusive.
     * @param entityOids OIDs of entities to fetch the stats for.
     * @param entityTypes entity types
     * @param resourceGroups resource groups
     * @return List of queried stats, in increasing order of timestamp.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    List<AggregatedSavingsStats> getSavingsStats(TimeFrame timeFrame,
            @Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException;

    /**
     * Returns raw (un-aggregated) savings stats records in the given time range.
     *
     * @param startTime Savings start time.
     * @param endTime Savings end time.
     * @return Stats record stream.
     * @throws EntitySavingsException Thrown on DB access exception.
     */
    @Nonnull
    Stream<EntitySavingsStatsRecord> getSavingsStats(long startTime, long endTime)
            throws EntitySavingsException;

    /**
     * Fetches hourly stats from store as per given inputs.
     *
     * @param statsTypes Type of stats to query for.
     * @param startTime Start time (epoch millis) to fetch stats for. Inclusive.
     * @param endTime End time (epoch millis) to fetch stats for. Exclusive.
     * @param entityOids OIDs of entities to fetch the stats for.
     * @param entityTypes entity types
     * @param resourceGroups resource group OIDs
     * @return List of queried stats, in increasing order of timestamp.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    List<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException;

    /**
     * Gets stats from rolled up daily stats table.
     *
     * @param statsTypes Type of stats to query for.
     * @param startTime Start time (epoch millis) to fetch stats for. Inclusive.
     * @param endTime End time (epoch millis) to fetch stats for. Exclusive.
     * @param entityOids OIDs of entities to fetch the stats for.
     * @param entityTypes entity types
     * @param resourceGroups resource groups
     * @return List of queried stats, in increasing order of timestamp.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    List<AggregatedSavingsStats> getDailyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException;

    /**
     * Gets stats from rolled up monthly stats table.
     *
     * @param statsTypes Type of stats to query for.
     * @param startTime Start time (epoch millis) to fetch stats for. Inclusive.
     * @param endTime End time (epoch millis) to fetch stats for. Exclusive.
     * @param entityOids OIDs of entities to fetch the stats for.
     * @param entityTypes entity types
     * @param resourceGroups resource groups
     * @return List of queried stats, in increasing order of timestamp.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    List<AggregatedSavingsStats> getMonthlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException;

    /**
     * Deletes stats records older than the given timestamp from the relevant hourly table.
     *
     * @param timestamp Min epoch millis for hourly stats table that will be kept.
     * @return Count of records deleted.
     */
    int deleteOlderThanHourly(long timestamp);

    /**
     * Deletes stats records older than the given timestamp from the relevant daily table.
     *
     * @param timestamp Min epoch millis for daily stats table that will be kept.
     * @return Count of records deleted.
     */
    int deleteOlderThanDaily(long timestamp);

    /**
     * Deletes stats records older than the given timestamp from the relevant monthly table.
     *
     * @param timestamp Min epoch millis for monthly stats table that will be kept.
     * @return Count of records deleted.
     */
    int deleteOlderThanMonthly(long timestamp);

    /**
     * Calls the rollup stored procedure.
     *
     * @param durationType whether this is a daily or monthly rollup
     * @param toTime       time of rollup record to be created/updated
     * @param fromTimes    times of source records to be rolled up
     */
    void performRollup(@Nonnull RollupDurationType durationType,
            long toTime, @Nonnull List<Long> fromTimes);

    /**
     * Delete all stats from the hourly, daily, and monthly tables.
     *
     * @param uuids list of UUIDs for which to delete stats.
     */
    void deleteStatsForUuids(@Nonnull Set<Long> uuids);
}
