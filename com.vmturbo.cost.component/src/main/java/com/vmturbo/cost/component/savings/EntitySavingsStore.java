package com.vmturbo.cost.component.savings;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;

/**
 * Interface for read/write of entity savings/investments related hourly/daily/monthly statistics.
 */
public interface EntitySavingsStore {

    /**
     * Stores records to hourly stats table.
     *
     * @param hourlyStats Set of stats to store. Each is of a particular stats type, has a timestamp,
     *  and the entityId that the stats belongs to.
     * @throws EntitySavingsException Thrown on write error.
     */
    void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStats)
            throws EntitySavingsException;

    /**
     * Fetches hourly stats from store as per given inputs.
     *
     * @param statsTypes Type of stats to query for.
     * @param startTime Start time (epoch millis) to fetch stats for. Inclusive.
     * @param endTime End time (epoch millis) to fetch stats for. Exclusive.
     * @param entityIds Set of entity ids to get data for.
     * @return Set of queried stats.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    Set<EntitySavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime, @Nonnull Set<Long> entityIds)
            throws EntitySavingsException;
}
