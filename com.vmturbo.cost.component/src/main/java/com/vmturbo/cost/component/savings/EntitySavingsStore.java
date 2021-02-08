package com.vmturbo.cost.component.savings;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.MultiValuedMap;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
     * @param entitiesByType EntityType to a set of entities of that type to scope by.
     * @return Set of queried stats.
     * @throws EntitySavingsException Thrown on storage error.
     */
    @Nonnull
    Set<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException;

    /**
     * Gets the timestamp in milliseconds of the last stats record. (i.e. the max timestamp)
     *
     * @return timestamp in milliseconds of the last record, null if table is empty.
     */
    @Nullable
    Long getMaxStatsTime();
}
