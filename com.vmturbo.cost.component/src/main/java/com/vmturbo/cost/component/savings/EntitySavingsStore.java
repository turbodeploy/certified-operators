package com.vmturbo.cost.component.savings;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.commons.TimeFrame;

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
     * Fetches hourly stats from store as per given inputs.
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
     * Get metadata about last time rollup was done.
     *
     * @return Last rollup times.
     */
    @Nonnull
    LastRollupTimes getLastRollupTimes();

    /**
     * Set last rollup times in DB.
     *
     * @param rollupTimes New rollup times.
     */
    void setLastRollupTimes(@Nonnull LastRollupTimes rollupTimes);

    /**
     * Calls the rollup stored procedure.
     *
     * @param rollupTimeInfo Info required to call rollup procedure.
     */
    void performRollup(@Nonnull RollupTimeInfo rollupTimeInfo);

    /**
     * Types of rollup done based on hourly stats table data.
     */
    enum RollupDurationType {
        /**
         * Default option, we write to hourly table directly.
         */
        HOURLY,

        /**
         * For rollup of hourly data to daily stats tables.
         */
        DAILY,

        /**
         * For rollup of daily data to monthly stats tables.
         */
        MONTHLY
    }

    /**
     * Util class to keep info about rollup (daily or monthly) to be done.
     */
    class RollupTimeInfo {
        /**
         * Whether rolling to daily or monthly.
         */
        private final RollupDurationType durationType;

        /**
         * Time rolling from, e.g hourly '2021-02-16 19:00:00' data being rolled over to daily.
         */
        private final long fromTime;

        /**
         * Time rolling to, e.g hourly data rolled up to day time '2021-02-16 00:00:00'.
         */
        private final long toTime;

        /**
         * Creates a new instance.
         *
         * @param durationType Type of duration - daily or monthly.
         * @param fromTime Source time from which rollup needs to be done.
         * @param toTime Target time to which rollup is to be done.
         */
        RollupTimeInfo(RollupDurationType durationType, long fromTime, long toTime) {
            this.durationType = durationType;
            this.fromTime = fromTime;
            this.toTime = toTime;
        }

        public boolean isDaily() {
            return durationType == RollupDurationType.DAILY;
        }

        public boolean isMonthly() {
            return durationType == RollupDurationType.MONTHLY;
        }

        public long fromTime() {
            return fromTime;
        }

        public long toTime() {
            return toTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RollupTimeInfo that = (RollupTimeInfo)o;
            return fromTime == that.fromTime && toTime == that.toTime
                    && durationType == that.durationType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(durationType, fromTime, toTime);
        }

        @Override
        public String toString() {
            return String.format("%s: %d -> %d", isDaily() ? "D" : "M", fromTime, toTime);
        }
    }

    /**
     * Contains info about last rollup times, read from aggregation metadata table.
     */
    class LastRollupTimes {
        /**
         * Name of the key field in metadata table.
         * Note: Don't change this name, same name gets set via sql as well.
         */
        static final String tableName = "entity_savings";

        /**
         * Last time rollup metadata was updated in DB, 0L if never set.
         */
        private long lastTimeUpdated;

        /**
         * Last hourly stats data that was processed and written to DB, 0L if never set.
         */
        private long lastTimeByHour;

        /**
         * Last day start time (12 AM) for which data was rolled up into monthly, 0L if never set.
         */
        private long lastTimeByDay;

        /**
         * Last month end time to which daily data was rolled up, 0L if never set.
         */
        private long lastTimeByMonth;

        LastRollupTimes() {
            this(0L, 0L, 0L, 0L);
        }

        LastRollupTimes(long lastUpdated, long lastHourly, long lastDaily, long lastMonthly) {
            lastTimeUpdated = lastUpdated;
            lastTimeByHour = lastHourly;
            lastTimeByDay = lastDaily;
            lastTimeByMonth = lastMonthly;
        }

        public static String getTableName() {
            return tableName;
        }

        void setLastTimeUpdated(long lastUpdated) {
            lastTimeUpdated = lastUpdated;
        }

        long getLastTimeUpdated() {
           return lastTimeUpdated;
        }

        void setLastTimeByHour(long lastHourly) {
            lastTimeByHour = lastHourly;
        }

        long getLastTimeByHour() {
            return lastTimeByHour;
        }

        boolean hasLastTimeByHour() {
            return lastTimeByHour != 0L;
        }

        void setLastTimeByDay(long lastDaily) {
            lastTimeByDay = lastDaily;
        }

        long getLastTimeByDay() {
            return lastTimeByDay;
        }

        boolean hasLastTimeByDay() {
            return lastTimeByDay != 0L;
        }

        void setLastTimeByMonth(long lastMonthly) {
            lastTimeByMonth = lastMonthly;
        }

        long getLastTimeByMonth() {
            return lastTimeByMonth;
        }

        boolean hasLastTimeByMonth() {
            return lastTimeByMonth != 0L;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            if (lastTimeUpdated > 0) {
                sb.append("Updated: ").append(lastTimeUpdated);
            }
            if (lastTimeByHour > 0) {
                sb.append(", LastHour: ").append(lastTimeByHour);
            }
            if (lastTimeByDay > 0) {
                sb.append(", LastDay: ").append(lastTimeByDay);
            }
            if (lastTimeByMonth > 0) {
                sb.append(", LastMonth: ").append(lastTimeByMonth);
            }
            return sb.toString();
        }
    }
}
