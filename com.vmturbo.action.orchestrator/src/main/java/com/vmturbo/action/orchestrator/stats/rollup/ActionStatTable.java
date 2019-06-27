package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.TimeRange;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Common abstraction for a table containing action stats.
 * <p>
 * There are several very similar tables - latest, hour, day, and month - that we want to operate
 * on in very similar ways. This interface helps do that.
 */
public interface ActionStatTable {

    /**
     * A {@link Reader} should be used to query the stats in an action stat table and roll them up
     * into a {@link RolledUpActionStats} object.
     */
    interface Reader {

        /**
         * Query the stat records already saved in the database.
         *
         * @param timeRange The time range for the query.
         * @param mgmtUnitSubgroups The IDs of the target management unit subgroups.
         *                          If empty, there will be no results.
         * @param matchedActionGroups The target action groups.
         * @return An ordered list of {@link QueryResultsFromSnapshot}, one for each time that
         *         had matching results. Sorted in ascending order (by time).
         * @throws DataAccessException If there is an error interacting with the database.
         */
        @Nonnull
        List<QueryResultsFromSnapshot> query(
            @Nonnull final TimeRange timeRange,
            @Nonnull final Set<Integer> mgmtUnitSubgroups,
            @Nonnull final MatchedActionGroups matchedActionGroups)
                throws DataAccessException;

        /**
         * Roll-up the action stats in this table for a particular {@link MgmtUnitSubgroup} and
         * time range. Note that we always roll-up one time unit at a time (i.e. one hour in
         * the latest table, one day in the hourly table, one month in the daily table), so
         * we only need the start time to identify a time range.
         *
         * @param mgmtUnitSubgroupId The id of the {@link MgmtUnitSubgroup} to roll up.
         * @param startTime The start time for the time range. This should be appropriately
         *                  truncated, depending on the table we're rolling up to (e.g. truncated
         *                  to hour when reading from latest to roll up to hourly).
         * @return An {@link Optional} containing the {@link RolledUpActionStats}
         * @throws DataAccessException If there is an error interacting with the database.
         */
        @Nonnull
        Optional<RolledUpActionStats> rollup(final int mgmtUnitSubgroupId,
                         @Nonnull final LocalDateTime startTime) throws DataAccessException;

        /**
         * Get a list of times from this table which are "rollup-ready".
         * A time range is rollup-ready if there will be no more snapshots in the time frame.
         * For example, in the "latest" table time 17:00 becomes "rollup-ready" once there is
         * a snapshot with time greater-than-or-equal-to 18:00.
         *
         * These time-ranges can be passed into {@link Reader#rollup(int, LocalDateTime)}.
         *
         * @return A {@link RollupReadyInfo} describing the times ready for rollup.
         * @throws DataAccessException If there is an error interacting with the database.
         */
        @Nonnull
        List<RollupReadyInfo> rollupReadyTimes() throws DataAccessException;
    }

    /**
     * A {@link Writer} should be used to write rolled-up action stats from a "lower-level" table
     * (e.g. latest) to the "higher-level" table (e.g. hour).
     */
    interface Writer {

        /**
         * Insert a rolled-up stats record into the stat table this writer is for.
         *
         * @param mgmtUnitSubgroupId The id of the {@link MgmtUnitSubgroup} the record applies to.
         * @param summary The {@link RolledUpActionStats} to insert.
         * @throws DataAccessException If there is an error interacting with the database.
         */
        void insert(final int mgmtUnitSubgroupId,
                    @Nonnull final RolledUpActionStats summary) throws  DataAccessException;

        /**
         * Trim stats records from the stat table this writer is for.
         *
         * @param trimToTime The earliest time allowed in the table. Records for any earlier times
         *                   will be deleted.
         * @throws DataAccessException If there is an error interacting with the database.
         */
        void trim(@Nonnull final LocalDateTime trimToTime) throws DataAccessException;
    }

    /**
     * Get the {@link Reader} to use for this table.
     *
     * @return The {@link Reader} used for queries on this stat table.
     */
    Reader reader();

    /**
     * Get the {@link Writer} to use for this table.
     *
     * @return The {@link Writer} used for writes to this table.
     */
    Writer writer();

    /**
     * Determine whether or not the table needs to be trimmed in order to conform with
     * the input retention periods.
     *
     * @param retentionPeriods The {@link RetentionPeriods} specifying how long to retain
     *                         various stats data for.
     * @return If a trim is needed, an {@link Optional} containing the time to trim to (i.e.
     *         the earlies snapshot in the table that's within the retention period).
     *         If a trim is not needed, an empty {@link Optional}.
     */
    @Nonnull
    LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods);

    /**
     * Information about the underlying database table. The various time frames have very similar
     * table structures, and this {@link TableInfo} allows code to generically interact with all
     * of them.
     *
     * @param <STAT_RECORD> The type of {@link Record} in the action stat table.
     */
    @Value.Immutable
    interface TableInfo<STAT_RECORD extends Record, SNAPSHOT_RECORD extends Record> {
        /**
         * The stat table {@link Table}. This is the table that actually keeps the stats.
         */
        Table<STAT_RECORD> statTable();

        /**
         * The stat snapshot {@link Table}. This is the table that keeps information about the
         * available snapshots.
         */
        Table<SNAPSHOT_RECORD> snapshotTable();

        /**
         * The snapshot_time field in the stat table.
         */
        Field<LocalDateTime> statTableSnapshotTime();

        /**
         * The snapshot_time field in the snapshot table.
         */
        Field<LocalDateTime> snapshotTableSnapshotTime();

        /**
         * The mgmt_unit_subgroup_id field in the stat table.
         */
        Field<Integer> mgmtUnitSubgroupIdField();

        /**
         * The action_group_id field in the stat table.
         */
        Field<Integer> actionGroupIdField();

        /**
         * The function to truncate a {@link LocalDateTime} to this table's {@link TemporalUnit}.
         * Separate from the {@link TableInfo#temporalUnit()} property because "Month" needs
         * special truncate logic.
         */
        Function<LocalDateTime, LocalDateTime> timeTruncateFn();

        /**
         * The {@link TemporalUnit} for this table's time frame.
         */
        TemporalUnit temporalUnit();

        /**
         * The getter for the action_group_id field in the stat table record.
         */
        Function<STAT_RECORD, Integer> actionGroupIdExtractor();

        /**
         * The short name of the table used for metrics. Not necessarily the name of the underlying
         * table.
         */
        String shortTableName();

    }

    /**
     * The results for a query from a single "snapshot" - i.e. a single set of records with the
     * same time in the proper underlying stat table.
     */
    @Value.Immutable
    interface QueryResultsFromSnapshot {

        /**
         * The time of the record.
         */
        LocalDateTime time();

        /**
         * The number of "latest" action snapshots rolled up into this record.
         */
        Integer numActionSnapshots();

        /**
         * The stats matching the query at this time, arranged by {@link ActionGroup} and
         * {@link MgmtUnitSubgroup} ID.
         */
        Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroupAndMu();
    }

    /**
     * Information about a time range that's ready to be rolled up into the next table.
     */
    @Value.Immutable
    interface RollupReadyInfo {
        /**
         * The time range to roll up data over.
         */
        LocalDateTime startTime();

        /**
         * The management units that have data for the time range.
         */
        Set<Integer> managementUnits();
    }

    /**
     * Action stats for a particular {@link ActionGroup}, rolled up for a particular
     * {@link MgmtUnitSubgroup} over a time range.
     */
    @Value.Immutable
    interface RolledUpActionGroupStat {

        int       priorActionCount();
        int       newActionCount();
        double    avgActionCount();
        int       minActionCount();
        int       maxActionCount();

        double    avgEntityCount();
        int       minEntityCount();
        int       maxEntityCount();

        double    avgSavings();
        double    minSavings();
        double    maxSavings();

        double    avgInvestment();
        double    minInvestment();
        double    maxInvestment();
    }

    /**
     * Action stats for all {@link ActionGroup}s, rolled up for a particular time range and
     * {@link MgmtUnitSubgroup}.
     */
    @Value.Immutable
    interface RolledUpActionStats {

        LocalDateTime startTime();

        /**
         * The total number of action plan snapshots rolled up into the time unit starting at
         * start time. For example, suppose there are 3 snapshots per hour for a day.
         * When rolling up an hour worth of snapshots into the HOURLY table, this property
         * should be "3". When rolling up the whole day's worth of hourly snapshots into the
         * DAILY table, this property should be "3 * 24 = 72".
         */
        int numActionSnapshots();

        Map<Integer, RolledUpActionGroupStat> statsByActionGroupId();
    }
}
