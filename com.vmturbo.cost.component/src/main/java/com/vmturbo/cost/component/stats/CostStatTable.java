package com.vmturbo.cost.component.stats;

import java.time.LocalDateTime;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Interface describing the Cost Stats tables.
 */
public interface CostStatTable {
    /**
     * Interface describing a trimmer to delete records for a given stat table.
     */
    interface Trimmer {
        /**
         * Trim stats records from the stat table this writer is for.
         *
         * @param trimToTime Earliest time allowed int he table. All records before this time will be deleted.
         *
         * @throws DataAccessException A Data access exception
         */
        void trim(@Nonnull LocalDateTime trimToTime) throws DataAccessException;
    }

    /**
     * Get the trim time of a table based on the retention period.
     *
     * @param retentionPeriods The retention periods specified in the global defaults.
     *
     * @return A trim time representing the time for deletion of particular records prior.
     */
    @Nonnull
    LocalDateTime getTrimTime(@Nonnull RetentionPeriods retentionPeriods);

    /**
     * Get the {@link Trimmer} to use for this table.
     *
     * @return The {@link Trimmer} used for writes to this table.
     */
    Trimmer writer();

    /**
     * Interface describing the table information.
     */
    @Value.Immutable
    interface TableInfo {
        /**
         * The stat table {@link Table}. This is the table that actually keeps the stats.
         *
         * @return The table.
         */
        Table statTable();

        /**
         * The snapshot_time field in the stat table.
         *
         * @return The date and time fo the snapshot record.
         */
        Field<LocalDateTime> statTableSnapshotTime();

        /**
         * The short name of the table used for metrics. Not necessarily the name of the underlying
         * table.
         *
         * @return The short table name.
         */
        String shortTableName();

        /**
         * The function to truncate a {@link LocalDateTime} to this table's unit of time (hour, daily, monthly).
         *
         * @return The function.
         */
        Function<LocalDateTime, LocalDateTime> timeTruncateFn();
    }
}
