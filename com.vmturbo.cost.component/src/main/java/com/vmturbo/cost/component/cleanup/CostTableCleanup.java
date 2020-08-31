package com.vmturbo.cost.component.cleanup;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

/**
 * Interface describing a Cost component table.
 */
public interface CostTableCleanup {

    /**
     * Interface describing a trimmer to delete records for a given table.
     */
    interface Trimmer {
        /**
         * Trims records from the table this writer is for.
         *
         * @param trimToTime Earliest time allowed in the table. All records before this time will be deleted.
         *
         * @throws DataAccessException A Data access exception
         */
        void trim(@Nonnull LocalDateTime trimToTime) throws DataAccessException;
    }

    /**
     * Get the trim time of the table.
     *
     * @return A trim time representing the time for deletion of particular records prior.
     */
    @Nonnull
    LocalDateTime getTrimTime();

    /**
     * Get the {@link Trimmer} to use for this table.
     *
     * @return The {@link Trimmer} used for writes to this table.
     */
    @Nonnull
    Trimmer writer();

    /**
     * The {@link TableInfo} for this cleanup task.
     * @return The {@link TableInfo}.
     */
    @Nonnull
    TableInfo tableInfo();

    /**
     * Interface describing the table information.
     */
    @Value.Immutable
    interface TableInfo {
        /**
         * The {@link Table}.
         *
         * @return The table.
         */
        Table table();

        /**
         * The time field in the table, used to filter against for record deletion.
         *
         * @return The date and time of the record.
         */
        Field<LocalDateTime> timeField();

        /**
         * The short name of the table used for metrics. Not necessarily the name of the underlying
         * table.
         *
         * @return The short table name.
         */
        String shortTableName();
    }
}
