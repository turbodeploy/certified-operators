package com.vmturbo.cost.component.cleanup;

import java.time.Duration;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

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
         * @param trimTimeResolver Earliest time allowed in the table. All records before this time will be deleted.
         *
         * @throws DataAccessException A Data access exception
         */
        void trim(@Nonnull TrimTimeResolver trimTimeResolver) throws DataAccessException;
    }

    /**
     * Get the {@link Trimmer} to use for this table.
     *
     * @return The {@link Trimmer} used for writes to this table.
     */
    @Nonnull
    Trimmer writer();

    /**
     * The {@link TableCleanupInfo} for this cleanup task.
     * @return The {@link TableCleanupInfo}.
     */
    @Nonnull
    TableCleanupInfo tableInfo();

    /**
     * The {@link TrimTimeResolver} for this cleanup task.
     * @return The {@link TrimTimeResolver} for this cleanup task.
     */
    TrimTimeResolver trimTimeResolver();

    /**
     * Interface describing the table information.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface TableCleanupInfo {
        /**
         * The {@link Table}.
         *
         * @return The table.
         */
        Table<?> table();

        /**
         * The time field in the table, used to filter against for record deletion.
         *
         * @return The date and time of the record.
         */
        Field<LocalDateTime> timeField();

        /**
         * The short name of the table used for metrics.
         *
         * @return The short table name.
         */
        @Derived
        default String shortTableName() {
            return table().getName();
        }

        @Default
        default Duration cleanupRate() {
            return Duration.ofHours(1);
        }

        @Default
        default int retryLimit() {
            return 3;
        }

        @Default
        default Duration retryDelay() {
            return Duration.ofMinutes(5);
        }

        @Default
        default Duration longRunningDuration() {
            return Duration.ofMinutes(10);
        }

        @Default
        default int numRowsToBatchDelete() {
            return 1000;
        }

        @Default
        default boolean blockIngestionOnLongRunning() {
            return false;
        }

        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link TableCleanupInfo} instances.
         */
        class Builder extends ImmutableTableCleanupInfo.Builder {}
    }

    /**
     * A resolver of the trim time for a given table.
     */
    interface TrimTimeResolver {

        /**
         * Get the trim time of the table.
         *
         * @return A trim time representing the time for deletion of particular records prior.
         */
        @Nonnull
        LocalDateTime getTrimTime();
    }
}
