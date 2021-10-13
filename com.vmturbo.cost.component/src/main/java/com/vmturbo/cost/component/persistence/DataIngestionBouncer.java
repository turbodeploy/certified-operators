package com.vmturbo.cost.component.persistence;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.jooq.Table;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableCleanupInfo;
import com.vmturbo.cost.component.cleanup.CostTableCleanupManager;

/**
 * Determines whether ingestion is allowed for a given cost table. The bouncer is intended to be a
 * failsafe, preventing overloading the database and cleanup tasks whenever either show performance issues.
 * While blocking data ingestion may lead to a loss of data, this is preferable to the database becoming
 * overloaded and requiring manual intervention.
 */
public class DataIngestionBouncer {

    private final CostTableCleanupManager tableCleanupManager;

    private final DataIngestionConfig ingestionConfig;

    /**
     * Constructs a new {@link DataIngestionBouncer} instance.
     * @param tableCleanupManager The cost table cleanup manager.
     * @param ingestionConfig The data ingestion config.
     */
    public DataIngestionBouncer(@Nonnull CostTableCleanupManager tableCleanupManager,
                                @Nonnull DataIngestionConfig ingestionConfig) {
        this.tableCleanupManager = Objects.requireNonNull(tableCleanupManager);
        this.ingestionConfig = Objects.requireNonNull(ingestionConfig);
    }

    /**
     * Determines whether data ingestion for the provided {@code table} is allowed. Currently, data
     * ingestion is only blocked for select tables through {@link TableCleanupInfo#blockIngestionOnLongRunning()},
     * whenever a currently running cleanup task is determined to be long running.
     * @param table The target table.
     * @return True, if data ingestion is allowed. False, otherwise.
     */
    public boolean isTableIngestible(@Nonnull Table<?> table) {

        if (ingestionConfig.tableCleanupInfoMap().containsKey(table)
                && ingestionConfig.tableCleanupInfoMap().get(table).blockIngestionOnLongRunning()) {

            return !tableCleanupManager.isTableCleanupLongRunning(table);
        } else {
            return true;
        }
    }

    /**
     * The data ingestion config.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface DataIngestionConfig {

        /**
         * The table cleanup info list.
         * @return The table cleanup info list.
         */
        @Nonnull
        List<TableCleanupInfo> cleanupInfoList();

        /**
         * A map to of configured tables to the associated cleanup info.
         * @return An immutable map to of configured tables to the associated cleanup info.
         */
        @Derived
        default Map<Table<?>, TableCleanupInfo> tableCleanupInfoMap() {
            return cleanupInfoList().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            TableCleanupInfo::table,
                            Function.identity()));
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link DataIngestionConfig} instances.
         */
        class Builder extends ImmutableDataIngestionConfig.Builder {}
    }
}
