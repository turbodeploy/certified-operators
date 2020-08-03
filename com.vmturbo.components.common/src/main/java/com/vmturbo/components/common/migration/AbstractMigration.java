package com.vmturbo.components.common.migration;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;

/**
 * Provides common migration functionality like status reporting.
 */
public abstract class AbstractMigration implements Migration {

    /**
     * For logging migration status.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Start the migration.
     *
     * @return {@link MigrationProgressInfo} describing the details
     * of the migration
     */
    @Override
    public final MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        return doStartMigration();
    }

    /**
     * Start the migration.
     *
     * <p>All subclasses must implement this method. This is where the core of the migration goes.</p>
     *
     * @return a {@link MigrationProgressInfo} describing the details of the migration
     */
    protected abstract MigrationProgressInfo doStartMigration();

    /**
     * Generate a migrationInfo indicating a successful migration.
     *
     * @return migrationInfo indicating a successful migration
     */
    @Nonnull
    protected MigrationProgressInfo migrationSucceeded() {
        return MigrationProgressInfo.newBuilder()
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.SUCCEEDED)
            .setCompletionPercentage(100)
            .build();
    }

    /**
     * Generate a migrationInfo indicating a failed migration.
     *
     * @param errorMessage a message indicating why the migration failed
     * @return migrationInfo indicating a failed migration
     */
    @Nonnull
    protected MigrationProgressInfo migrationFailed(@Nonnull String errorMessage) {
        return MigrationProgressInfo.newBuilder()
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.FAILED)
            .setStatusMessage("Migration failed: " + errorMessage)
            .build();
    }

    /**
     *  Update the migrationInfo and return a new MigrationProgressInfo
     *  with the updated status.
     *
     * @param status updated status of the migration in progress
     * @param completionPercentage a decimal value from 0.0 to 100.0
     * @param msg a message to associate with the current status
     * @return migrationInfo indicating the updated status of the migration
     */
    @Nonnull
    protected MigrationProgressInfo updateMigrationProgress(@Nonnull MigrationStatus status,
                                                            @Nonnull float completionPercentage,
                                                            @Nonnull String msg) {
            return MigrationProgressInfo.newBuilder()
                .setStatus(status)
                .setCompletionPercentage(completionPercentage)
                .setStatusMessage(msg)
                .build();
    }

}
