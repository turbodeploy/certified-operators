package com.vmturbo.components.common.migration;

import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.common.Migration.MigrationInfo;

/**
 * Interface that the migrations have to implement.
 *
 */
public interface Migration {

    /**
     * Retrieve the current status of the migration.
     *
     * See {@link MigrationStatus} for the list of states.
     *
     * @return the current {@link MigrationStatus}
     */
    MigrationStatus getMigrationStatus();


    /**
     * Retrieve the current info about the migration.
     *
     * @return the current {@link MigrationInfo}
     */
    MigrationInfo getMigrationInfo();


    /**
     * Start the migration.
     *
     * @return {@link MigrationInfo } describing the details of the migration
     */
    MigrationInfo startMigration();
}
