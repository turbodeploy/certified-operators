package com.vmturbo.components.common.migration;

import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;

/**
 * Interface that the migrations have to implement.
 *
 * @deprecated in favor of the Flyway framework and its JdbcMigration interface.
 */
@Deprecated
public interface Migration {


    /**
     * Start the migration.
     *
     * @return {@link MigrationProgressInfo} describing the details
     * of the migration
     * @deprecated in favor of the Flyway framework and its JdbcMigration::migrate method.
     */
    @Deprecated
    MigrationProgressInfo startMigration();
}
