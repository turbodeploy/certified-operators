package db.migrations.history.mariadb;

import java.sql.Connection;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import db.migrations.history.common.V1v35v1InitializeAvailableHistoryTimesTable;

/**
 * Migration to populate newly-created (by migration V1.35) available_timestamps table, with existing
 * timestamps found in the corresponding stats tables.
 *
 * <p>This will will permit much more efficient execution of some frequently used queries.</p>
 *
 * <p>This is a copy of migration V1.27.1, created during the merge of 7.17 and 7.21 parallel
 * development branches in order to handle migration sequence gaps.</p>
 */
public class V1v35v1InitializeAvailableHistoryTimesTableMariaDB
        implements JdbcMigration, MigrationInfoProvider {

    /**
     * Create a new instance of the delegate migration and migrate.
     */
    @Override
    public void migrate(final Connection connection) throws Exception {
        new V1v35v1InitializeAvailableHistoryTimesTable().migrate(connection);
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("1.35.1");
    }

    @Override
    public String getDescription() {
        return "Initialize available history times table";
    }
}