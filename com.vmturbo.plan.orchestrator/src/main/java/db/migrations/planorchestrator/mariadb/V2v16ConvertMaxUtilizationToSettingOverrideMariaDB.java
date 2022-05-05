package db.migrations.planorchestrator.mariadb;

import java.sql.Connection;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import db.migrations.planorchestrator.common.V2v16ConvertMaxUtilizationToSettingOverride;

/**
 * Migration to update all the maxUtilizationSettings in plan scenarios to setting overrides.
 */
public class V2v16ConvertMaxUtilizationToSettingOverrideMariaDB
        implements JdbcMigration, MigrationInfoProvider {

    /**
     * Create a new instance of the delegate migration and migrate.
     */
    @Override
    public void migrate(Connection connection) throws Exception {
        new V2v16ConvertMaxUtilizationToSettingOverride().migrate(connection);
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("2.16");
    }

    @Override
    public String getDescription() {
        return "convert max utilization to setting override";
    }
}
