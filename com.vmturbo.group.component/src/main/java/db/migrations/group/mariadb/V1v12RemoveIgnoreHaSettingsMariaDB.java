package db.migrations.group.mariadb;

import java.sql.Connection;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import db.migrations.group.common.V1v12RemoveIgnoreHaSettings;

/**
 * Migration to go over the policy settings and remove all the ignoreHa settings.
 */
public class V1v12RemoveIgnoreHaSettingsMariaDB implements JdbcMigration, MigrationInfoProvider {

    /**
     * Create a new instance of the delegate migration and migrate.
     */
    @Override
    public void migrate(Connection connection) throws Exception {
        new V1v12RemoveIgnoreHaSettings().migrate(connection);
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("1.12");
    }

    @Override
    public String getDescription() {
        return "RemoveIgnoreHaSettings";
    }
}