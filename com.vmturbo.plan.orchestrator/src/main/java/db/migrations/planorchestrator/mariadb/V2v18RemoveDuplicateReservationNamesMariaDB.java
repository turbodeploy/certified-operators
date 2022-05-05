package db.migrations.planorchestrator.mariadb;

import java.sql.Connection;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

/**
 * remove the duplicate reservation name by suffixing it with __[1], __[2] ...
 * Also enforce unique constraint after de-duplication.
 */
public class V2v18RemoveDuplicateReservationNamesMariaDB
        implements JdbcMigration, MigrationInfoProvider {

    /**
     * Create a new instance of the delegate migration and migrate.
     */
    @Override
    public void migrate(Connection connection) throws Exception {
        new db.migrations.planorchestrator.common.V2v18RemoveDuplicateReservationNames().migrate(
                connection);
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("2.18");
    }

    @Override
    public String getDescription() {
        return "Remove duplicate reservation names";
    }
}