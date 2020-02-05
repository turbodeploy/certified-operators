package com.vmturbo.cost.component.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * This callback adjusts, if necessary, the Flyway-recorded checksum for the V1.26 migration.
 *
 * <p>The migration as originally could take a long time if the table associate to cost is large
 * . Therefore, there was a possibility that the component restarts in the middle of migration
 * and therefore run the migration halfway through. Since, the migration script was not
 * idempotent this results in broken migration which needed to be fixed manually.</p>
 *
 * <p>I have replaced that migration with a migration which is idempotent. Therefore, even if the
 * component restarts in the middle of migration the migration will not fail. However, this will
 * break the flyway checksum.
 * </p>
 *
 * <p>This class implements a pre-validation callback method that checks corrects the V1.26 checksum
 * if it is incorrect, thereby allowing validation - and migrations - to succeed.</p>
 */
@SuppressWarnings("checkstyle:TypeName")
public class CostFlywayCallback extends BaseFlywayCallback {
    private static final Logger logger = LogManager.getLogger();

    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String MIGRATION_CORRECTION_PATH = "migration_correction.yaml";
    private String dbName;

    static final String GET_DATABASE_QUERY = "SELECT DATABASE()";
    private static final String TABLE_EXISTS_QUERY =
        "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'";


    @Override
    public void beforeValidate(final Connection connection) {
        try {
            MigrationUpdates migrationUpdates = new Yaml().loadAs(getClass().getClassLoader()
                    .getResourceAsStream(MIGRATION_CORRECTION_PATH), MigrationUpdates.class);
            dbName = getDatabase(connection);
            migrationUpdates.getMigrationUpdates().forEach(migrationUpdate ->
                    validateAndMigrate(connection, migrationUpdate, dbName));
        } catch (SQLException e) {
            logger.error("Failed to make connection", e);
        } catch (YAMLException e) {
            logger.error("Migration yaml file is not valid. ", e);
        }
    }

    void validateAndMigrate(@Nonnull final Connection connection,
                            @Nonnull final MigrationUpdate migrationUpdate,
                            @Nonnull final String dbName) {
        String version = migrationUpdate.getVersion();
        try {
            if (needsFix(connection, dbName, migrationUpdate.getNewChecksum(), version)) {
                logger.info("Applying fix for {} migration", version);

                applyFix(connection, migrationUpdate, version);
            } else {
                logger.info("Fixing is not required for {} migration.", version);
            }
        } catch (SQLException e) {
            logger.error("Failed to apply fix for {} migration", version, e);
        }
    }

    /**
     * Check if the fix is needed.
     *
     * <p>We only need it if a V1.26 migration has been applied and if it's been recorded with the
     * incorrect checksum.</p>
     *
     * @param connection  DB connection
     * @param dbName      The database that we are working on.
     * @param newChecksum checksum to compare.
     * @param version     schema version in cost DB.
     * @return true if the fix is needed
     * @throws SQLException if there's a problem any of our db operations
     */
    private boolean needsFix(Connection connection, String dbName, final Long newChecksum, final String version) throws SQLException {
        // check to make sure the flyway version table exists
        return schemaTableExists(connection, dbName)
                && checksumIsIncorrect(connection, newChecksum, version);
    }

    private boolean schemaTableExists(final Connection connection, String dbName) throws SQLException {
        String query = String.format(TABLE_EXISTS_QUERY, dbName, FLYWAY_TABLE_NAME);
        try (ResultSet result = connection.createStatement().executeQuery(query)) {
            // if we got a row, the schema table exists
            return result.next();
        }
    }

    private boolean checksumIsIncorrect(final Connection connection, final Long checkSum, final String version) throws SQLException {
        final String getChecksumQuery = String.format(
                "SELECT checksum FROM %s WHERE version=%s",
                FLYWAY_TABLE_NAME, version);
        try (ResultSet result = connection.createStatement().executeQuery(getChecksumQuery)) {
            return result.next() &&
                result.getInt(1) != checkSum;
        }
    }

    private String getDatabase(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_DATABASE_QUERY)) {
            result.next();
            return result.getString(1);
        }
    }

    private void applyFix(@Nonnull Connection connection,
                          @Nonnull final MigrationUpdate migrationUpdate,
                          @Nonnull final String version)
            throws SQLException {
        //check if this migration sql failed.
        final String getSucessQuery = String.format(
                "SELECT success FROM %s WHERE version=%s",
                FLYWAY_TABLE_NAME, version);
        final boolean deleteMigration;
        try (ResultSet result = connection.createStatement().executeQuery(getSucessQuery)) {
            deleteMigration = result.next() &&
                    !result.getBoolean(1);
        }

        if (deleteMigration) {
            final String deleteMigrationRun = String.format(
                    "DELETE FROM %s WHERE version='%s'",
                    FLYWAY_TABLE_NAME, version);
            connection.createStatement().executeUpdate(deleteMigrationRun);
        } else {
            final String updateChecksumStmt = String.format(
                    "UPDATE %s SET checksum = %s WHERE version='%s'",
                    FLYWAY_TABLE_NAME, migrationUpdate.getNewChecksum(), version);
            connection.createStatement().executeUpdate(updateChecksumStmt);
        }
    }

    /**
     * Class to read migration updates array.
     */
    public static class MigrationUpdates {
        private List<MigrationUpdate> migrationUpdates;

        public List<MigrationUpdate> getMigrationUpdates() {
            return migrationUpdates;
        }

        public void setMigrationUpdates(final List<MigrationUpdate> migrationUpdates) {
            this.migrationUpdates = migrationUpdates;
        }
    }

    /**
     * Helper Class to read migration update.
     */
    public static class MigrationUpdate {
        private String version;
        private Long newChecksum;

        private String getVersion() {
            return version;
        }

        public void setVersion(final String version) {
            this.version = version;
        }

        private Long getNewChecksum() {
            return newChecksum;
        }

        public void setNewChecksum(final Long newChecksum) {
            this.newChecksum = newChecksum;
        }
    }
}