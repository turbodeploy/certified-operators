package com.vmturbo.cost.component.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

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
public class V1_26__Callback extends BaseFlywayCallback {
    private static final Logger logger = LogManager.getLogger();

    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String V1_26_VERSION = "1.26";
    static final int V1_26_CORRECT_CHECKSUM = 546013560;
    private String dbName;


    static final String GET_DATABASE_QUERY = "SELECT DATABASE()";
    private static final String TABLE_EXISTS_QUERY =
        "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'";
    static final String GET_CHECKSUM_QUERY = String.format(
        "SELECT checksum FROM %s WHERE version=%s",
        FLYWAY_TABLE_NAME, V1_26_VERSION);
    static final String UPDATE_CHECKSUM_STMT = String.format(
        "UPDATE %s SET checksum = %d WHERE version='%s'",
        FLYWAY_TABLE_NAME, V1_26_CORRECT_CHECKSUM, V1_26_VERSION);

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            dbName = getDatabase(connection);
            if (needsFix(connection, dbName)) {
                logger.info("Applying fix for V1.26 migration");
                applyFix(connection);
            } else {
                logger.info("Fixing is not required for V1.26 migration.");
            }
        } catch (SQLException e) {
            logger.error("Failed to apply fix for V1.26 migration", e);
        }
    }

    /**
     * Check if the fix is needed.
     *
     * <p>We only need it if a V1.26 migration has been applied and if it's been recorded with the
     * incorrect checksum.</p>
     *
     * @param connection DB connection
     * @param dbName The database that we are working on.
     * @return true if the fix is needed
     * @throws SQLException if there's a problem any of our db operations
     */
    private boolean needsFix(Connection connection, String dbName) throws SQLException {
        // check to make sure the flyway version table exists
        return schemaTableExists(connection, dbName)
            && checksumIsIncorrect(connection);
    }

    private boolean schemaTableExists(final Connection connection, String dbName) throws SQLException {
        String query = String.format(TABLE_EXISTS_QUERY, dbName, FLYWAY_TABLE_NAME);
        try (ResultSet result = connection.createStatement().executeQuery(query)) {
            // if we got a row, the schema table exists
            return result.next();
        }
    }

    private boolean checksumIsIncorrect(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_CHECKSUM_QUERY)) {
            return result.next() &&
                result.getInt(1) != V1_26_CORRECT_CHECKSUM;
        }
    }

    private String getDatabase(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_DATABASE_QUERY)) {
            result.next();
            return result.getString(1);
        }
    }

    private void applyFix(Connection connection) throws SQLException {
        connection.createStatement().executeUpdate(UPDATE_CHECKSUM_STMT);
    }
}