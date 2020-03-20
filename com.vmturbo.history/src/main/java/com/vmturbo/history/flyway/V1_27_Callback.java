package com.vmturbo.history.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

/**
 * This deletes, if necessary, the Flyway-recorded checksum for the V1.27 migration.
 *
 * <p>Different migrations with this version number were created in different branches, and
 * both made it into release. To permit upgrades to a subsequently merged branch, we copied
 * both migrations to distinct later version numbers. We also must remove the records recording
 * their initial application to avoid flyway errors due to recorded migrations no longer being
 * discovered in the classpath.</p>
 */
@SuppressWarnings("checkstyle:TypeName")
public class V1_27_Callback extends BaseFlywayCallback {
    private static final Logger logger = LogManager.getLogger();

    private static final String GROUP_COMPONENT_SCHEMA = "group_component";
    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String V1_27_VERSION = "1.27";

    static final String TABLE_EXISTS_QUERY = String.format(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=database() " +
                    "AND table_name='%s'", FLYWAY_TABLE_NAME);
    static final String DELETE_ENTRY_SQL = String.format(
            "DELETE FROM %s WHERE version='%s'", FLYWAY_TABLE_NAME, V1_27_VERSION);

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            if (needsFix(connection)) {
                logger.info("Applying fix for V1.27 migration");
                applyFix(connection);
            }
        } catch (SQLException e) {
            logger.error("Failed to apply fix for V1.27 migration", e);
        }
    }

    /**
     * Check if the fix is needed.
     *
     * <p>We only need it if the flyway migrations table exists. We could also see if a V1.27
     * record exists, but doing so will be as expensive as just deleting it when it's not there,
     * so we don't bother checking</p>
     *
     * @param connection DB connection
     * @return true if the fix is needed
     * @throws SQLException if there's a problem any of our db operations
     */
    private boolean needsFix(Connection connection) throws SQLException {
        // check to make sure the flyway version table exists
        return schemaTableExists(connection);
    }

    private boolean schemaTableExists(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(TABLE_EXISTS_QUERY)) {
            // if we got a row, the schema table exists
            return result.next();
        }
    }

    private void applyFix(Connection connection) throws SQLException {
        connection.createStatement().executeUpdate(DELETE_ENTRY_SQL);
    }
}
