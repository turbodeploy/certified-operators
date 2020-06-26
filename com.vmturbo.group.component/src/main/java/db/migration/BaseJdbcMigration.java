package db.migration;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

/**
 * Implements base JDBC migration methods.
 */
public abstract class BaseJdbcMigration implements JdbcMigration {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Performs migration.
     *
     * @param connection connection to DB
     * @throws Exception error during migration
     */
    @Override
    public void migrate(Connection connection) throws Exception {
        boolean autoCommit = connection.getAutoCommit();
        try {
            performMigrationTasks(connection);
        } catch (Exception e) {
            logger.warn("Failed performing migration", e);
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    /**
     * Performs all migration tasks.
     *
     * @param connection connection to DB
     * @throws SQLException error during work with queries
     * @throws InvalidProtocolBufferException parsing data exception
     */
    protected abstract void performMigrationTasks(Connection connection)
            throws SQLException, InvalidProtocolBufferException;
}
