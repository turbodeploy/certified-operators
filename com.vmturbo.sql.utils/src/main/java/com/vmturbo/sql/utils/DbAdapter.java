package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class used by {@link DbEndpoint} to supply connections and/or perform provisioning operations.
 */
public abstract class DbAdapter {
    protected final Logger logger = LogManager.getLogger();

    protected final DbEndpoint config;

    protected DbAdapter(DbEndpoint config) {
        this.config = config;
    }

    static DbAdapter of(DbEndpoint config) throws UnsupportedDialectException {
        switch (config.getDialect()) {
            case MARIADB:
            case MYSQL:
                return new MariaDBMySqlAdapter(config);
            case POSTGRES:
                return new PostgresAdapter(config);
            default:
                throw new UnsupportedDialectException(config.getDialect());
        }
    }

    void init() throws UnsupportedDialectException, SQLException, InterruptedException {
        if (!componentCredentialsWork()) {
            logger.info("Setting up component database/schema and user");
            setupComponentCredentials();
        }
        if (config.isMigrationEnabled()) {
            performMigrations();
        }
    }

    DataSource getDataSource() throws UnsupportedDialectException, SQLException, InterruptedException {
        return getDataSource(config.getUrl(), config.getUserName(), config.getPassword());
    }

    abstract DataSource getDataSource(String url, String user, String password)
            throws UnsupportedDialectException, SQLException, InterruptedException;

    private boolean componentCredentialsWork() throws UnsupportedDialectException, InterruptedException {
        try (Connection conn = getNonRootConnection()) {
            return true;
        } catch (SQLException e) {
            logger.warn("Failed to obtain DB connection for user {}", config.getUserName(), e);
            return false;
        }
    }

    private void setupComponentCredentials() throws UnsupportedDialectException, SQLException, InterruptedException {
        createSchema();
        createNonRootUser();
        performNonRootGrants(config.getAccess());
    }

    private void performMigrations() throws UnsupportedDialectException, SQLException, InterruptedException {
        new FlywayMigrator(Duration.ofMinutes(1),
                Duration.ofSeconds(5),
                config.getSchemaName(),
                Optional.ofNullable(config.getMigrationLocation()),
                getDataSource(),
                config.getFlywayCallbacks()
        ).migrate();
    }

    protected abstract void createNonRootUser() throws SQLException, UnsupportedDialectException, InterruptedException;

    protected abstract void performNonRootGrants(DbEndpointAccess access)
            throws SQLException, UnsupportedDialectException, InterruptedException;

    Connection getNonRootConnection() throws UnsupportedDialectException, SQLException, InterruptedException {
        return getConnection(config.getUrl(), config.getUserName(), config.getPassword());
    }

    protected Connection getRootConnection(String database) throws UnsupportedDialectException, SQLException, InterruptedException {
        return getConnection(config.getUrl(database), config.getRootUserName(), config.getRootPassword());
    }

    Connection getConnection(String url, String user, String password)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return getDataSource(url, user, password).getConnection();
    }

    protected abstract void createSchema() throws SQLException, UnsupportedDialectException, InterruptedException;

    protected void execute(Connection conn, String sql) throws SQLException {
        logger.info("Executing SQL: {}", sql);
        conn.createStatement().execute(sql);
    }

    /**
     * Set up the retention policy for a table based on the retention parameters provided.
     *
     * @param table name of the table to set up retention policy
     * @param timeUnit unit of the retention period
     * @param retentionPeriod retention period
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     * @throws SQLException if there are DB problems
     */
    public void setupRetentionPolicy(String table, ChronoUnit timeUnit, int retentionPeriod)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        // do nothing by default
    }
}
