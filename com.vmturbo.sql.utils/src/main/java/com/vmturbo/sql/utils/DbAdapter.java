package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Optional;

import javax.sql.DataSource;

import com.google.common.base.Strings;
import com.sun.istack.NotNull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class used by {@link DbEndpoint} to supply connections and/or perform provisioning operations.
 */
public abstract class DbAdapter {
    protected final Logger logger = LogManager.getLogger();

    protected final DbEndpointConfig config;

    protected DbAdapter(DbEndpointConfig config) {
        this.config = config;
    }

    static DbAdapter of(DbEndpointConfig config) throws UnsupportedDialectException {
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

    void init() throws UnsupportedDialectException, SQLException {
        if (componentCredentialsFail()) {
            logger.debug("Setting up user {} for database {} / schema {}",
                    config.getUserName(), config.getDatabaseName(), config.getSchemaName());
            // try to provision things if we can. If it doesn't work, the method will throw
            // an exception that should cause the retry loop to retry, in the hope tha it will
            // eventually work, or some other component will do it (we could fail because we don't
            // have provisioning authority configured)
            performProvisioning();
        }
        if (!Strings.isNullOrEmpty(config.getMigrationLocations())) {
            if (config.getShouldProvisionDatabase() && config.getAccess().isWriteAccess()) {
                // perform migrations if we have provisioning responsibilities
                performMigrations();
            } else {
                // validate if we don't, so we'll fail initialization and retries until the
                // responsible component completes migrations.
                validateMigrations();
            }
        }
    }

    DataSource getDataSource() throws UnsupportedDialectException, SQLException {
        return getDataSource(getUrl(config), config.getUserName(), config.getPassword());
    }

    abstract DataSource getDataSource(String url, String user, String password)
            throws SQLException;

    DataSource getRootDataSource() throws UnsupportedDialectException, SQLException {
        return getDataSource(getUrl(config), config.getRootUserName(), config.getRootPassword());
    }

    private boolean componentCredentialsFail() throws UnsupportedDialectException {
        try (Connection conn = getNonRootConnection()) {
            return false;
        } catch (SQLException e) {
            return true;
        }
    }

    private void performProvisioning() throws UnsupportedDialectException, SQLException {
        if (config.getShouldProvisionDatabase()) {
            createSchema();
            createReadersGroup();
        }
        if (config.getShouldProvisionUser()) {
            createNonRootUser();
            performNonRootGrants(config.getAccess());
        }
        // check whether things now work, and throw an exception if not
        if (componentCredentialsFail()) {
            throw new IllegalStateException("Credentialed access not yet available");
        }
    }

    private void performMigrations() throws UnsupportedDialectException, SQLException {
        if (!config.getMigrationLocations().isEmpty()) {
            new FlywayMigrator(Duration.ofMinutes(1),
                    Duration.ofSeconds(5),
                    config.getSchemaName(),
                    Optional.of(config.getMigrationLocations()).filter(s -> s.length() > 0),
                    getDataSource(),
                    config.getFlywayCallbacks()
            ).migrate();
        }
    }

    private void validateMigrations() throws UnsupportedDialectException, SQLException {
        if (!config.getMigrationLocations().isEmpty()) {
            new FlywayMigrator(Duration.ofMinutes(1),
                    Duration.ofSeconds(5),
                    config.getSchemaName(),
                    Optional.of(config.getMigrationLocations()).filter(s -> s.length() > 0),
                    getRootDataSource(),
                    config.getFlywayCallbacks()
            ).validate();
        }
    }

    protected abstract void createNonRootUser() throws SQLException, UnsupportedDialectException;

    protected abstract void performNonRootGrants(DbEndpointAccess access)
            throws SQLException, UnsupportedDialectException;

    Connection getNonRootConnection() throws UnsupportedDialectException, SQLException {
        return getConnection(getUrl(config), config.getUserName(), config.getPassword());
    }

    protected Connection getRootConnection(String database) throws UnsupportedDialectException, SQLException {
        return getConnection(getUrl(config, database), config.getRootUserName(), config.getRootPassword());
    }

    Connection getConnection(String url, String user, String password)
            throws SQLException {
        return getDataSource(url, user, password).getConnection();
    }

    protected abstract void createSchema() throws SQLException, UnsupportedDialectException;

    protected void createReadersGroup() throws UnsupportedDialectException, SQLException {}

    protected void execute(Connection conn, String sql) throws SQLException {
        logger.info("Executing SQL: {}", sql);
        conn.createStatement().execute(sql);
    }

    /**
     * Get a connection URL for this endpoint, connecting to the endpoint's configured database.
     *
     * @param config the endpoint config
     * @return connection URL
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public static String getUrl(DbEndpointConfig config) throws UnsupportedDialectException {
        return getUrl(config, config.getDatabaseName());
    }

    /**
     * Get a connection URL for this endpoint, connecting to the given database.
     *
     * @param config   the endpoint config
     * @param database name of database to connect to, or null to connect to server default
     *                 database
     * @return connection URL
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public static String getUrl(DbEndpointConfig config, String database) throws UnsupportedDialectException {
        UriComponentsBuilder builder = getUrlBuilder(config)
                .path(database != null ? "/" + database : "/");
        updateBuilderForSecureConnection(config, builder);
        return builder.build().toUriString();
    }

    private static void updateBuilderForSecureConnection(
            DbEndpointConfig config, UriComponentsBuilder builder) {
        if (config.getSecure()) {
            switch (config.getDialect()) {
                case MYSQL:
                case MARIADB:
                    builder.queryParam("trustServerCertificate", true);
                    break;
                case POSTGRES:
                    break;
            }
        }
    }

    @NotNull
    private static UriComponentsBuilder getUrlBuilder(DbEndpointConfig config) throws UnsupportedDialectException {
        final UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                .scheme("jdbc:" + getJdbcProtocol(config))
                .host(config.getHost())
                .port(config.getPort());
        config.getDriverProperties().forEach(builder::queryParam);
        return builder;
    }

    /**
     * Get the JDBC protocol string to use in connection URLs for the given endpoint, based on the
     * server type.
     *
     * @param config the endpoint config
     * @return protocol string
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
     static String getJdbcProtocol(DbEndpointConfig config) throws UnsupportedDialectException {
        switch (config.getDialect()) {
            case MARIADB:
            case MYSQL:
                return config.getDialect().getNameLC();
            case POSTGRES:
                return "postgresql";
            default:
                throw new UnsupportedDialectException(config.getDialect());
        }
    }

    /**
     * Set up the retention policy for a table based on the retention parameters provided.
     *
     * @param table           name of the table to set up retention policy
     * @param timeUnit        unit of the retention period
     * @param retentionPeriod retention period
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there are DB problems
     */
    public void setupRetentionPolicy(String table, ChronoUnit timeUnit, int retentionPeriod)
            throws UnsupportedDialectException, SQLException {
        // do nothing by default
    }

    // TODO Move all these test-only methods to sql-test-utils module
    /**
     * Truncate all tables in the database configured for this endpoint.
     *
     * <p>This is intended primarily for use in tests, to reset a test database to an initial
     * state prior to each test execution.</p>
     *
     * @throws UnsupportedDialectException If the endpoint is mis-configured
     * @throws SQLException                if a DB operation fails
     */
    public void truncateAllTables() throws UnsupportedDialectException, SQLException {
        try (Connection conn = getNonRootConnection()) {
            for (final String table : getAllTableNames(conn)) {
                conn.createStatement().execute(String.format("TRUNCATE TABLE \"%s\"", table));
            }
        }
    }

    protected abstract Collection<String> getAllTableNames(Connection conn) throws SQLException;

    /**
     * Drop the database configured for this endpoint.
     *
     * <p>This is intended primarily for use in tests, to remove a temporary database after
     * all tests in a test class have completed.</p>
     */
    public void dropDatabase() {
        try (Connection conn = getRootConnection(null)) {
            dropDatabaseIfExists(conn);
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop database {}", config.getDatabaseName(), e);
        }
    }

    protected abstract void dropDatabaseIfExists(Connection conn) throws SQLException;

    /**
     * Drop the non-root user associated with this endpoint.
     *
     * <p>This is intended primarily for use in tests, to remove a temporary user after all tests
     * in a test class have completed.</p>
     */
    public void dropUser() {
        try (Connection conn = getRootConnection(null)) {
            dropUserIfExists(conn);
        } catch (UnsupportedDialectException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Drop the readers user which acts as a group.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException if there are DB problems
     */
    public void dropReadersGroupUser() throws UnsupportedDialectException, SQLException {}

    protected abstract void dropUserIfExists(Connection conn) throws SQLException;
}
