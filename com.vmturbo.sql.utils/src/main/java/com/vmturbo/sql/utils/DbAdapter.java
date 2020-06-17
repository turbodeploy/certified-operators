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
        if (!componentCredentialsWork()) {
            logger.info("Setting up user {} for database {} / schema {}",
                    config.getDbUserName(), config.getDbDatabaseName(), config.getDbSchemaName());
            setupComponentCredentials();
        }
        if (!Strings.isNullOrEmpty(config.getDbMigrationLocations())) {
            performMigrations();
        }
    }

    DataSource getDataSource() throws UnsupportedDialectException, SQLException {
        return getDataSource(getUrl(config), config.getDbUserName(), config.getDbPassword());
    }

    abstract DataSource getDataSource(String url, String user, String password)
            throws SQLException;

    private boolean componentCredentialsWork() throws UnsupportedDialectException {
        try (Connection conn = getNonRootConnection()) {
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private void setupComponentCredentials() throws UnsupportedDialectException, SQLException {
        createSchema();
        createNonRootUser();
        performNonRootGrants(config.getDbAccess());
    }

    private void performMigrations() throws UnsupportedDialectException, SQLException {
        new FlywayMigrator(Duration.ofMinutes(1),
                Duration.ofSeconds(5),
                config.getDbSchemaName(),
                Optional.of(config.getDbMigrationLocations()).filter(s -> s.length() > 0),
                getDataSource(),
                config.getDbFlywayCallbacks()
        ).migrate();
    }

    protected abstract void createNonRootUser() throws SQLException, UnsupportedDialectException;

    protected abstract void performNonRootGrants(DbEndpointAccess access)
            throws SQLException, UnsupportedDialectException;

    Connection getNonRootConnection() throws UnsupportedDialectException, SQLException {
        return getConnection(getUrl(config), config.getDbUserName(), config.getDbPassword());
    }

    protected Connection getRootConnection(String database) throws UnsupportedDialectException, SQLException {
        return getConnection(getUrl(config, database), config.getDbRootUserName(), config.getDbRootPassword());
    }

    Connection getConnection(String url, String user, String password)
            throws SQLException {
        return getDataSource(url, user, password).getConnection();
    }

    protected abstract void createSchema() throws SQLException, UnsupportedDialectException;

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
        return getUrl(config, config.getDbDatabaseName());
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
        if (config.getDbSecure()) {
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
                .host(config.getDbHost())
                .port(config.getDbPort());
        config.getDbDriverProperties().forEach(builder::queryParam);
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
    private static String getJdbcProtocol(DbEndpointConfig config) throws UnsupportedDialectException {
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

    // TODO Move all these test-only methods to sql-test-utils modulek
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
            logger.error("Failed to dop database {}", config.getDbDatabaseName(), e);
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

    protected abstract void dropUserIfExists(Connection conn) throws SQLException;
}
