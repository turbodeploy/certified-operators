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
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointException;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class used by {@link DbEndpoint} to perform various database operations that may be needed for
 * the application to use the endpoint. These include:
 * <ul>
 *     <li>
 *         Supplying connections for the endpoint.
 *     </li>
 *     <li>
 *         Performing provisioning operations.
 *     </li>
 *     <li>
 *         Executing or verifying migrations.
 *     </li>
 * </ul>
 *
 * <p>Provisioning encompasses the creation and configuration of the database, schema, and/or user
 * associated with the endpoint, including arranging for the users to have proper privileges
 * for working with objects belonging to the schema.</p>
 *
 * <p>The provisioning operations are executed every time the component starts up, so they should be
 * "idempotent" in the sense that they change nothing except on first execution, if executed
 * repeatedly. Repeating these operations on every component restart means that we can recover from
 * many scenarios where the state of any database, schema or user goes amiss for any reason, e.g.
 * through incorrect manually applied changes or buggy code.</p>
 *
 * <p>Provisioning should avoid all manner of "destructive" operation - generally meaning dropping
 * of just about anything. Such operations are never necessary for provisioning, and some cases they
 * can result in catastrophic data loss. If operations are failing due to mis-configured db objects,
 * this should be noted in logs and investigated manually.</p>
 *
 * <p>For any given endpoint, provisioning of its database/schema or it user can be disabled through
 * endpoint configuration. In such cases, the affected db objects should never be provisioned by the
 * adapter, and if they are missing or incorrectly configured, manual intervention will be required.
 * </p>
 *
 * <p>Some provisioning operations are performed by a "root" or "super" user login, whose
 * credenetials must be supplied via endpoint configuration. Others are performed by the user
 * associated with an endpoint configured for `ALL` access after it has been provisioned. The
 * main reason to disable provisioning is to avoid the need to make a root user available to
 * the Turbonomic appliance.</p>
 *
 * <p>Migrations are handled in either of two ways, depending on the access configured for the
 * endpoint:</p>
 *
 * <ul>
 *     <li>
 *         For an endpoint with `ALL` access, migrations are executed. Flyway will attempt to
 *         perform any migrations that were not performed previously, and will succeed if there are
 *         none or if they all succeed individually. Generally, if any migration fails, manual
 *         intervention will be required in order for the component to operate correctly.
 *     </li>
 *     <li>
 *         For `READ_ONLY` only access, migrations are verified but not executed. Flyway will simply
 *         check that each migration is recorded as having succeeded at some time in the past.
 *         Failure will cause endpoint initialization to fail, which will normally cause the retry to
 *         start, and another if another component performs missing migrations, this endpoint should
 *         pass verification on the subsequent retry and thereby complete endpoint initialization.
 *     </li>
 * </ul>
 */
public abstract class DbAdapter {
    protected final Logger logger = LogManager.getLogger();

    protected final DbEndpointConfig config;

    protected DbAdapter(DbEndpointConfig config) {
        this.config = config;
    }

    /**
     * Create an adapter instance for an endpoint, based on the endpoint's dialect.
     *
     * @param config fully resolved {@link DbEndpointConfig} object for the endpoint
     * @return new adapter
     * @throws UnsupportedDialectException for an unsupported dialect
     */
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

    /**
     * Perform initialization operations, including provisioning and either execution or
     * verification of migrations.
     *
     * @throws DbEndpointException if initialization fails for any reason
     */
    void init() throws DbEndpointException {
        try {
            // perform provisioning where this endpoint has responsibility
            if (config.getShouldProvisionDatabase()) {
                createSchema();
                createReadersGroup();
                createWritersGroup();
            }
            if (config.getShouldProvisionUser()) {
                createNonRootUser();
                performNonRootGrants();
            }
            // check to make sure that the endpoint user can now connect to the database
            try {
                final Connection ignored = getNonRootConnection();
            } catch (SQLException e) {
                final String msg = String.format(
                        "Failed to establish test connection after provisioning endpoint %s", config);
                throw new DbEndpointException(msg, e);

            }
            // perform or verify migrations, if any are configured
            if (!Strings.isNullOrEmpty(config.getMigrationLocations())) {
                if (config.getAccess() == DbEndpointAccess.ALL) {
                    // perform migrations if we can create objects in the schema
                    performMigrations();
                } else {
                    // validate if we can't, so we'll fail initialization and retries until the
                    // responsible component completes migrations.
                    validateMigrations();
                }
            }
        } catch (Exception e) {
            throw new DbEndpointException("Endpoint %s failed initialization", e);
        }
    }

    /**
     * Create a {@link DataSource} object for this endpoint, using non-root credentials.
     *
     * @return new datasource object
     * @throws UnsupportedDialectException for an unsupported endpoint dialect
     * @throws SQLException                if the datasource cannot be created
     */
    DataSource getDataSource() throws UnsupportedDialectException, SQLException {
        return getDataSource(getUrl(config), config.getUserName(), config.getPassword());
    }

    /**
     * Create a new {@link DataSource} for the given DB url and credentials.
     *
     * @param url      URL for DB server
     * @param user     login user name
     * @param password login password
     * @return new datasource
     * @throws SQLException if the datasource cannot be created
     */
    abstract DataSource getDataSource(String url, String user, String password)
            throws SQLException;

    /**
     * Get a new {@link DataSource} object for this endpoint, suitable for use with flyway.
     *
     * <p>The `{@link PostgresAdapter} class overrides this to work around a flyway bug; see its
     * javadoc for details. Other adapters currently just use the normal datasource method.</p>
     *
     * @return data source
     * @throws UnsupportedDialectException for an unsuppported dialect
     * @throws SQLException                if there's a problem creating the data source
     */
    protected DataSource getDataSourceForFlyway() throws UnsupportedDialectException, SQLException {
        return getDataSource();
    }

    /**
     * Create a datasource for this endpoint using root credentials.
     *
     * @param database database to connect to, or null ot use endpoint's configured database
     * @return new datasource object, if the endpoint has root access enabled
     * @throws UnsupportedDialectException   for an unsupported dialect
     * @throws UnsupportedOperationException if this endpoint is not configured forroot access
     * @throws SQLException                  if the datasource could not be created
     */
    DataSource getRootDataSource(String database)
            throws UnsupportedDialectException, UnsupportedOperationException, SQLException {
        if (config.isRootAccessEnabled()) {
            return getDataSource(getUrl(config, database),
                    config.getRootUserName(), config.getRootPassword());
        } else {
            throw new UnsupportedOperationException(
                    String.format("DbEndpoint %s is not enabled for root access", this));
        }

    }

    /**
     * Create a {@link Connection} to this endpoint's database, using the non-root user
     * credentials.
     *
     * @return new connection
     * @throws UnsupportedDialectException for an unsupported dialect
     * @throws SQLException                if the connection could not be created
     */
    Connection getNonRootConnection() throws UnsupportedDialectException, SQLException {
        final Connection conn = getDataSource().getConnection();
        try {
            setConnectionUser(conn, config.getUserName());
        } catch (SQLException ignored) {
            // only for logging, so we swallow any exception
        }

        return conn;
    }

    /**
     * Create a {@link Connection} to this endpoint's database, using the root-user credentials, if
     * this endpoint is configured to use root credentials.
     *
     * @return new connection, or null if this endpoint is not enabled for root access
     * @throws UnsupportedDialectException   for an unsupported dialect
     * @throws UnsupportedOperationException if this endpoint is not configured for root access
     * @throws SQLException                  if the connection could not be created
     */
    protected Connection getRootConnection()
            throws UnsupportedDialectException, UnsupportedOperationException, SQLException {
        final Connection conn = getRootDataSource(config.getDatabaseName()).getConnection();
        setConnectionUser(conn, config.getRootUserName());
        return conn;
    }

    /**
     * Create a {@link Connection} to the given database, using the root-user credentials, if
     * this endpoint is configured to use root credentials.
     *
     * <p>Passing `null` for `databaseName` means to create a connection without designating
     * a database, i.e. connect to the server's default database.</p>
     *
     * @param databaseName name of database to connect to, or null for server default
     * @return new connection, or null if this endpoint is not enabled for root access
     * @throws UnsupportedDialectException   for an unsupported dialect
     * @throws UnsupportedOperationException if this endpoint is not configured for root access
     * @throws SQLException                  if the connection could not be created
     */
    protected Connection getRootConnection(String databaseName)
            throws UnsupportedDialectException, UnsupportedOperationException, SQLException {
        final Connection conn = getRootDataSource(databaseName).getConnection();
        setConnectionUser(conn, config.getRootUserName());
        return conn;
    }

    /**
     * Perform flyway migrations for this endpoint. This will actually perform any migrations that
     * are not already recorded by flyway as having been previously performed in this schema.
     *
     * @throws UnsupportedDialectException for an unsupported dialect
     * @throws SQLException                if the operation fails
     */
    private void performMigrations() throws UnsupportedDialectException, SQLException {
        if (!config.getMigrationLocations().isEmpty()) {
            new FlywayMigrator(Duration.ofMinutes(1),
                    Duration.ofSeconds(5),
                    config.getSchemaName(),
                    Optional.of(config.getMigrationLocations()).filter(s -> s.length() > 0),
                    getDataSourceForFlyway(),
                    config.getFlywayCallbacks()
            ).migrate();
            logger.info("Completed migrations for endpoint {}", config);
        }
    }

    /**
     * Validate, but do not execute, flyway migrations. This will fail if there are any migrations
     * that are present for this schema but have not yet been performed in this schema.
     *
     * @throws UnsupportedDialectException for an unsupported dialect
     * @throws SQLException                if the validation fails
     */
    private void validateMigrations() throws UnsupportedDialectException, SQLException {
        if (!config.getMigrationLocations().isEmpty()) {
            new FlywayMigrator(Duration.ofMinutes(1),
                    Duration.ofSeconds(5),
                    config.getSchemaName(),
                    Optional.of(config.getMigrationLocations()).filter(s -> s.length() > 0),
                    getDataSourceForFlyway(),
                    config.getFlywayCallbacks()
            ).validate();
            logger.info("Validated migrations for endpoint {}", config);
        }
    }

    /**
     * Create the user DB object for this endpoint, if it doesn't already exist.
     *
     * @throws SQLException                if the operation fails
     * @throws UnsupportedDialectException for an unsupported migration
     */
    protected abstract void createNonRootUser() throws SQLException, UnsupportedDialectException;

    /**
     * Perform privilege grants for the user associated with this endpoint, based on the configured
     * access level.
     *
     * @throws SQLException                if privilege grants fail
     * @throws UnsupportedDialectException for an unsupported dialect
     */
    protected abstract void performNonRootGrants() throws SQLException, UnsupportedDialectException;

    /**
     * Create a readers group for this endpoint's schema, if needed for read-level grants.
     *
     * @throws UnsupportedDialectException for unsupported dialect
     * @throws SQLException                if the group could not be created
     */
    protected void createReadersGroup() throws UnsupportedDialectException, SQLException {
    }

    /**
     * Create a writers group for this endpoint's schema, if needed for write-level grants.
     *
     * @throws UnsupportedDialectException for unsupported dialect
     * @throws SQLException                if the group could not be created
     */
    protected void createWritersGroup() throws UnsupportedDialectException, SQLException {
    }

    /**
     * Create the schema configured for this endpoint.
     *
     * @throws SQLException                if the operation fails
     * @throws UnsupportedDialectException for an unsupported dialect
     */
    protected abstract void createSchema() throws SQLException, UnsupportedDialectException;

    /**
     * Log and execute the given SQL statement.
     *
     * @param conn connection to use for execution
     * @param sql  SQL statement to execute
     * @throws SQLException if the operation fails
     */
    protected void execute(Connection conn, String sql) throws SQLException {
        // get the name of the logged-in user for this connection, for inclusion in the log
        // (the value was tagged automatically by the `get*Connection` method that created the
        // connection.)
        String connectionUser = null;
        try {
            connectionUser = getConnectionUser(conn);
        } catch (SQLException ignored) {
        }
        logger.info("Executing SQL as {}: {}", connectionUser, obscurePasswords(sql));
        conn.createStatement().execute(sql);
    }

    /**
     * Identify passwords embedded in the given SQL statement, and replace them with a fixed
     * string.
     *
     * @param sql SQL statement
     * @return obscured statement
     */
    protected abstract String obscurePasswords(String sql);

    /**
     * Get a connection URL for this endpoint, connecting to the endpoint's configured database.
     *
     * @param config the endpoint config
     * @return connection URL
     * @throws UnsupportedDialectException for an unsupported dialect
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

    /**
     * Adjust the URL builder if a secure connection is required.
     *
     * @param config  endpoint configuration
     * @param builder URL builder
     */
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

    /**
     * Create a {@link UriComponentsBuilder} for connecting to this endpoint's database server,
     * specifying the JDBC protocol, host/port, and special query parameters based on this
     * endpoint's dialect.
     *
     * @param config endpoint config
     * @return URL builder
     * @throws UnsupportedDialectException for an unsupported dialect
     */
    @NotNull
    private static UriComponentsBuilder getUrlBuilder(DbEndpointConfig config)
            throws UnsupportedDialectException {
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
     * @throws UnsupportedDialectException for unsupported dialect
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

    /**
     * Attach the given user name to the connection's "client info." Even though this is part of
     * JDBC, annoyingly different databases appear to support this in very different ways, so we
     * need to break it out to dialect-specific implementation.
     *
     * @param conn     connection
     * @param userName user name
     * @throws SQLException if the operation fails
     */
    public abstract void setConnectionUser(Connection conn, String userName) throws SQLException;

    /**
     * Get the user name attached to this connection's "client info".
     *
     * @param conn the connection
     * @return the user name, or null if none is attached
     * @throws SQLException if the operation fails
     */
    public abstract String getConnectionUser(Connection conn) throws SQLException;


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
     * Delete database, user and groups associated with this endpoint.
     *
     * <p>This method is used by the test rule at the end of test class execution.</p>
     */
    protected abstract void tearDown();
}
