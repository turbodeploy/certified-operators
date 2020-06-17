package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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

    void init() throws UnsupportedDialectException, SQLException, InterruptedException {
        if (!componentCredentialsWork()) {
            logger.info("Setting up component database/schema and user");
            setupComponentCredentials();
        }
        if (!Strings.isNullOrEmpty(config.getDbMigrationLocations())) {
            performMigrations();
        }
    }

    DataSource getDataSource() throws UnsupportedDialectException, SQLException, InterruptedException {
        return getDataSource(getUrl(config), config.getDbUserName(), config.getDbPassword());
    }

    abstract DataSource getDataSource(String url, String user, String password)
            throws UnsupportedDialectException, SQLException, InterruptedException;

    private boolean componentCredentialsWork() throws UnsupportedDialectException, InterruptedException {
        try (Connection conn = getNonRootConnection()) {
            return true;
        } catch (SQLException e) {
            logger.warn("Failed to obtain DB connection for user {}", config.getDbUserName(), e);
            return false;
        }
    }

    private void setupComponentCredentials() throws UnsupportedDialectException, SQLException, InterruptedException {
        createSchema();
        createNonRootUser();
        performNonRootGrants(config.getDbAccess());
    }

    private void performMigrations() throws UnsupportedDialectException, SQLException, InterruptedException {
        new FlywayMigrator(Duration.ofMinutes(1),
                Duration.ofSeconds(5),
                config.getDbSchemaName(),
                Optional.of(config.getDbMigrationLocations()).filter(s -> s.length() > 0),
                getDataSource(),
                config.getDbFlywayCallbacks()
        ).migrate();
    }

    protected abstract void createNonRootUser() throws SQLException, UnsupportedDialectException, InterruptedException;

    protected abstract void performNonRootGrants(DbEndpointAccess access)
            throws SQLException, UnsupportedDialectException, InterruptedException;

    Connection getNonRootConnection() throws UnsupportedDialectException, SQLException, InterruptedException {
        return getConnection(getUrl(config), config.getDbUserName(), config.getDbPassword());
    }

    protected Connection getRootConnection(String database) throws UnsupportedDialectException, SQLException, InterruptedException {
        return getConnection(getUrl(config, database), config.getDbRootUserName(), config.getDbRootPassword());
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
        builder = addSecureConnectionParams(config, builder);
        return builder.build().toUriString();
    }

    private static UriComponentsBuilder addSecureConnectionParams(DbEndpointConfig config, UriComponentsBuilder builder) {
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
        return builder;
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
