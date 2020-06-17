package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

import javax.sql.DataSource;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import com.vmturbo.auth.api.db.DBPasswordUtil;

/**
 * This class manages access to a database, including initializing the database on first use,
 * applying migrations during restarts, and providing access in the form of JDBC connections and
 * data sources, and jOOQ DSL contexts.
 *
 * <p>Normally, a component should create db endpoints with the assistance of the {@link
 * SQLDatabaseConfig2} class, which constructs a context for resolving the config objects that apply
 * to each endpoint.</p>
 *
 * <p>A given endpoint may be defined with a tag, in which case config property names are all
 * prefixed with the tag name and an underscore, e.g. "xxx_dbPort" instead of just "dbPort". A
 * component may define (at least through {@link SQLDatabaseConfig2} at most one untagged endpoint,
 * and for that component, config properties are un-prefixed.</p>
 *
 * <p>Each endpoint is defined with a {@link SQLDialect} value that identifies the type of database
 * server accessed by the endpoint. Some config property defaults are based on this value. Defaults
 * are used when the Spring {@link Environment} constructed during application context creation does
 * not supply a value.</p>
 *
 * <p>Relevant properties include:</p>
 * <dl>
 *     <dt>dbHost</dt>
 *     <dd>The host name or IP address of the DB server. No default.</dd>
 *     <dt>dbPort</dt>
 *     <dd>The port on which the server listens for new connections. Default is the standard port
 *     for server type, e.g. 3306 for MySql/MariaDB, 5432 for Postgres, etc.</dd>
 *     <dt>dbDatabaseName</dt>
 *     <dd>The name of the database to be accessed on the server. Defaults to the tag name for
 *     tagged endpoints, or to the underlying component name for untagged endpoints.</dd>
 *     <dt>dbSchemaName</dt>
 *     <dd>The name of the schema to be accessed (not relevant for MySql/MariaDB). Defaults to the
 *     tag name for tagged instances, or to the component name for untagged instances.</dd>
 *     <dt>dbUserName</dt>
 *     <dd>The non-root user name by which this endpoint will access the database. Defaults to the
 *     tag name for tagged instances, or to the component name for untagged instances.</dd>
 *     <dt>dbPassword</dt>
 *     <dd>The password for the non-root user. Defaults to the root DB password obtained from the
 *     auth component.</dd>
 *     <dt>dbAccess</dt>
 *     <dd>Specifies the access level required for this endpoint, selected from the
 *     {@link DbEndpointAccess} enum. Default is READ_ONLY.</dd>
 *     <dt>dbRootUserName</dt>
 *     <dd>The name of a DB user with root (super) privileges that will be used to create and set up
 *     this endpoint's database, schema, and non-root user, if needed. Defaults to the user name
 *     provided by the auth component.</dd>
 *     <dt>dbRootPassword</dt>
 *     <dd>The password for the root user. Defaults to the password provided by the auth component.</dd>
 *     <dt>dbDriverProperties</dt>
 *     <dd>Map of name/value pairs for properties to be conveyed as JDBC URL query parameters when
 *     creating a connection to the database. Defaults are based on the {@link SQLDialect}. Values
 *     provided through configuration are merged into the defaults, and should be specified using
 *     the Spring (SPEL) syntax for map literals.</dd>
 *     <dt>dbSecure</dt>
 *     <dd>Boolean indicating whether the database should be accessed with a secure (SSL) connection.
 *     Default is false.</dd>
 *     <dt>dbMigrationLocations</dt>
 *     <dd>Package name (or names, separated by commas) defining the location where Flyway can find
 *     migrations for this database. Defaults to "db.migration".</dd>
 *     <dt>dbFlywayCallbacks</dt>
 *     <dd>Array of {@link FlywayCallback} instances to be invoked during migration processing.
 *     This cannot be specified via configuration, but must be supplied in the endpoint definition
 *     using the {@link DbEndpointBuilder#withDbFlywayCallbacks(FlywayCallback...)} method.
 *     Defaults to no callbacks.
 *     </dd>
 *     <dt>dbDestructiveProvisioningEnabled</dt>
 *     <dd>True if provisioning of this endpoint (creationg of databases, schemas, etc.) may make
 *     use of destructive operations, like dropping an existing user prior to recreating it, in order
 *     to clean up presumed issues with the current object. Defaults to false.</dd>
 *     <dt>dbEndpointEnabled</dt>
 *     <dd>Whether this endpoint should be initialized at all. Defaults to true.</dd>
 * </dl>
 *
 * <p>Endpoints are constructed using a builder pattern where values for selected properties can be
 * explicitly provided. Any such property will be used in place of spring-injected property values
 * or built-in defaults.</p>
 */
public class DbEndpoint {

    static final List<Pair<DbEndpointConfig, CompletableFuture<DbEndpoint>>> pendingEndpoints
            = new ArrayList<>();

    static UnaryOperator<String> resolver;
    private static DBPasswordUtil dbPasswordUtil;

    private final DbEndpointConfig config;
    private final DbAdapter adapter;

    static Future<DbEndpoint> register(DbEndpointConfig config) {
        final CompletableFuture<DbEndpoint> future = new CompletableFuture<>();
        if (resolver == null) {
            pendingEndpoints.add(Pair.of(config, future));
        } else {
            synchronized (pendingEndpoints) {
                completeEndpoint(config, future);
            }
        }
        return future;
    }

    static void setResolver(UnaryOperator<String> resolver, DBPasswordUtil dbPasswordUtil) {
        synchronized (pendingEndpoints) {
            DbEndpoint.resolver = resolver;
            DbEndpoint.dbPasswordUtil = dbPasswordUtil;
            completePendingEndpoints();
        }
    }

    private static void completePendingEndpoints() {
        for (Pair<DbEndpointConfig, CompletableFuture<DbEndpoint>> pair : pendingEndpoints) {
            completeEndpoint(pair.getLeft(), pair.getRight());
        }
    }

    private static void completeEndpoint(DbEndpointConfig config,
            CompletableFuture<DbEndpoint> future) {
        logger.info("Completing endpoint with tag {}", config.getTag());
        try {
            resolveConfig(config);
            final DbAdapter adapter = DbAdapter.of(config);
            adapter.init();
            future.complete(new DbEndpoint(config, adapter));
        } catch (UnsupportedDialectException | SQLException | InterruptedException e) {
            String label = config.getTag() == null ? "untagged DbEndpoint"
                    : String.format("DbEndpoint tagged '%s'", config.getTag());
            logger.error("Failed to create {}", label, e);
            future.completeExceptionally(e);
        }
    }

    private static void resolveConfig(DbEndpointConfig config)
            throws UnsupportedDialectException {
        new DbEndpointResolver(config, resolver, dbPasswordUtil).resolve();
    }

    private DbEndpoint(DbEndpointConfig config, DbAdapter adapter) {
        this.config = config;
        this.adapter = adapter;
    }

    public DbEndpointConfig getConfig() {
        return config;
    }


    // TODO add some debug logging
    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a {@link DSLContext} bound to this endpoint.
     *
     * @return dsl context
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException        if interrupted
     */
    public DSLContext dslContext() throws UnsupportedDialectException, SQLException, InterruptedException {
        if (config.getDbEndpointEnabled()) {
            return DSL.using(getConfiguration());
        } else {
            throw new IllegalStateException("Attempt to use disabled database endpoint");
        }
    }

    /**
     * Get a {@link DataSource} bound to this endpoint.
     *
     * @return dataSource data source
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException        if interrupted
     */
    public DataSource datasource() throws UnsupportedDialectException, SQLException, InterruptedException {
        if (config.getDbEndpointEnabled()) {
            return adapter.getDataSource();
        } else {
            throw new IllegalStateException("Attempt to use disabled database endpoint");
        }
    }

    /**
     * Get a jOOQ {@link Configuration} object configured for and bound to this endpoint.
     *
     * @return the configuration object
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem creating the configuration
     * @throws InterruptedException        if interrupted
     */
    private Configuration getConfiguration() throws UnsupportedDialectException, SQLException, InterruptedException {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
        jooqConfiguration.set(connectionProvider());
        jooqConfiguration.set(new Settings()
                .withRenderNameStyle(RenderNameStyle.LOWER)
                // Set withRenderSchema to false to avoid rendering schema name in Jooq generated SQL
                // statement. For example, with false withRenderSchema, statement
                // "SELECT * FROM vmtdb.entities" will be changed to "SELECT * FROM entities".
                // And dynamically set schema name in the constructed JDBC connection URL to support
                // multi-tenant database.
                .withRenderSchema(false));
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));
        jooqConfiguration.set(config.getDialect());
        return jooqConfiguration;
    }

    /**
     * Get a connection provider bound to this endpoint.
     *
     * @return the connection provider
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException        if interrupted
     */
    private DataSourceConnectionProvider connectionProvider()
            throws UnsupportedDialectException, SQLException, InterruptedException {
        return new DataSourceConnectionProvider(
                new TransactionAwareDataSourceProxy(
                        new LazyConnectionDataSourceProxy(
                                adapter.getDataSource())));
    }

    /**
     * Get the adapter for this endpoint.
     *
     * @return {@link DbAdapter}
     */
    public DbAdapter getAdapter() {
        return adapter;
    }

    /**
     * Exception to throw when an unsupported {@link SQLDialect} value is used to configure an
     * endpoint.
     */
    public static class UnsupportedDialectException extends Exception {
        /**
         * Create a new instance with a message identifying the unsupported dialect value.
         *
         * @param dialect dialect value
         */
        public UnsupportedDialectException(SQLDialect dialect) {
            this("Unsupported SQLDialect: " + dialect.name());
        }

        /**
         * Create a new instance with the given message.
         *
         * @param s message
         */
        public UnsupportedDialectException(final String s) {
            super(s);
        }
    }

    /**
     * Create a jOOQ exception translator for this endpoint.
     *
     * @return exception translator
     */
    public JooqExceptionTranslator exceptionTranslator() {
        return new JooqExceptionTranslator();
    }

    /**
     * Enum declaring levels of access for database endpoints.
     */
    public enum DbEndpointAccess {

        /** Full access to the configured schema. */
        ALL,
        /** Ability to read and write to all objects in the database, but not to create new objects. */
        READ_WRITE_DATA,
        /**
         * Ability to read all objects in the database, but not to alter any data or create new
         * objects.
         */
        READ_ONLY
    }
}
