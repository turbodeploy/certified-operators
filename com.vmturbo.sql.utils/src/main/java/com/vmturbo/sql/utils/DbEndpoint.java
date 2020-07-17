package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
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
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.web.context.ConfigurableWebApplicationContext;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.Operation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.api.ServerStartedNotifier;
import com.vmturbo.components.api.ServerStartedNotifier.ServerStartedListener;

/**
 * This class manages access to a database, including initializing the database on first use,
 * applying migrations during restarts, and providing access in the form of JDBC connections and
 * data sources, and jOOQ DSL contexts.
 *
 * <p>A given endpoint may be defined with a tag, in which case config property names are all
 * prefixed with the tag name and an underscore, e.g. "xxx_dbPort" instead of just "dbPort". A
 * component may define at most one untagged endpoint, and for that component,
 * config properties are un-prefixed.</p>
 *
 * <p>Each endpoint is defined with a {@link SQLDialect} value that identifies the type of database
 * server accessed by the endpoint. Some config property defaults are based on this value. Defaults
 * are used when the Spring {@link Environment} constructed during application context creation does
 * not supply a value.</p>
 *
 * <p>Relevant properties include:</p>
 * <dl>
 *     <dt>dbHost</dt>
 *     <dd>The host name or IP address of the DB server. Default is "localhost."/dd>
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
 *     the Spring (SpEL) syntax for map literals.</dd>
 *     <dt>dbSecure</dt>
 *     <dd>Boolean indicating whether the database should be accessed with a secure (SSL) connection.
 *     Default is false.</dd>
 *     <dt>dbMigrationLocations</dt>
 *     <dd>Package name (or names, separated by commas) defining the location where Flyway can find
 *     migrations for this database. Empty string suppresses migration activity.
 *     Defaults to "db.migration.&lt;component-name&gt;".</dd>
 *     <dt>dbFlywayCallbacks</dt>
 *     <dd>Array of {@link FlywayCallback} instances to be invoked during migration processing.
 *     This cannot be specified via configuration, but must be supplied in the endpoint definition
 *     using the {@link DbEndpointBuilder#withDbFlywayCallbacks(FlywayCallback...)} method.
 *     Defaults to no callbacks (empty array).
 *     </dd>
 *     <dt>dbDestructiveProvisioningEnabled</dt>
 *     <dd>True if provisioning of this endpoint (creation of databases, schemas, etc.) may make
 *     use of destructive operations, like dropping an existing user prior to recreating it, in order
 *     to clean up presumed issues with the current object. Defaults to false.</dd>
 *     <dt>dbEndpointEnabled</dt>
 *     <dd>Whether this endpoint should be initialized at all. Defaults to true.</dd>
 *     <dt>dbShouldProvisionDatabase</dt>
 *     <dd>Whether this endpoint should perform database/schema provisioning if required. Defaults
 *     to false.</dd>
 *     <dt>dbShouldProvisionUser</dt>
 *     <dd>Whether this endpoint should perform user provisioning if required. Defaults to false.</dd>
 *     <dt>dbProvisioningSuffix</dt>
 *     <dd>A string appended to names of database objects created during provisioning, including
 *     database names, schema names, user names, etc. This can be set during tests, for example,
 *     to cause the endpoint to create, provision, and provide access to a temporary database,
 *     rather the database normally accessed by that endpoint.</dd>
 * </dl>
 *
 * <p>Endpoints are constructed using a builder pattern where values for selected properties can be
 * explicitly provided. Any such property will be used in place of spring-injected property values
 * or built-in defaults.</p>
 */
public class DbEndpoint {
    private final DbEndpointConfig config;
    private CompletableFuture<Void> future;
    private DbAdapter adapter;
    private boolean retriesCompleted = false;
    private Throwable failureCause;

    /**
     * Create a new {@link DbEndpoint primary database endpoint}, without a tag.
     *
     * <p>Properties for this endpoint should be configured with out tag prefixes. A component
     * may compare at most one primary endpoint.</p>
     *
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     */
    public static DbEndpointBuilder primaryDbEndpoint(SQLDialect dialect) {
        return secondaryDbEndpoint("", dialect);
    }

    /**
     * Create a new {@link DbEndpoint secondary database endpoint} with a given tag.
     *
     * <p>Properties for this endpoint should be configured with property names that include the
     * given tag as a prefix, e.g. "xxx_dbPort" to configure the port number for an endpoint with
     * "xxx" as the tag.</p>,
     *
     * <p>A component may define any number of secondary endpoints (all with distinct tags), in
     * addition to a single primary endpoint, if the latter is required.</p>
     *
     * @param tag     the tag for this endpoint
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     */
    public static DbEndpointBuilder secondaryDbEndpoint(@Nonnull final String tag, SQLDialect dialect) {
        return new DbEndpointBuilder(tag, dialect);
    }

    /**
     * Create a new abstract endpoint.
     *
     * <p>An abstract DB endpoint will never be operationalized. It is a means of creating a
     * template that can be shared across components that need to access the same database objects.
     * Each such component should create its own endpoint(s) derived from the shared abstract
     * endpoint.</p>
     *
     * <p>Typically, an abstract endpoint will specify database name, and schema name, but
     * any of the properties can be specified in the builder. Any such property values will be
     * used instead of the normal built-in defaults in derived components.</p>
     *
     * <p>Abstract components are not affected by external configuration, but derived endpoints
     * can have their properties set or changed through external configuration, including
     * properties that are set in the abstract endpoint.</p>
     *
     * <p>An abstract endpoint also has no dialect, since it is neither resolved (hence no need to
     * access dialect-dependent property defaults), nor operational (so no need for an adapter).</p>
     *
     * @return a new abstract endpoint
     */
    public static DbEndpointBuilder abstractDbEndpoint() {
        return new DbEndpointBuilder("", null).setAbstract();
    }

    private DbEndpoint(DbEndpointConfig config) {
        this.config = config;
        this.future = new CompletableFuture<>();
    }

    public DbEndpointConfig getConfig() {
        return config;
    }

    private void markComplete(DbAdapter adapter) {
        this.adapter = adapter;
        this.future.complete(null);
    }

    private void markComplete(Throwable e) {
        this.failureCause = e;
        future.completeExceptionally(e);
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
        awaitCompletion(config.getMaxAwaitCompletionMs(), TimeUnit.MILLISECONDS);
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
        awaitCompletion(config.getMaxAwaitCompletionMs(), TimeUnit.MILLISECONDS);
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
     */
    private Configuration getConfiguration() throws UnsupportedDialectException, SQLException {
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
     */
    private DataSourceConnectionProvider connectionProvider()
            throws UnsupportedDialectException, SQLException {
        return new DataSourceConnectionProvider(
                new TransactionAwareDataSourceProxy(
                        new LazyConnectionDataSourceProxy(
                                adapter.getDataSource())));
    }

    /**
     * Get the adapter for this endpoint.
     *
     * @return {@link DbAdapter}
     * @throws InterruptedException if interrupted
     */
    public DbAdapter getAdapter() throws InterruptedException {
        awaitCompletion(config.getMaxAwaitCompletionMs(), TimeUnit.MILLISECONDS);
        return adapter;
    }

    public boolean isReady() {
        return future.isDone() && !future.isCompletedExceptionally() && !future.isCancelled();
    }

    /**
     * Wait for the endpoint to become ready for use.
     *
     * <p>This is invoked other public methods of this class that would provide access to the
     * database, e.g. by returning a connection to the database.</p>
     *
     * <p>The operation of this method is controlled by the retry schedule configured via the
     * {@link DbEndpointResolver#DB_RETRY_BACKOFF_TIMES_SEC_PROPERTY} property:</p>
     *
     * <ul>
     *     <li>
     *         If the property is an an empty array, this method will silently wait indefinitely for
     *         the endpoint to be successfully initialized or to fail initialization.
     *     </li>
     *     <li>
     *         Otherwise, the indicated retry schedule will be undertaken. On each iteration,
     *         the initialization procedure will be invoked anew, and after waiting the scheduled
     *         number of seconds, the disposition of the endpoint will be checked. If the endpoint
     *         is ever found to be successfully initialized, the schedule terminates and this method
     *         returns. If the schedule completes without that happening, this method throws
     *         {@link IllegalStateException} to indicate ultimate failure.
     *     </li>
     * </ul>
     *
     * <p>Note that if the final backoff period in a non-empty schedule is -1, the prior period
     * will be repeated indefinitely, and this method will not return until the endpoint is
     * successfully initialized.</p>
     *
     * <p>TODO: Try to determine cases where the reason is unrepairable, and don't perform
     * additional retries.</p>
     *
     * @param timeout The time to wait for completion to be finished.
     * @param timeUnit The time unit for the completion wait time.
     * @throws InterruptedException if interrupted
     */
    public synchronized void awaitCompletion(long timeout, TimeUnit timeUnit) throws InterruptedException {
        try {
            if (isReady()) {
                if (!retriesCompleted) {
                    logger.info("{} was successfully initialized", this);
                }
                return;
            } else if (retriesCompleted) {
                throw new IllegalStateException(String.format(
                        "DB endpoint %s failed initialization including attempted retries", this),
                        failureCause);
            }
            // true output means retry
            final Operation<Boolean> op = () -> {
                if (future.isDone()) {
                    try {
                        logger.debug("Waiting for future....");
                        future.get();
                        // endpoint is ready to go... no more retries
                        return false;
                    } catch (ExecutionException e) {
                        // prior attempt failed... try again with a fresh future
                        this.future = new CompletableFuture<>();
                        DbEndpointCompleter.get().completeEndpoint(this);
                        // we'll get the last of these handed back to us if we ultimately give up
                        throw new RetriableOperationFailedException(e);
                    } catch (InterruptedException e) {
                        // this will cause us to break out of the retry loop
                        throw new RetriableOperationFailedException(e);
                    }
                } else {
                    // still waiting for completion
                    return true;
                }
            };
            RetriableOperation.newOperation(op)
                    .retryOnOutput(Boolean::booleanValue)
                    // we retry after anything but a wrapped InterruptedException
                    .retryOnException(e ->
                            !(e instanceof RetriableOperationFailedException
                                    && e.getCause() instanceof InterruptedException))
                    .run(timeout, timeUnit);
            // exited non-exceptionally means we succeeded
            logger.info("{} completed initialization successfully", this);
        } catch (RetriableOperationFailedException wrapped) {
            Throwable e = wrapped.getCause();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            } else if (e instanceof ExecutionException) {
                // if initialization threw an exception, unwrap it from the exception thrown
                // by the future
                e = e.getCause();
            }
            logger.error("Endpoint {} failed initialization.", this, e);
            throw new IllegalStateException(String.format("Endpoint %s failed initialization", this), e);
        } catch (TimeoutException timeoutException) {
            logger.error("Endpoint {} still not initialized after retries: {}", this, timeoutException.getMessage());
            throw new IllegalStateException(
                    String.format("Endpoint %s still not initialized after retries", this));
        } finally {
            // don't go through this whole thing more than once per endpoint
            this.retriesCompleted = true;
        }
    }

    /**
     * Reset the internal endpoint registry to its initial state, as if no endpoints have been
     * created since startup.
     *
     * <p>This never happens during normal operations, where endpoints never undo their
     * provisioning and typically remain available (in the Spring context) until the component
     * terminates. In tests, where isolation between test classes is important, we use this method
     * to reset all endpoint configuration when DbEndpointRule processes the end of a test class in
     * which it appears.</p>
     */
    static void resetAll() {
        DbEndpointCompleter.get().resetAll();
    }

    @Override
    public String toString() {
        // "tag"
        String tagLabel = !StringUtils.isEmpty(config.getTag()) ? "tag " + config.getTag() : "untagged";
        if (isReady()) {
            String url;
            try {
                url = DbAdapter.getUrl(config);
            } catch (UnsupportedDialectException e) {
                url = "[invalid dialect]";
            }
            return FormattedString.format("DbEndpoint[{}; url={}; user={}]", tagLabel, url, config.getDbUserName());
        } else {
            return FormattedString.format("DbEndpoint[{}; uninitialized", tagLabel);
        }
    }

    /**
     * Class to manage the initialization of endpoint, resulting in completion of the {@link
     * Future}s created during endpoint registration.
     */
    public static class DbEndpointCompleter implements ServerStartedListener {
        private static final DbEndpointCompleter INSTANCE = new DbEndpointCompleter();

        private DbEndpointCompleter() {
            ServerStartedNotifier.get().registerListener(this);
        }

        final List<DbEndpoint> pendingEndpoints = new ArrayList<>();
        static UnaryOperator<String> resolver;
        private static DBPasswordUtil dbPasswordUtil;

        /**
         * Get the singleton instance of the {@link DbEndpointCompleter}.
         *
         * @return The {@link DbEndpointCompleter} instance.
         */
        public static DbEndpointCompleter get() {
            return INSTANCE;
        }

        DbEndpoint register(DbEndpointConfig config) {
            if (config.dbIsAbstract()) {
                // no need to register abstract endpoints, since there's no completion processing
                // involved - just wrap the config in an abstract endpoint and we're done
                return new AbstractDbEndpoint(config);
            }
            final DbEndpoint endpoint = new DbEndpoint(config);
            synchronized (pendingEndpoints) {
                if (resolver == null) {
                    pendingEndpoints.add(endpoint);
                } else {
                    completeEndpoint(endpoint);
                }
            }
            return endpoint;
        }

        @Override
        public void onServerStarted(ConfigurableWebApplicationContext serverContext) {
            ConfigurableEnvironment environment = serverContext.getEnvironment();
            String authHost = Objects.requireNonNull(environment.getProperty("authHost"));
            String authRoute = Objects.requireNonNull(environment.getProperty("authRoute", ""));
            int serverHttpPort = Integer.parseInt(
                    Objects.requireNonNull(environment.getProperty("serverHttpPort")));
            int authRetryDelaySec = Integer.parseInt(
                    Objects.requireNonNull(environment.getProperty("authRetryDelaySecs")));
            DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, serverHttpPort, authRoute, authRetryDelaySec);
            setResolver(environment::getProperty, dbPasswordUtil, true);
        }

        void setResolver(UnaryOperator<String> resolver, DBPasswordUtil dbPasswordUtil,
                boolean completePending) {
            synchronized (pendingEndpoints) {
                DbEndpointCompleter.resolver = resolver;
                DbEndpointCompleter.dbPasswordUtil = dbPasswordUtil;
                if (completePending) {
                    completePendingEndpoints();
                }
            }
        }

        private void completePendingEndpoints() {
            logger.info("Completing {} uninitialized endpoints.", pendingEndpoints.size());
            for (DbEndpoint endpoint : pendingEndpoints) {
                completeEndpoint(endpoint);
            }
        }

        /**
         * Only used for tests.
         *
         * @param endpoint db endpoint
         */
        void completePendingEndpoint(DbEndpoint endpoint) {
            if (endpoint.config.dbIsAbstract() || endpoint.future.isDone()) {
                return;
            }
            synchronized (pendingEndpoints) {
                if (endpoint.getConfig().getTemplate() != null) {
                    completePendingEndpoint(endpoint.getConfig().getTemplate());
                }
                final List<DbEndpoint> endpointsToRemove = new ArrayList<>();
                pendingEndpoints.stream()
                        .filter(endpoint::equals)
                        .peek(endpointsToRemove::add)
                        .findFirst()
                        .ifPresent(this::completeEndpoint);
                pendingEndpoints.removeAll(endpointsToRemove);
            }
        }

        void completeEndpoint(DbEndpoint endpoint) {
            if (endpoint.isReady()) {
                logger.info("Skipping endpoint completion for {} because it's ready.", endpoint);
                return;
            }
            final DbEndpointConfig config = endpoint.getConfig();
            logger.info("Completing {}", endpoint);
            try {
                resolveConfig(config);
                final DbAdapter adapter = DbAdapter.of(config);
                adapter.init();
                endpoint.markComplete(adapter);
            } catch (Exception e) {
                logger.warn("Failed to create {}. Error: {}", endpoint, e.getMessage());
                endpoint.markComplete(e);
            }
        }

        private void resolveConfig(DbEndpointConfig config)
                throws UnsupportedDialectException {
            new DbEndpointResolver(config, resolver, dbPasswordUtil).resolve();
        }

        private void resetAll() {
            synchronized (pendingEndpoints) {
                pendingEndpoints.clear();
                resolver = null;
                dbPasswordUtil = null;
            }
        }
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
     * An endpoint class that is intended solely to be used as a base for other endpoints,
     * passing on any builder-specified properties as defaults to the derived endpoints.
     */
    private static class AbstractDbEndpoint extends DbEndpoint {

        AbstractDbEndpoint(final DbEndpointConfig config) {
            super(config);
        }

        @Override
        public DSLContext dslContext() {
            throw new UnsupportedOperationException("Abstract DbEndpoint cannot be used for database access");
        }

        @Override
        public DataSource datasource() {
            throw new UnsupportedOperationException("Abstract DbEndpoint cannot be used for database access");
        }

        @Override
        public DbAdapter getAdapter() {
            throw new UnsupportedOperationException("Abstract DbEndpoint cannot be used for database access");
        }

        @Override
        public boolean isReady() {
            throw new UnsupportedOperationException("Abstract DbEndpoint cannot be used for database access");
        }

        @Override
        public synchronized void awaitCompletion(final long timeout, final TimeUnit timeUnit) {
            throw new UnsupportedOperationException("Abstract DbEndpoint cannot be used for database access");
        }
    }

    /**
     * Enum declaring levels of access for database endpoints.
     */
    public enum DbEndpointAccess {

        /** Full access to the configured schema. */
        ALL(true, true),
        /** Ability to read and write to all objects in the database, but not to create new objects. */
        READ_WRITE_DATA(true, false),
        /**
         * Ability to read all objects in the database, but not to alter any data or create new
         * objects.
         */
        READ_ONLY(false, false);

        private final boolean writeAccess;
        private final boolean createNewTable;

        DbEndpointAccess(boolean writeAccess, boolean createNewTable) {
            this.writeAccess = writeAccess;
            this.createNewTable = createNewTable;
        }

        public boolean isWriteAccess() {
            return writeAccess;
        }

        /**
         * Whether this endpoint can create new tables.
         *
         * @return true if it can create new table, otherwise false
         */
        public boolean canCreateNewTable() {
            return createNewTable;
        }
    }
}
