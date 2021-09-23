package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_MAX_POOL_SIZE;
import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_MIN_POOL_SIZE;
import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_USE_CONNECTION_POOL;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.sql.DataSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.dbmonitor.DbMonitor;
import com.vmturbo.sql.utils.dbmonitor.DbMonitorConfig;
import com.vmturbo.sql.utils.dbmonitor.ProcessListClassifier;
import com.vmturbo.sql.utils.pool.DbConnectionPoolConfig;
import com.vmturbo.sql.utils.pool.HikariPoolMonitor;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Configuration for interaction with database.
 *
 * <p>Components that want to connect to the database should import this configuration into their
 * Spring context with an @Import annotation (please do not @ComponentScan the sql.utils package!).
 * </p>
 */
@Configuration
@EnableTransactionManagement
@Import({DbMonitorConfig.class})
public abstract class SQLDatabaseConfig {

    // hardcoded temp db user for checking root user GRANT permissions.
    private static String turboTestUser = "vmttmpuser";

    @Value("${enableSecureDBConnection:false}")
    private boolean isSecureDBConnectionRequested;

    @Value("${dbHost}")
    private String dbHost;

    @Value("${dbPort}")
    private int dbPort;

    /**
     * DB root username.
     */
    @Value("${dbRootUsername:root}")
    private String dbRootUsername;

    /**
     * DB root password.
     */
    @Value("${dbRootPassword:}")
    private String dbRootPassword;

    /**
     * If true, use a connection pool for database connections.
     */
    @Value("${conPoolActive:" + DEFAULT_USE_CONNECTION_POOL + "}")
    private boolean isConnectionPoolActive;

    /**
     * Duration between reports logged by Connection pool monitor in seconds. Zero means reporting is not active.
     */
    @Value("${conPoolMonitorIntervalSec:60}")
    private int poolMonitorIntervalSec;

    /**
     * DB connection pool initial and minimum size. Defaults to 1.
     */
    @Value("${conPoolInitialSize:" + DEFAULT_MIN_POOL_SIZE + "}")
    private int dbMinPoolSize;

    /**
     * DB connection pool maximum size. Defaults to 10.
     */
    @Value("${conPoolMaxActive:" + DEFAULT_MAX_POOL_SIZE + "}")
    private int dbMaxPoolSize;

    /**
     * DB connection pool frequency to send keep alive messages on idle connections. Defaults to 5.
     */
    @Value("${conPoolKeepAliveIntervalMinutes:" + DbConnectionPoolConfig.DEFAULT_KEEPALIVE_TIME_MINUTES + "}")
    private int dbPoolKeepAliveIntervalMinutes;

    @Value("${sqlDialect}")
    private String sqlDialectName;

    @Value("${migrationLocation:}")
    private String migrationLocation;

    @Value("${authHost}")
    protected String authHost;

    @Value("${authRoute:}")
    protected String authRoute;

    @Value("${serverHttpPort}")
    protected int authPort;

    @Value("${authRetryDelaySecs}")
    protected int authRetryDelaySecs;

    @Value("${mariadbDriverProperties:useServerPrepStmts=true}")
    private String mariadbDriverProperties;

    @Value("${mysqlDriverProperties:useServerPrepStmts=true}")
    private String mysqlDriverProperties;

    private static final Logger logger = LogManager.getLogger();

    private DBPasswordUtil dbPasswordUtil;

    /**
     * Get the name of the database schema. Each component has its own schema.
     *
     * @return The name of the schema.
     */
    public abstract String getDbSchemaName();

    public abstract String getDbUsername();

    @Bean
    @Primary
    public DataSource dataSource() {
        return dataSource(getSQLConfigObject().getDbRootUrl(), dbRootUsername,
                getDBRootPassword(false), dbMinPoolSize, dbMaxPoolSize, false);
    }

    /**
     * Get the DataSource from the given DB url, username and password.
     *
     * @param dbUrl      Given JDBC connection url.
     * @param dbUsername Given DB username.
     * @param dbPassword Given DB password.
     * @return DataSource from which DB connection can be obtained.
     */
    @Nonnull
    protected DataSource dataSource(@Nonnull String dbUrl, @Nonnull String dbUsername,
                                    @Nonnull String dbPassword, final int minPoolSize,
                                    final int maxPoolSize, final boolean usePool) {
        try {
            if (usePool) {
                // Should be logged only once, on container startup
                logger.info("Initializing pooled database connection source.");
                final String poolName = DbConnectionPoolConfig.generatePoolName(getDbSchemaName());
                HikariDataSource dataSource = DbConnectionPoolConfig.getPooledDataSource(
                    dbUrl, dbUsername, dbPassword, minPoolSize, maxPoolSize,
                    dbPoolKeepAliveIntervalMinutes, poolName);
                return  dataSource;
            } else {
                MariaDbDataSource dataSource = new MariaDbDataSource();
                // Should be logged only once, on container startup
                logger.info("Initializing non-pooled database connection source.");
                dataSource.setUrl(dbUrl);
                dataSource.setUser(dbUsername);
                dataSource.setPassword(dbPassword);
                return dataSource;
            }
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
    }

    @Bean
    public LazyConnectionDataSourceProxy lazyConnectionDataSource() {
        return new LazyConnectionDataSourceProxy(dataSource());
    }

    @Bean
    public TransactionAwareDataSourceProxy transactionAwareDataSource() {
        return new TransactionAwareDataSourceProxy(lazyConnectionDataSource());
    }

    @Bean
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(lazyConnectionDataSource());
    }

    @Bean
    public DataSourceConnectionProvider connectionProvider() {
        return new DataSourceConnectionProvider(transactionAwareDataSource());
    }

    @Bean
    public JooqExceptionTranslator exceptionTranslator() {
        return new JooqExceptionTranslator();
    }

    @Bean
    public DefaultConfiguration configuration() {
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
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()),
            JooqTracingInterceptor::new);

        SQLDialect dialect = SQLDialect.valueOf(sqlDialectName);
        jooqConfiguration.set(dialect);

        return jooqConfiguration;
    }

    /**
     * Callbacks to be configured for our Flyway migrations.
     *
     * <p>These can be used to handle issues such as problematic migrations that have been released
     * to customers and thus cannot generally be either replaced or removed from the migration
     * sequence.</p>
     *
     * <p>A component should define a {@link Primary} bean elsewhere in order to override the
     * empty default.</p>
     *
     * @return array of callback objects, in order in which they should be invoked
     */
    @Bean
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[0];
    }

    /**
     * Flyway migration on given scheme with given db username and password.
     *
     * @param schemaName Given schema name.
     * @param dataSource DataSource from which DB connection can be obtained.
     * @return Flyway.
     */
    @Nonnull
    private Flyway flyway(@Nonnull String schemaName, @Nonnull DataSource dataSource) {
        return new FlywayMigrator(Duration.ofMinutes(1),
            Duration.ofSeconds(5),
            schemaName,
            StringUtils.isEmpty(migrationLocation) ? Optional.empty() : Optional.of(migrationLocation),
            dataSource,
            flywayCallbacks()
        ).migrate();
    }

    @Bean
    public DSLContext dsl() {
        return new DefaultDSLContext(configuration());
    }

    /**
     * Get DTO with all the SQL connection parameters.
     *
     * @return {@link SQLConfigObject}
     */
    @Bean
    public SQLConfigObject getSQLConfigObject() {
        String dbPassword = getDBRootPassword(false);
        final Optional<UsernamePasswordCredentials> rootCredentials = (dbRootUsername != null && dbPassword != null) ?
            Optional.ofNullable(new UsernamePasswordCredentials(dbRootUsername, dbPassword)) : Optional.empty();
        final Map<SQLDialect, String> driverPropertiesMap = ImmutableMap.of(
                SQLDialect.MARIADB, mariadbDriverProperties,
                SQLDialect.MYSQL, mysqlDriverProperties);
        return new SQLConfigObject(dbHost, dbPort, getDbSchemaName(), rootCredentials, sqlDialectName,
            isSecureDBConnectionRequested, driverPropertiesMap);
    }

    /**
     * Helper method to get {@link DataSource}.
     *
     * @param dbSchemaName db schema name.
     * @param dbUsername   db user name.
     * @param password     db user password.
     * @return {@link DataSource}
     */
    @NotNull
    protected DataSource getDataSource(String dbSchemaName, String dbUsername,
            Optional<String> password) {
        // If no db password specified, use root password.
        return dataSourceConfig(dbSchemaName, dbUsername,
                password.orElseGet(() -> getDBRootPassword(true)),
                password.isPresent());
    }

    /**
     * Set up DB configuration. If DB connection is available with the provided user credentials and
     * schema, then access the database in this component with the given user; else if DB connection
     * is not available with the given schema and user, use root credentials to run flyway migration
     * to create the DB schema and create DB user with the given credentials, and then access the
     * database in the component using the created user. If password is provided by system (not injected
     * by user), after failing to establish connection, it will be reset.
     *
     * @param schemaName Given schema name.
     * @param dbUsername Given db user name.
     * @param dbPassword Given db password.
     * @param isPasswordInjected is password injected by user.
     * @return DataSource from which database connection can be obtained..
     */
    @Nonnull
    public DataSource dataSourceConfig(@Nonnull final String schemaName,
            @Nonnull final String dbUsername, @Nonnull final String dbPassword,
            final boolean isPasswordInjected) {
        DataSource dataSource = dataSource(getSQLConfigObject().getDbUrl(), dbUsername, dbPassword,
                dbMinPoolSize, dbMaxPoolSize, isConnectionPoolActive);
        try {
            // Test DB connection first with to the schema under given user credentials.
            // We use an un-pooled datasource because otherwise, failures can cause long waits.
            new MariaDbDataSource(getSQLConfigObject().getDbUrl())
                    .getConnection(dbUsername, dbPassword).close();
            logger.info("DB connection is available to schema '{}' from user '{}'.", schemaName,
                    dbUsername);
            // Run flyway migration with given user.
            flyway(schemaName, dataSource);
            return dataSource;
        } catch (SQLException sqlException) {
            // SQLException will be thrown if given db schema name or db user does not exist or
            // password has been changed. This is a valid case.
            logger.info(
                    "Database schema '{}' or user '{}' does not exist or password has been changed. "
                            + "Initializing schema and user under root credentials is needed.",
                    schemaName, dbUsername);
        }
        final String dbRootPassword = getDBRootPassword(false);
        DataSource rootDataSource =
                dataSource(getSQLConfigObject().getDbRootUrl(), dbRootUsername, dbRootPassword,
                        dbMinPoolSize, dbMaxPoolSize, false);
        try (Connection rootConnection = rootDataSource.getConnection()) {
            if (hasGrantPrivilege(rootConnection, dbRootUsername, dbRootPassword, schemaName)) {
                // explicitly create the needed database if it doesn't exist, so we can run
                // migrations with non-root user
                createDatabase(rootConnection, schemaName);
                // Allow given user to access the database (= grants.sql):
                final String requestUser = "'" + dbUsername + "'@'%'";
                grantDbPrivileges(schemaName, dbPassword, rootConnection, requestUser);
                // now run flyway using our non-root connection
                flyway(schemaName, dataSource);
            } else {
                logger.error("Database connection is not available with root credentials" +
                        " or doesn't have GRANT permission. Failed " +
                        "to create db user {} for schema {}.", dbUsername, schemaName);
                throw new BeanCreationException(dbRootUsername +
                        " does NOT have GRANT permission to reset component user: " + dbUsername);
            }
        } catch (SQLException e) {
            logger.error("Database connection is not available with root credentials. Failed " +
                    "to create db user {} for schema {}.", dbUsername, schemaName);
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
        return dataSource;
    }

    protected void createDatabase(
            @Nonnull final Connection connection, @Nonnull final String schemaName) throws SQLException {
        logger.info("Ensuring that database {} exists", schemaName);
        try (Statement statement = connection.createStatement()) {
            statement
                .execute(String.format("CREATE DATABASE IF NOT EXISTS `%s`", schemaName));
        }
    }

    protected void grantDbPrivileges(@Nonnull final String schemaName,
            @Nonnull final String dbPassword, final Connection rootConnection, final String requestUser)
            throws SQLException {
        logger.info("Initialize permissions for {} on `{}`...", requestUser, schemaName);
        logger.info("Removing {} db user if it exists.", requestUser);
        dropDbUser(rootConnection, requestUser);
        logger.info("Creating {} db user.", requestUser);
        try (Statement statement = rootConnection.createStatement()) {
            statement.execute(
                String.format("CREATE USER %s IDENTIFIED BY '%s'", requestUser, dbPassword));
        }
        logger.info("Granting ALL privileges for database {} to user {}.", schemaName, requestUser);
        try (Statement statement = rootConnection.createStatement()) {
            statement.execute(
                    String.format("GRANT ALL PRIVILEGES ON `%s`.* TO %s", schemaName, requestUser));
        }
        try (Statement statement = rootConnection.createStatement()) {
            statement.execute("FLUSH PRIVILEGES");
        }
    }

    /**
     * Check if the DB connection has GRANT permission.
     *
     * @param rootConnection root connection.
     * @param rootUser       root user name.
     * @param rootPass       root user password.
     * @param schemaName     requested schema name.
     * @return true if have grant privilege.
     */
    @VisibleForTesting
    static boolean hasGrantPrivilege(@Nonnull final Connection rootConnection,
            @Nonnull final String rootUser, @Nonnull final String rootPass,
            @Nonnull String schemaName) {
        logger.info("Checking if {} has GRANT permission.", rootUser);

        try {
            dropDbUser(rootConnection, turboTestUser);
            try (Statement statement = rootConnection.createStatement()) {
                statement
                    .execute(
                            String.format("GRANT ALL PRIVILEGES ON `%s`.* TO %s IDENTIFIED BY '%s'",
                                    schemaName, turboTestUser, rootPass));
            }
            dropDbUser(rootConnection, turboTestUser);
            return true;
        } catch (SQLException e) {
            logger.debug("{} does NOT have GRANT permission. Skip dropping component DB user.",
                    rootUser);
            return false;
        }
    }

    private static void dropDbUser(@Nonnull final Connection rootConnection, @Nonnull final String user) {
        try (Statement statement = rootConnection.createStatement()) {
            statement.execute(
                    // DROP USER IF EXISTS does not appear until MySQL 5.7, and breaks Jenkins build
                    String.format("DROP USER %s", user));
        } catch (SQLException e) {
            // did not previously exist
        }
    }

    /**
     * Get DB root password. If DB password passed in from environment, use it;
     * Next, if `isGettingPassFromAuth` is true, return from Auth component, otherwise
     * return the default root DB password.
     *
     * @param isGettingPassFromAuth should get DB root password from auth component?
     * @return DB root password.
     *
     */
    @VisibleForTesting
    String getDBRootPassword(boolean isGettingPassFromAuth) {
        if (!Strings.isEmpty(dbRootPassword)) {
            return dbRootPassword;
        } else if (isGettingPassFromAuth) {
            return getDbPasswordUtil().getSqlDbRootPassword();
        }
        return getDbPasswordUtil().obtainDefaultPW();
    }

    // Lazy initialization due to auth connection parameters are injected by Spring.
    synchronized DBPasswordUtil getDbPasswordUtil() {
        if (dbPasswordUtil != null) {
            return dbPasswordUtil;
        }
        dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySecs);
        return dbPasswordUtil;
    }

    // use for testing only
    @VisibleForTesting
    void setDbPasswordUtil(@Nonnull final DBPasswordUtil dbPasswordUtil) {
        this.dbPasswordUtil = dbPasswordUtil;
    }

    // use for testing only
    @VisibleForTesting
    void setDbRootPassword(@Nonnull final String  dbRootPassword) {
        this.dbRootPassword = dbRootPassword;
    }

    /**
     * Enable sub class to overwrite DB migration location.
     *
     * @param migrationLocation DB migration location.
     */
    protected void setMigrationLocation(@Nonnull final String migrationLocation) {
        this.migrationLocation = migrationLocation;
    }

    /**
     * A value object contains all the SQL connection parameters.
     */
    @Immutable
    public static class SQLConfigObject {
        private final String dbRootUrl;
        private final String dbUrl;
        private final String dbHost;
        private final int dbPort;
        private final SQLDialect sqlDialect;
        private final boolean isSecureDBConnectionRequested;
        private final Map<SQLDialect, String> driverPropertiesMap;
        private final Optional<UsernamePasswordCredentials> rootCredentials;


        /**
         * Create a new instance.
         *  @param dbHost                        host name or IP address of DB server
         * @param dbPort                        port to access DB server
         * @param dbSchemaName                  DB schema name.
         * @param rootCredentials               authentication rootCredentials for DB
         * @param sqlDialectName                JOOQ dialect name for DB server
         * @param isSecureDBConnectionRequested true if connection should be secure
         * @param driverPropertiesMap           map of driver property strings keyed by dialect
         */
        public SQLConfigObject(@Nonnull final String dbHost, @Nonnull final int dbPort,
                               @Nonnull String dbSchemaName,
                               @Nonnull final Optional<UsernamePasswordCredentials> rootCredentials,
                               @Nonnull final String sqlDialectName,
                               final boolean isSecureDBConnectionRequested,
                               @Nonnull final Map<SQLDialect, String> driverPropertiesMap) {
            this.dbHost = dbHost;
            this.dbPort = dbPort;
            this.rootCredentials = rootCredentials;
            this.sqlDialect = SQLDialect.valueOf(sqlDialectName);
            this.isSecureDBConnectionRequested = isSecureDBConnectionRequested;
            this.driverPropertiesMap = driverPropertiesMap;
            this.dbRootUrl = createDbUrlBuilder(isSecureDBConnectionRequested).build().toUriString();
            this.dbUrl = createDbUrlBuilder(isSecureDBConnectionRequested).path(dbSchemaName)
                .build().toUriString();
        }

        /**
         * Return the connection URL to access the database.
         *
         * @return DB connection URL.
         */
        @Nonnull
        public String getDbUrl() {
            return dbUrl;
        }

        /**
         * Return DB root connection URL to access the database.
         *
         * @return DB root connection URL.
         */
        @Nonnull
        public String getDbRootUrl() {
            return dbRootUrl;
        }

        /**
         * Return the DB server host.
         *
         * @return host name or IP address
         */
        @Nonnull
        public String getDbHost() {
            return dbHost;
        }

        public int getDbPort() {
            return dbPort;
        }

        /**
         * Get login root credentials.
         *
         * @return login root credentials, if available.
         */
        @Nonnull
        public Optional<UsernamePasswordCredentials> getRootCredentials() {
            return rootCredentials;
        }

        /**
         * Get the JOOQ dialect for this DB connection.
         *
         * @return dialect enum value
         */
        @Nonnull
        public SQLDialect getSqlDialect() {
            return sqlDialect;
        }

        /**
         * Get secure connection requirement.
         *
         * @return true of a secure connection is required
         */
        public boolean isSecureDBConnectionRequested() {
            return isSecureDBConnectionRequested;
        }

        /**
         * Get the local DB driver properties that should be included in the DB connection URL.
         *
         * <p>The properties are returned in the form they will appear in the URL, i.e. query
         * string format, with no initial "?" or "&" prefix.</p>
         *
         * @return driver properties query string
         */
        @Nonnull
        public String getDriverProperties() {
            final String driverProperties = driverPropertiesMap.get(sqlDialect);
            if (driverProperties == null) {
                throw new IllegalArgumentException(
                        String.format("No DB driver properties configured for dialect %s",
                                sqlDialect));
            }
            return driverProperties;
        }

        private UriComponentsBuilder createDbUrlBuilder(boolean isSecureDBConnectionRequested) {
            final UriComponentsBuilder urlBuilder = UriComponentsBuilder.newInstance()
                    .scheme("jdbc:" + getSqlDialect().name().toLowerCase())
                    .host(dbHost)
                    .port(dbPort);
            String driverProperties = getDriverProperties();
            if (StringUtils.isNotEmpty(driverProperties)) {
                urlBuilder.query(driverProperties);
            }
            if (isSecureDBConnectionRequested) {
                logger.info("Enabling secure DB connection with host: {}, port: {}", dbHost, dbPort);
            }
            return isSecureDBConnectionRequested ? urlBuilder
                    .queryParam("useSSL", "true")
                    .queryParam("trustServerCertificate", "true")  : urlBuilder;
        }
    }

    @Autowired
    private ProcessListClassifier processListClassifier;

    /** Seconds between reports logged by DbMonitor. */
    @Value("${dbMonitorIntervalSec:60}")
    private int dbMonitorIntervalSec;

    /**
     * Time threshold for a process to be considered long-running.
     *
     * <p>Once the process is in that category at least two consecutive cycles, it will be logged
     * individually.</p>
     */
    @Value("${longRunningQueryThresholdSecs:300}")
    private int longRunningQueryThresholdSecs;

    @Bean
    public DbMonitor dbMonitorLoop() {
        return new DbMonitor(processListClassifier, dsl(), dbMonitorIntervalSec,
                longRunningQueryThresholdSecs, getDbSchemaName(), getDbUsername());
    }

    @Nullable
    @Bean
    public HikariPoolMonitor poolMonitor() {
        final DataSource dataSource = dataSource();
        if (isConnectionPoolActive && poolMonitorIntervalSec > 0
                && dataSource instanceof HikariDataSource) {
            final HikariDataSource ds = (HikariDataSource)dataSource;
            final HikariPoolMonitor poolMonitor = new HikariPoolMonitor(ds.getPoolName(), poolMonitorIntervalSec,
                    getDbSchemaName(), poolMonitorExecutorService());
            ds.setMetricRegistry(poolMonitor.getMetricRegistry());
            return poolMonitor;
        } else {
            logger.debug("The pool monitor will not be used.");
            return null;
        }
    }

    @Nonnull
    @Bean
    public ScheduledExecutorService poolMonitorExecutorService() {
        final String poolName = DbConnectionPoolConfig.generatePoolName(getDbSchemaName());
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("DbPoolMonitor-" + poolName + "-%d").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    public void startDbMonitor() {
        logger.info("Starting Database monitor");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("db-monitor-%d")
                .setDaemon(true)
                .build();
        threadFactory.newThread(this::runDbMonitor).start();
    }

    private void runDbMonitor() {
        try {
            dbMonitorLoop().run();
        } catch (InterruptedException e) {
            logger.error("Monitoring interrupted; db monitoring suspended");
        }
    }
}

