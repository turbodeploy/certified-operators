package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.sql.DataSource;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.FlywayCallback;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.common.utils.EnvironmentUtils;

/**
 * Configuration for interaction with database.
 *
 * <p>Components that want to connect to the database should import this configuration into their
 * Spring context with an @Import annotation (please do not @ComponentScan the sql.utils package!).
 * </p>
 */
@Configuration
@EnableTransactionManagement
public class SQLDatabaseConfig {

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

    @Value("${dbSchemaName}")
    private String dbSchemaName;

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

    @Value("{mariadbDriverProperties}")
    private String mariadbDriverProperties;

    @Value("{mysqlDriverProperties}")
    private String mysqlDriverProperties;

    private static final Logger logger = LogManager.getLogger();

    @Bean
    @Primary
    public DataSource dataSource() {
        return dataSource(getSQLConfigObject().getDbRootUrl(), dbRootUsername, getDBRootPassword());
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
                                    @Nonnull String dbPassword) {
        MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(dbUrl);
            dataSource.setUser(dbUsername);
            dataSource.setPassword(dbPassword);
            return dataSource;
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
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));

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
        String dbPassword = getDBRootPassword();
        final Optional<UsernamePasswordCredentials> rootCredentials = (dbRootUsername != null && dbPassword != null) ?
            Optional.ofNullable(new UsernamePasswordCredentials(dbRootUsername, dbPassword)) : Optional.empty();
        final Map<SQLDialect, String> driverPropertiesMap = ImmutableMap.of(
                SQLDialect.MARIADB, mariadbDriverProperties,
                SQLDialect.MYSQL, mysqlDriverProperties);
        return new SQLConfigObject(dbHost, dbPort, dbSchemaName, rootCredentials, sqlDialectName,
            isSecureDBConnectionRequested, driverPropertiesMap);
    }

    /**
     * Set up DB configuration. If DB connection is available with the provided user credentials and
     * schema, then access the database in this component with the given user; else if DB connection
     * is not available with the given schema and user, use root credentials to run flyway migration
     * to create the DB schema and create DB user with the given credentials, and then access the
     * database in the component using the created user.
     *
     * @param schemaName Given schema name.
     * @param dbUsername Given db user name.
     * @param dbPassword Given db password.
     * @return DataSource from which database connection can be obtained..
     */
    @Nonnull
    protected DataSource dataSourceConfig(@Nonnull String schemaName, @Nonnull String dbUsername,
                                          @Nonnull String dbPassword) {
        DataSource dataSource = dataSource(getSQLConfigObject().getDbUrl(), dbUsername, dbPassword);
        try {
            // Test DB connection first to the schema under given user credentials.
            dataSource.getConnection().close();
            logger.info("DB connection is available to schema '{}' from user '{}'.", schemaName, dbUsername);
            // Run flyway migration with given user.
            flyway(schemaName, dataSource);
            return dataSource;
        } catch (SQLException sqlException) {
            // SQLException will be thrown if given db schema name or db user does not exist or
            // password has been changed. This is a valid case.
            logger.info("Database schema '{}' or user '{}' does not exist or password has been changed. " +
                "Initializing schema and user under root credentials is needed.", schemaName, dbUsername);
        }
        DataSource rootDataSource = dataSource(getSQLConfigObject().getDbRootUrl(),
            dbRootUsername, getDBRootPassword());
        try (Connection rootConnection = rootDataSource.getConnection()) {
            // Run flyway migration under root credentials.
            flyway(schemaName, rootDataSource);
            // Clean up existing db user, if it exists.
            try (PreparedStatement stmt = rootConnection.prepareStatement(
                "DROP USER ?@'%';")) {
                stmt.setString(1, dbUsername);
                stmt.execute();
                logger.info("Cleaned up '{}@%' db user.", dbUsername);
            } catch (SQLException e) {
                // SQLException will be thrown when trying to drop not existed username% user in DB. It's valid case.
                logger.info("'{}@%' user is not in the DB, clean up is not needed.", dbUsername);
            }
            // Create db user.
            try (PreparedStatement stmt = rootConnection.prepareStatement(
                "CREATE USER ?@'%' IDENTIFIED BY ?;")) {
                stmt.setString(1, dbUsername);
                stmt.setString(2, dbPassword);
                stmt.execute();
                logger.info("Created '{}@%' db user.", dbUsername);
            }
            // Grant db user privileges
            try (PreparedStatement stmt = rootConnection.prepareStatement(
                "GRANT ALL PRIVILEGES ON " + schemaName + ".* TO ?@'%';")) {
                stmt.setString(1, dbUsername);
                stmt.execute();
                logger.info("Granted all privileges on schema '{}' to '{}@%' db user.", schemaName, dbUsername);
            }
            // Flush user privileges
            try (PreparedStatement stmt = rootConnection.prepareStatement("FLUSH PRIVILEGES;")) {
                stmt.execute();
                logger.info("Flushed DB privileges on schema '{}' to user '{}'.", schemaName, dbUsername);
            }
        } catch (SQLException e) {
            logger.error("Database connection is not available with root credentials. Failed " +
                "to create db user {} for schema {}.", dbUsername, schemaName);
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
        return dataSource;
    }

    /**
     * Get DB root password. If DB password passed in from environment, use it; otherwise use the
     * default root password from DBPasswordUtil.
     *
     * @return DB root password.
     */
    private String getDBRootPassword() {
        return !Strings.isEmpty(dbRootPassword) ? dbRootPassword : DBPasswordUtil.obtainDefaultPW();
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
}

