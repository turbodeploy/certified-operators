package com.vmturbo.auth.component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
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

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.auth.component.services.SecureStorageController;
import com.vmturbo.auth.component.store.DBStore;
import com.vmturbo.auth.component.store.ISecureStore;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.sql.utils.JooqExceptionTranslator;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for AUTH component interaction with a database.
 */
@Configuration
@Import({SQLDatabaseConfig.class})
@EnableTransactionManagement
public class AuthDBConfig {
    /**
     * The connection reconnect sleep time.
     */
    private static final long RECONNECT_SLEEP_MS = 10000L;

    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(AuthDBConfig.class);

    /**
     * The Consul key
     */
    private static final String CONSUL_KEY = "dbcreds";

    /**
     * The Consul root DB username key.
     */
    public static final String CONSUL_ROOT_DB_USER_KEY = "rootdbUsername";

    /**
     * The Consul root DB password key.
     */
    public static final String CONSUL_ROOT_DB_PASS_KEY = "rootdbcreds";

    /**
     * The arango root DB password key.
     */
    public static final String ARANGO_ROOT_PW_KEY = "arangocreds";

    /**
     * The influx root DB password key.
     */
    public static final String INFLUX_ROOT_PW_KEY = "influxcreds";

    /**
     * The DB schema name.
     */
    @Value("${dbSchemaName}")
    private String dbSchemaName;

    /**
     * The REST config.
     */
    @Autowired
    private AuthRESTSecurityConfig authRESTSecurityConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    /**
     * Generate a random password.  The value is returned as a sequence of 32 hexadecimal
     * characters.
     *
     * @return The generated password.
     * @throws SecurityException In case of any error generating the password.
     */
    private @Nonnull String generatePassword() throws SecurityException {
        return Hex.encodeHexString(CryptoFacility.getRandomBytes(16));
    }

    /**
     * Enjoy an uninterrupted sleep.
     *
     * @param ms Sleep soundly for {@code ms} milliseconds.
     */
    private void uninterruptedSleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e1) {
            // Ignore the interrupt.
        }
    }

    /**
     * Returns the root SQL DB username.
     * 1. if db username passed in from environment, use it, also store it to Consul.
     * 2. if not passed in, try to get it from Consul.
     * 3. if not in Consul, use default username.
     *
     * @return The root DB password.
     */
    @Bean
    public String getRootSqlDBUser() {
        // Both dbUsername and dbUserPassword have default values defined in {@link @SQLDatabaseConfig.java},
        // So credential should not be null.
        if (databaseConfig.getSQLConfigObject().getCredentials().isPresent()) {
            String rootDBUser = databaseConfig.getSQLConfigObject().getCredentials().get().getUserName();
            // It will be updated every time auth is started, so we always have the latest root db user.
            authKVConfig.authKeyValueStore().put(CONSUL_ROOT_DB_USER_KEY, rootDBUser);
            return rootDBUser;
        }
        // it should not be reached.
        Optional<String> rootDbUser = authKVConfig.authKeyValueStore().get(CONSUL_ROOT_DB_USER_KEY);
        if (rootDbUser.isPresent()) {
            return rootDbUser.get();
        }
        return DBPasswordUtil.obtainDefaultRootDbUser();
    }

    /**
     * Returns the root SQL DB password.
     * 1. if db password passed in from environment, use it, also store the encrypted value to Consul
     * 2. if not passed in, try to get it from Consul.
     * 3. if not in Consul, use default password
     *
     * @return The root DB password.
     */
    @Bean
    public  @Nonnull String getRootSqlDBPassword() {
        if (databaseConfig.getSQLConfigObject().getCredentials().isPresent()) {
            String dbPassword = databaseConfig.getSQLConfigObject().getCredentials().get().getPassword();
            // It will be updated every time auth is started, so we always have the latest root db password.
            authKVConfig.authKeyValueStore().put(CONSUL_ROOT_DB_PASS_KEY, CryptoFacility.encrypt(dbPassword));
            return dbPassword;
        }
        Optional<String> rootDbPassword = authKVConfig.authKeyValueStore().get(CONSUL_ROOT_DB_PASS_KEY);
        if (rootDbPassword.isPresent()) {
            return CryptoFacility.decrypt(rootDbPassword.get());
        }
        return DBPasswordUtil.obtainDefaultPW();
    }

    /**
     * Returns the root Arango password.
     * In case the password is not yet encrypted and stored in Consul, do that.
     *
     * @return The root Arango password.
     */
    @Bean
    public  @Nonnull String getDefaultArangoRootPassword() {
        Optional<String> arangoDbPassword = authKVConfig.authKeyValueStore().get(ARANGO_ROOT_PW_KEY);
        if (arangoDbPassword.isPresent()) {
            return CryptoFacility.decrypt(arangoDbPassword.get());
        }
        String defaultPwd = DBPasswordUtil.obtainDefaultArangoPW();
        authKVConfig.authKeyValueStore().put(ARANGO_ROOT_PW_KEY, CryptoFacility.encrypt(defaultPwd));
        return defaultPwd;
    }
    /**
     * Returns the root Influx password.
     * In case the password is not yet encrypted and stored in Consul, do that.
     *
     * @return The root Influx password.
     */
    @Bean
    public  @Nonnull String getDefaultInfluxRootPassword() {
        Optional<String> influxDbPassword = authKVConfig.authKeyValueStore().get(INFLUX_ROOT_PW_KEY);
        if (influxDbPassword.isPresent()) {
            return CryptoFacility.decrypt(influxDbPassword.get());
        }
        String defaultPwd = DBPasswordUtil.obtainDefaultInfluxPW();
        authKVConfig.authKeyValueStore().put(INFLUX_ROOT_PW_KEY, CryptoFacility.encrypt(defaultPwd));
        return defaultPwd;
    }

    /**
     * Creates the data source.
     * The part of the process is to set the database user.
     * So, the secure storage database credentials will have to be kept in the Consul.
     *
     * @return The data source.
     */
    @Bean
    @Primary
    public @Nonnull DataSource dataSource() {
        // Create a correct data source.
        @Nonnull MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(getDbUrl());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Optional<String> credentials = authKVConfig.authKeyValueStore().get(CONSUL_KEY);
        try {
            if (credentials.isPresent()) {
                dataSource.setUser(dbSchemaName);
                dataSource.setPassword(credentials.get());
            } else {
                // Use the well worn out defaults.
                dataSource.setUser(getRootSqlDBUser());
                dataSource.setPassword(getRootSqlDBPassword());
            }
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }


        // Ensure the connection is available before proceeding.
        while (true) {
            try {
                dataSource.getConnection().close();
                return dataSource;
            } catch (SQLException | NullPointerException e) {
                logger.info("Connection is unavailable, wait for it", e);
                uninterruptedSleep(RECONNECT_SLEEP_MS);
            }
        }
    }

    @Bean
    public SecureStorageController secureStorageController() {
        return new SecureStorageController(secureDataStore());
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

    /**
     * Creates the JDBC exception translator used by the Spring.
     *
     * @return The JDBC exception translator used by the Spring.
     */
    @Bean
    public JooqExceptionTranslator exceptionTranslator() {
        return new JooqExceptionTranslator();
    }

    /**
     * Performs the database migration.
     */
    @Bean
    public Flyway performMigration() {
        // Perform the database migration.
        Flyway migrator = new Flyway();
        migrator.setDataSource(dataSource());
        migrator.setSchemas(dbSchemaName);
        migrator.migrate();
        return migrator;
    }

    /**
     * Creates the JOOQ configuration.
     *
     * @return The JOOQ configuration.
     */
    @Bean
    public DefaultConfiguration configuration() {
        // Perform migration first using root connection.
        performMigration();

        // We explicitly assume MySQL datasource here for a moment.
        MariaDbDataSource dataSource = (MariaDbDataSource)dataSource();
        try {
            dataSource.setUrl(getMariaDBDbUrl());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Create the user and grant privileges in case the user has not been yet created.
        Optional<String> credentials = authKVConfig.authKeyValueStore().get(CONSUL_KEY);
        try {
            if (!credentials.isPresent()) {
                // use the same externalized admin db password as prefix and append it with random characters.
                String dbPassword = getRootSqlDBPassword() + generatePassword();
                // Make sure we have the proper user here.
                // We call this only when the database user is not yet been created and set.
                // We are doing the inlined code here, since creating multiple beans causes the circular
                // dependencies in Sprint.
                dataSource.setUser(getRootSqlDBUser());
                dataSource.setPassword(getRootSqlDBPassword());
                try (Connection connection = dataSource.getConnection()) {

                    // Clean up existing auth db user, if it exists.
                    try (PreparedStatement stmt = connection.prepareStatement(
                            "DROP USER 'auth'@'%';")) {
                        stmt.execute();
                        logger.info("Cleaned up auth@% db user.");
                    } catch (SQLException e) {
                        // SQLException will be thrown when trying to drop not existed auth@% user in DB. It's valid case.
                        logger.info("auth@% user is not in the DB, clean up is not needed.");
                    }
                    // Create auth db user.
                    try (PreparedStatement stmt = connection.prepareStatement(
                            "CREATE USER 'auth'@'%' IDENTIFIED BY ?;")) {
                        stmt.setString(1, dbPassword);
                        stmt.execute();
                        logger.info("Created auth@% db user.");
                    }
                    // Grant auth db user privileges
                    try (PreparedStatement stmt = connection.prepareStatement(
                            "GRANT ALL PRIVILEGES ON auth.* TO 'auth'@'%';")) {
                        stmt.execute();
                        logger.info("Granted privileges to auth@% db user.");
                    }
                    // Flush user privileges
                    try (PreparedStatement stmt = connection.prepareStatement("FLUSH PRIVILEGES;")) {
                        stmt.execute();
                        logger.info("Flushed DB privileges.");
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                authKVConfig.authKeyValueStore().put(CONSUL_KEY, dbPassword);
                dataSource.setUser(dbSchemaName);
                dataSource.setPassword(dbPassword);
            }
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }

        // Create a JOOQ configuration.
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
        jooqConfiguration.set(connectionProvider());
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));
        jooqConfiguration.set(new Settings().withRenderNameStyle(RenderNameStyle.LOWER));
        jooqConfiguration.set(SQLDialect.valueOf(databaseConfig.getSQLConfigObject().getSqlDialect()));
        return jooqConfiguration;
    }

    /**
     * Force initialization of flyway before getting a reference to the database.
     *
     * @return The DSL context.
     */
    @Bean
    public DSLContext dslContext() {
        return new DefaultDSLContext(configuration());
    }

    /**
     * Returns the DB-backed secure store.
     *
     * @return The DB-backed secure store.
     */
    @Bean
    public ISecureStore secureDataStore() {
        return new DBStore(dslContext(), authKVConfig.authKeyValueStore(), getDbUrl());
    }

    /**
     * Creates the database URL.
     *
     * @return The database URL.
     */
    private String getDbUrl() {
        return databaseConfig.getSQLConfigObject().getDbUrl();
    }

    /**
     * Creates the MariaDB database URL.
     * Note:
     * Secure URL: jdbc:mysql://host:port?useSSL=true&trustServerCertificate=true, so will append "&" and
     * parameters. It will be: jdbc:mysql://host:port?useSSL=true&trustServerCertificate=true&useUnicode=true...
     * Insecure URL: jdbc:mysql://host:port, so will append "?" and parameters. It will be:
     * jdbc:mysql://host:port?useUnicode=true...
     * @TODO consider using {@link UriComponentsBuilder} to better build parameters.
     *
     * @return The MariaDB database URL.
     */
    @VisibleForTesting
    String getMariaDBDbUrl() {

        final String parameters = "useUnicode=true"
            + "&tcpRcvBuf=8192"
            + "&tcpSndBuf=8192"
            + "&characterEncoding=UTF-8"
            + "&characterSetResults=UTF-8"
            + "&connectionCollation=utf8_unicode_ci";
        return databaseConfig.getSQLConfigObject().isSecureDBConnectionRequested() ?
            databaseConfig.getSQLConfigObject().getDbUrl() + "&" + parameters :
            databaseConfig.getSQLConfigObject().getDbUrl() + "?" + parameters;
    }
}
