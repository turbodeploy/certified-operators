package com.vmturbo.auth.component;

import java.sql.SQLException;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.annotation.EnableTransactionManagement;

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
@EnableTransactionManagement
public class AuthDBConfig extends SQLDatabaseConfig {
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
    @VisibleForTesting
    static final String CONSUL_KEY = "dbcreds";

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
     * DB user name accessible to given schema.
     */
    @Value("${authDbUsername:auth}")
    private String authDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${authDbPassword:}")
    private String authDbPassword;

    /**
     * The DB schema name.
     */
    @Value("${dbSchemaName}")
    private String dbSchemaName;

    /**
     * The subpath to use to look for Flyway migrations in the classpath.
     * If empty, Flyway will look through all files in the classpath for migration files that
     * match the flyway naming convention.
     * If set, Flyway will only look in the specified folder (relative to the classpath).
     */
    @Value("${migrationLocation:}")
    private String migrationLocation;

    /**
     * The REST config.
     */
    @Autowired
    private AuthRESTSecurityConfig authRESTSecurityConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

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
        // Both dbRootUsername and dbRootPassword have default values defined in {@link @SQLDatabaseConfig.java},
        // So credential should not be null.
        if (getSQLConfigObject().getRootCredentials().isPresent()) {
            String rootDBUser = getSQLConfigObject().getRootCredentials().get().getUserName();
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
        if (getSQLConfigObject().getRootCredentials().isPresent()) {
            String dbPassword = getSQLConfigObject().getRootCredentials().get().getPassword();
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
        Optional<String> credentials = authKVConfig.authKeyValueStore().get(CONSUL_KEY);
        String dbPassword;
        if (StringUtils.isNotEmpty(authDbPassword)) {
            // Use authDbPassword if specified as environment variable.
            dbPassword = authDbPassword;
        } else if (!credentials.isPresent()) {
            // else if db credentials do not exist in Consul, use the same externalized admin db
            // password as prefix and append it with random characters, and store the encrypted db
            // password in Consul.
            dbPassword = getRootSqlDBPassword() + generatePassword();
            authKVConfig.authKeyValueStore().put(CONSUL_KEY, CryptoFacility.encrypt(dbPassword));
        } else {
            // else, use the decrypted db password stored in Consul.
            dbPassword = getDecryptPassword(credentials.get());
        }
        // Get DataSource from the given DB schema name and user. Make sure we have the proper user
        // here. If the user does not exist, create it under root connection.
        DataSource dataSource = dataSourceConfig(dbSchemaName, authDbUsername, dbPassword);

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

    /**
     * Get plan text password from encrypted the cipher text.
     * Note: if the password is not encrypted, we will encrypted and persistent it.
     *
     * @param encryptedPassword encrypted password
     * @return plan text password
     */
    @VisibleForTesting
    String getDecryptPassword(@Nonnull final String encryptedPassword) {
        try {
            return CryptoFacility.decrypt(encryptedPassword);
        } catch (SecurityException e) {
            logger.debug("Auth db user password is in plain text, will encrypt it.");
            authKVConfig.authKeyValueStore()
                    .put(CONSUL_KEY, CryptoFacility.encrypt(encryptedPassword));
            return encryptedPassword;
        }
    }

    @Bean
    public SecureStorageController secureStorageController() {
        return new SecureStorageController(secureDataStore());
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
     * Returns the DB-backed secure store.
     *
     * @return The DB-backed secure store.
     */
    @Bean
    public ISecureStore secureDataStore() {
        return new DBStore(dsl(), authKVConfig.authKeyValueStore(), getDbUrl());
    }

    /**
     * Creates the database URL.
     *
     * @return The database URL.
     */
    private String getDbUrl() {
        return getSQLConfigObject().getDbUrl();
    }

    /**
     * For testing only.
     *
     * @param config {@link AuthKVConfig}.
     */
    @VisibleForTesting
    AuthDBConfig(@Nonnull final AuthKVConfig config) {
        this.authKVConfig = config;
    }
}
