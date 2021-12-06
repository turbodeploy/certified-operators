package com.vmturbo.auth.component;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ARANGO_ROOT_PW_KEY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.CONSUL_KEY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.CONSUL_ROOT_DB_PASS_KEY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.CONSUL_ROOT_DB_USER_KEY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.INFLUX_ROOT_PW_KEY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.POSTGRES_ROOT_USER_KEY;

import java.sql.SQLException;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.auth.component.services.SecureStorageController;
import com.vmturbo.auth.component.store.DBStore;
import com.vmturbo.auth.component.store.ISecureStore;
import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.JooqExceptionTranslator;

/**
 * Configuration for AUTH component interaction with a database using DbEndpoint.
 * Note: At current state, this class duplicates logics from {@link AuthDBConfig} to avoid any
 * potential regressions.
 * We will consolidate them on future tasks.
 */
@Configuration
@EnableTransactionManagement
@Conditional(DbEndpointCondition.class)
public class AuthDbEndpointConfig extends DbEndpointsConfig {

    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(AuthDbEndpointConfig.class);

    /**
     * DB user name accessible to given schema.
     */
    @Value("${authDbUsername:auth}")
    private String authDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${authDbPassword:#{null}}")
    private String authDbPassword;

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
    private @Nonnull
    String generatePassword() throws SecurityException {
        return Hex.encodeHexString(CryptoFacility.getRandomBytes(16));
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
        if (StringUtils.isNotEmpty(super.dbRootUsername)) {
            String rootDBUser = super.dbRootUsername;
            // It will be updated every time auth is started, so we always have the latest root db user.
            authKVConfig.authKeyValueStore().put(CONSUL_ROOT_DB_USER_KEY, rootDBUser);
            return rootDBUser;
        }
        // it should not be reached.
        Optional<String> rootDbUser = authKVConfig.authKeyValueStore().get(CONSUL_ROOT_DB_USER_KEY);
        return rootDbUser.orElseGet(
                () -> DBPasswordUtil.obtainDefaultRootDbUser(SQLDialect.MYSQL.toString()));
    }

    /**
     * Setup the Postgres root username in consul.
     *
     * @return The Postgres root username.
     */
    @Bean
    public @Nonnull
    String getPostgresRootUsername() {
        Optional<String> postgresRootUsername = authKVConfig.authKeyValueStore().get(
                POSTGRES_ROOT_USER_KEY);
        if (postgresRootUsername.isPresent()) {
            return postgresRootUsername.get();
        }
        String defaultRootDbUser = DBPasswordUtil.obtainDefaultRootDbUser(
                SQLDialect.POSTGRES.toString());
        authKVConfig.authKeyValueStore().put(POSTGRES_ROOT_USER_KEY, defaultRootDbUser);
        return defaultRootDbUser;
    }

    /**
     * Returns the root SQL DB password.
     * 1. if db password passed in from environment, use it, also store the encrypted value to
     * Consul
     * 2. if not passed in, try to get it from Consul.
     * 3. if not in Consul, use default password
     *
     * @return The root DB password.
     */
    @Bean
    public @Nonnull
    String getRootSqlDBPassword() {
        if (StringUtils.isNotEmpty(super.dbRootPassword)) {
            String dbPassword = super.dbRootPassword;
            // It will be updated every time auth is started, so we always have the latest root db password.
            authKVConfig.authKeyValueStore().put(CONSUL_ROOT_DB_PASS_KEY,
                    CryptoFacility.encrypt(dbPassword));
            return dbPassword;
        }
        Optional<String> rootDbPassword = authKVConfig.authKeyValueStore().get(
                CONSUL_ROOT_DB_PASS_KEY);
        return rootDbPassword.map(CryptoFacility::decrypt).orElseGet(
                DBPasswordUtil::obtainDefaultPW);
    }

    /**
     * Returns the root Arango password.
     * In case the password is not yet encrypted and stored in Consul, do that.
     *
     * @return The root Arango password.
     */
    @Bean
    public @Nonnull
    String getDefaultArangoRootPassword() {
        Optional<String> arangoDbPassword = authKVConfig.authKeyValueStore().get(
                ARANGO_ROOT_PW_KEY);
        if (arangoDbPassword.isPresent()) {
            return CryptoFacility.decrypt(arangoDbPassword.get());
        }
        String defaultPwd = DBPasswordUtil.obtainDefaultArangoPW();
        authKVConfig.authKeyValueStore().put(ARANGO_ROOT_PW_KEY,
                CryptoFacility.encrypt(defaultPwd));
        return defaultPwd;
    }

    /**
     * Returns the root Influx password.
     * In case the password is not yet encrypted and stored in Consul, do that.
     *
     * @return The root Influx password.
     */
    @Bean
    public @Nonnull
    String getDefaultInfluxRootPassword() {
        Optional<String> influxDbPassword = authKVConfig.authKeyValueStore().get(
                INFLUX_ROOT_PW_KEY);
        if (influxDbPassword.isPresent()) {
            return CryptoFacility.decrypt(influxDbPassword.get());
        }
        String defaultPwd = DBPasswordUtil.obtainDefaultInfluxPW();
        authKVConfig.authKeyValueStore().put(INFLUX_ROOT_PW_KEY,
                CryptoFacility.encrypt(defaultPwd));
        return defaultPwd;
    }

    /**
     * Creates the data source.
     * The part of the process is to set the database user.
     * So, the secure storage database credentials will have to be kept in the Consul.
     * @return The data source.
     * @throws SQLException if there's a problem any of our db operations
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws InterruptedException if current thread has been interrupted.
     */
    @Bean
    @Primary
    public @Nonnull
    DataSource dataSource()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return this.authDbEndpoint().datasource();
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
            authKVConfig.authKeyValueStore().put(CONSUL_KEY,
                    CryptoFacility.encrypt(encryptedPassword));
            return encryptedPassword;
        }
    }

    /**
     * Secure controller bean.
     *
     * @return {@link SecureStorageController}
     */
    @Bean
    public SecureStorageController secureStorageController() {
        try {
            return new SecureStorageController(secureDataStore());
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create secure storage controller bean", e);
        }
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
     * @throws SQLException if there's a problem any of our db operations
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws InterruptedException if current thread has been interrupted.
     */
    @Bean
    public ISecureStore secureDataStore()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return new DBStore(this.authDbEndpoint().dslContext(), authKVConfig.authKeyValueStore(),
                "");
    }

    /**
     * Endpoint for accessing clustermgr database.
     *
     * @return endpoint instance
     * @throws UnsupportedDialectException when unsupported dialect found.
     */
    @Bean
    @Primary
    public DbEndpoint authDbEndpoint() throws UnsupportedDialectException {
        Optional<String> credentials = authKVConfig.authKeyValueStore().get(CONSUL_KEY);
        String dbPassword;
        final boolean isPasswordInjected = StringUtils.isNotEmpty(authDbPassword);
        if (isPasswordInjected) {
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

        return fixEndpointForMultiDb(dbEndpoint("dbs.auth", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpoint.DbEndpointAccess.ALL)
                .withRootAccessEnabled(true)
                .withRootUserName(isPostgresDialect() ? getPostgresRootUsername() : getRootSqlDBUser())
                .withRootPassword(getRootSqlDBPassword())
                .withUserName(authDbUsername)
                .withPassword(dbPassword))
                .build();
    }

    /**
     * Helper to return if it's Postgres for MariaDB/MySQL dialect.
     *
     * @return true if it's Postgres dialect, false if it's MariaDB/MySQL dialect.
     * @throws UnsupportedDialectException for an unsupported dialect
     */
    boolean isPostgresDialect() throws UnsupportedDialectException {
        switch (sqlDialect) {
            case MARIADB:
            case MYSQL:
                return false;
            case POSTGRES:
                return true;
            default:
                throw new UnsupportedDialectException(sqlDialect);
        }
    }
}
