package com.vmturbo.auth.api.db;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.components.api.ComponentRestTemplate;

/**
 * Contains the method to retrieve the root password for XL databases (SQL, Arango, Influx, etc.)
 */
public class DBPasswordUtil {
    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger(
            DBPasswordUtil.class);

    public static final String SECURESTORAGE_PATH = "/securestorage/";
    public static final String SQL_DB_ROOT_PASSWORD_PATH = "getSqlDBRootPassword";
    public static final String SQL_DB_ROOT_USERNAME_PATH = "getSqlDBRootUsername";
    public static final String ARANGO_DB_ROOT_PASSWORD_PATH = "getArangoDBRootPassword";
    public static final String INFLUX_DB_ROOT_PASSWORD_PATH = "getInfluxDBRootPassword";
    public static final String POSTGRES_DB_ROOT_USERNAME_PATH = "getPostgresDBRootUsername";

    /**
     * The database root username.
     */
    private String dbRootUsername;

    /**
     * The database root password.
     */
    private String dbRootPassword;

    /**
     * The UTF-8 charset
     */
    private static final Charset DB_PASSW_CHARSET = Charset.forName("UTF-8");

    /**
     * The default SQL DB password. We repeat the password string to confuse the enemy.
     * The Base64-encoded variant has been precomputed.
     * TODO: https://vmturbo.atlassian.net/browse/OM-34291
     */
    private static final String DEFAULT_SQL_DB_PASSWORD = "dm10dXJib3ZtdHVyYm8=";

    /**
     * The default Arango password. We repeat the password string three times to confuse the enemy.
     * The Base64-encoded variant has been precomputed.
     * TODO: https://vmturbo.atlassian.net/browse/OM-34291
     */
    private static final String ARANGO_DB_DEFAULT_PASSWORD = "cm9vdHJvb3Ryb290";

    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate;
    private final String authHost;
    private final int authPort;
    private final String authRoute;
    private final int authRetryDelaySecs;

    /**
     * Constructs the DBUtil.
     *
     * @param authHost The auth component host.
     * @param authPort The auth component port.
     * @param authRoute The auth component route, to use as a prefix for auth URIs.
     * @param authRetryDelaySecs number of seconds to delay between connection retries
     */
    public DBPasswordUtil(String authHost, int authPort, String authRoute, int authRetryDelaySecs) {
        this.authHost = authHost;
        this.authPort = authPort;
        this.authRoute = authRoute;
        this.authRetryDelaySecs = authRetryDelaySecs;
        restTemplate = ComponentRestTemplate.create();
    }

    /**
     * Obtains the default DB password.
     * We have Base64 encoded default password repeating itself twice.
     * So we decode it.
     *
     * @return The default password.
     */
    public static @Nonnull String obtainDefaultPW() {
        String defPwd = new String(
                Base64.getDecoder().decode(DEFAULT_SQL_DB_PASSWORD.getBytes(DB_PASSW_CHARSET)),
                DB_PASSW_CHARSET);
        return defPwd.substring(0, defPwd.length() / 2);
    }

    /**
     * Obtains the default Arango password.
     * We have Base64 encoded default password repeating itself three times.
     * So we decode it.
     *
     * @return The default password.
     */
    public static @Nonnull String obtainDefaultArangoPW() {
        String defPwd = new String(
                Base64.getDecoder().decode(ARANGO_DB_DEFAULT_PASSWORD.getBytes(DB_PASSW_CHARSET)),
                DB_PASSW_CHARSET);
        return defPwd.substring(0, defPwd.length() / 3);
    }

    /**
     * Obtains the default InfluxDB password.
     *
     * @return The default password.
     */
    public static @Nonnull String obtainDefaultInfluxPW() {
        // Influx has the same default root password as Arango.
        return obtainDefaultArangoPW();
    }

    /**
     * Obtains the default root DB username.
     *
     * @param sqlDialect type of the sql database
     * @return The default root DB username
     */
    public static String obtainDefaultRootDbUser(String sqlDialect) {
        switch (sqlDialect) {
            case "POSTGRES":
                return "postgres";
            case "MYSQL":
            case "MARIADB":
                return "root";
            default:
                throw new UnsupportedOperationException("No default root username defined for: " + sqlDialect);
        }
    }

    /**
     * Fetch the the SQL database root password from the Auth component.
     *
     * In case we have an error obtaining the database root password from the auth component,
     * retry continually with a configured delay between each retry.
     *
     * If the auth component is down and the database root password has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @return The SQL database root password.
     */
    public synchronized @Nonnull String getSqlDbRootPassword() {
        return getRootPassword(SQL_DB_ROOT_PASSWORD_PATH, "SQL");
    }

    /**
     * Fetch the the SQL database root username from the Auth component.
     *
     * In case we have an error obtaining the database root username from the auth component,
     * retry continually with a configured delay between each retry.
     *
     * If the auth component is down and the database root username has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @param sqlDialect the type of the sql database
     * @return The SQL database root password.
     */
    public synchronized @Nonnull String getSqlDbRootUsername(String sqlDialect) {
        switch (sqlDialect) {
            case "POSTGRES":
                // for postgres, default root username is postgres
                return getRootUser(POSTGRES_DB_ROOT_USERNAME_PATH);
            case "MYSQL":
            case "MARIADB":
            default:
                // for other sql dbs like mysql, mariadb, default root username is root
                return getRootUser(SQL_DB_ROOT_USERNAME_PATH);
        }
    }

    /**
     * Fetch the the Arango database root password from the Auth component.
     *
     * In case we have an error obtaining the database root password from the auth component,
     * retry continually with a configured delay between each retry.
     *
     * If the auth component is down and the database root password has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @return The Arango database root password.
     */
    public synchronized @Nonnull String getArangoDbRootPassword() {
        return getRootPassword(ARANGO_DB_ROOT_PASSWORD_PATH, "Arango");
    }

    /**
     * Fetch the the Influx database root password from the Auth component.
     *
     * In case we have an error obtaining the database root password from the auth component,
     * retry continually with a configured delay between each retry.
     *
     * If the auth component is down and the database root password has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @return The Arango database root password.
     */
    public synchronized @Nonnull String getInfluxDbRootPassword() {
        return getRootPassword(INFLUX_DB_ROOT_PASSWORD_PATH, "Influx");
    }

    private @Nonnull String getRootUser(@Nonnull final String usernameKeyOffset) {
        for (int i = 1; dbRootUsername == null; i++) {
            // Obtains the database root username.
            // Since the password change in the database will require the JDBC pools to be
            // restarted, that implies we need to restart the history component. Which means
            // we can cache the username here.
            final String request = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(authHost)
                .port(authPort)
                .path(authRoute + SECURESTORAGE_PATH + usernameKeyOffset)
                .build().toUriString();
            try {
                ResponseEntity<String> result =
                    restTemplate.getForEntity(request, String.class);
                dbRootUsername = result.getBody();
                if (StringUtils.isEmpty(dbRootUsername)) {
                    throw new IllegalArgumentException("root db username is empty");
                }
            } catch (ResourceAccessException e) {
                logger.warn("...Unable to fetch the SQL database root name; sleep {} secs; try {}",
                    authRetryDelaySecs, i);
                try {
                    Thread.sleep(Duration.ofSeconds(authRetryDelaySecs).toMillis());
                } catch (InterruptedException e2) {
                    logger.warn("...Auth connection retry sleep interrupted; still waiting");
                }
            }
        }
        return dbRootUsername;
    }

    /**
     * Retrieves a database root password from the Auth component.
     *
     * In case we have an error obtaining the database root password from the auth component,
     * retry continually with a configured delay between each retry.
     *
     * If the auth component is down and the database root password has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @return The database root password.
     */
    private @Nonnull String getRootPassword(@Nonnull final String passwordKeyOffset,
                                            @Nonnull final String databaseType) {
        for (int i = 1; dbRootPassword == null; i++) {
            // Obtains the database root password.
            // Since the password change in the database will require the JDBC pools to be
            // restarted, that implies we need to restart the history component. Which means
            // we can cache the password here.
            final String request = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(authHost)
                .port(authPort)
                .path(authRoute + SECURESTORAGE_PATH + passwordKeyOffset)
                .build().toUriString();
            try {
                ResponseEntity<String> result =
                    restTemplate.getForEntity(request, String.class);
                dbRootPassword = result.getBody();
                if (StringUtils.isEmpty(dbRootPassword)) {
                    throw new IllegalArgumentException("root " + databaseType + " db password is empty");
                }
            } catch (ResourceAccessException e) {
                logger.warn("...Unable to fetch the {} database root password; sleep {} secs; try {}",
                    databaseType, authRetryDelaySecs, i);
                try {
                    Thread.sleep(Duration.ofSeconds(authRetryDelaySecs).toMillis());
                } catch (InterruptedException e2) {
                    logger.warn("...Auth connection retry sleep interrupted; still waiting");
                }
            }
        }
        return dbRootPassword;
    }

    /**
     * Get the rest template used in all requests.
     *
     * @return The rest template used in all requests.
     */
    @VisibleForTesting
    RestTemplate getRestTemplate() {
        return restTemplate;
    }
}
