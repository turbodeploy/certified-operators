package com.vmturbo.auth.api.db;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Contains the method to retrieve the root password.
 */
public class DBPasswordUtil {
    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger(
            DBPasswordUtil.class);

    /**
     * The database root password.
     */
    private String dbRootPassword;

    /**
     * The UTF-8 charset
     */
    private static final Charset DB_PASSW_CHARSET = Charset.forName("UTF-8");

    /**
     * The default DB password. We repeat the password string to confuse the enemy.
     * The Base64-encoded variant has been precomputed.
     */
    private static final String DEFAULT_DB_PASSWORD = "dm10dXJib3ZtdHVyYm8=";

    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate;

    /**
     * Constructs the DBUtil.
     */
    public DBPasswordUtil() {
        restTemplate = new RestTemplate();
        final Jaxb2RootElementHttpMessageConverter msgConverter =
                new Jaxb2RootElementHttpMessageConverter();
        restTemplate.getMessageConverters().add(msgConverter);
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
                Base64.getDecoder().decode(DEFAULT_DB_PASSWORD.getBytes(DB_PASSW_CHARSET)),
                DB_PASSW_CHARSET);
        return defPwd.substring(0, defPwd.length() / 2);
    }

    /**
     * Retrieves the database root password.
     * In case we have an error obtaining the database root password from the auth component,
     * return the default.
     * This could happen in one of two cases:
     * <ul>
     * <li>We are running unit tests</li>
     * <li>The auth component is down</li>
     * </ul>
     * If the auth component is down and the database root password has been changed, there will be
     * no security implications, as the component will not be able to access the database..
     *
     * @param authHost The auth component host.
     * @param authPort The auth component port.
     * @return The database root password.
     */
    public synchronized @Nonnull String getRootPassword(final @Nonnull String authHost,
                                                        final int authPort) {
        if (dbRootPassword == null || Objects.equals(dbRootPassword, obtainDefaultPW())) {
            // Obtains the database root password.
            // Since the password change in the database will require the JDBC pools to be
            // restarted, that implies we need to restart the history component. Which means
            // we can cache the password here.
            final String request = UriComponentsBuilder.newInstance()
                                                       .scheme("http")
                                                       .host(authHost)
                                                       .port(authPort)
                                                       .path("/securestorage/getDBRootPassword")
                                                       .build().toUriString();
            try {
                ResponseEntity<String> result =
                        restTemplate.getForEntity(request, String.class);
                dbRootPassword = result.getBody();
            } catch (ResourceAccessException e) {
                logger.error("Unable to obtain the database root password. Returning default.");
                return obtainDefaultPW();
            }
        }
        return dbRootPassword;
    }

}
