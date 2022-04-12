package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.config.SecretPropertiesReader.CLIENT_SECRET;
import static com.vmturbo.components.common.config.SecretPropertiesReader.PASSWORD;
import static com.vmturbo.components.common.config.SecretPropertiesReader.componentSecretKeyMap;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.Logger;

/**
 * Utility to check if the data is sensitive.
 */
public final class SensitiveDataUtil {

    /**
     * Mark sensitive data, such as password.
     */
    public static final String ASTERISKS = "xxxxx";
    // Other sensitive keys that are not currently included in component secrets.
    private static final Set<String> additionalSensitiveKeySet = ImmutableSet.of(
            "arangodbPass",
            "userPassword",
            "sslKeystorePassword",
            "readonlyPassword",
            "dbRootPassword",
            "ssl.keystore.password",
            "ssl.key.password",
            "ssl.truststore.password",
            "dbs.grafana.password",
            "grafanaAdminPassword",
            "grafanaDb.password");

    // utility class
    private SensitiveDataUtil() {
    }

    /**
     * Check if the key has sensitive data.
     *
     * @param obj key to check if it's sensitive key
     * @return true if the key is identified as sensitive key.
     */
    public static boolean hasSensitiveData(@Nonnull final Object obj) {
        return getSensitiveKey().contains(obj) || additionalSensitiveKeySet.contains(obj);
    }

    /**
     * Safe logger to avoid logging sensitive property, such as password.
     *
     * @param key property key
     * @param value property value
     * @param logger logger
     */
    public static void safeLogger(@Nonnull final Object key, @Nonnull final String value,
            @Nonnull final Logger logger) {
        if (hasSensitiveData(key)) {
            logger.info("       {} = '{}'", key, ASTERISKS);
        } else {
            logger.info("       {} = '{}'", key, value);
        }
    }

    /**
     * Get sensitive keys.
     *
     * @return all the sensitive keys.
     */
    public static Set<String> getSensitiveKey() {
        final Set<String> sensitiveKeys = componentSecretKeyMap.entrySet().stream().map(
                Entry::getValue).map(e -> {
            if (e.containsKey(PASSWORD)) {
                return e.get(PASSWORD);
            } else if (e.containsKey(CLIENT_SECRET)) {
                return e.get(CLIENT_SECRET);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());

        return ImmutableSet.<String>builder().addAll(sensitiveKeys).addAll(
                additionalSensitiveKeySet).build();
    }
}
