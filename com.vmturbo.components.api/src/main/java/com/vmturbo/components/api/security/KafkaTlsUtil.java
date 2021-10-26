package com.vmturbo.components.api.security;

import java.util.Objects;
import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Kafka utility class.
 */
public class KafkaTlsUtil {

    private static final String M_TLS_ENABLEMENT_FAILED = "mTLS enablement failed: ";
    private static final String TRUSTSTORE_LOCATION_IS_NOT_INJECTED =
            M_TLS_ENABLEMENT_FAILED + "Truststore location is not injected";
    private static final String KEY_PASSWORD_IS_NOT_INJECTED =
            M_TLS_ENABLEMENT_FAILED + "Key password is not injected";
    private static final String TRUSTSTORE_PASSWORD_IS_NOT_INJECTED =
            M_TLS_ENABLEMENT_FAILED + "Truststore password is not injected";
    private static final String KEYSTORE_LOCATION_IS_NOT_INJECTED =
            M_TLS_ENABLEMENT_FAILED + "Keystore location is not injected";
    private static final String KEYSTORE_PASSWORD_IS_NOT_INJECTED =
            M_TLS_ENABLEMENT_FAILED + "Keystore password is not injected";

    // Utility class
    private KafkaTlsUtil() {}

    /**
     * Helper to add Kafka TLS properties to configuration.
     *
     * @param props source properties object.
     * @param kafkaTlsProperty Kafka TLS properties.
     */
    public static void addSecurityProps(@Nonnull final Properties props,
            @Nonnull final KafkaTlsProperty kafkaTlsProperty) {
        if (kafkaTlsProperty.getTlsEnabled()) {
            props.put("security.protocol", "SSL");
            props.put("ssl.truststore.location",
                    Objects.requireNonNull(kafkaTlsProperty.getTlsTrustStore(),
                            TRUSTSTORE_LOCATION_IS_NOT_INJECTED));
            props.put("ssl.truststore.password",
                    Objects.requireNonNull(kafkaTlsProperty.getTlsTrustStorePass(),
                            TRUSTSTORE_PASSWORD_IS_NOT_INJECTED));
            props.put("ssl.keystore.location",
                    Objects.requireNonNull(kafkaTlsProperty.getTlsKeyStore(),
                            KEYSTORE_LOCATION_IS_NOT_INJECTED));
            props.put("ssl.keystore.password",
                    Objects.requireNonNull(kafkaTlsProperty.getTlsKeystorePass(),
                            KEYSTORE_PASSWORD_IS_NOT_INJECTED));
            props.put("ssl.key.password", Objects.requireNonNull(kafkaTlsProperty.getTlsKeyPass(),
                    KEY_PASSWORD_IS_NOT_INJECTED));
            props.put("ssl.enabled.protocols", kafkaTlsProperty.getTlsEnabledProtocols());
        }
    }
}