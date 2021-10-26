package com.vmturbo.components.api.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

/**
 * Base configuration for Kafka, holding kafka servers initial list as well as the namespace used
 * for prefix of all topics and consumer groups to support multi XL deployments on a single Kafka.
 */
public class TlsConfig {

    @Value("${TLS_KEYSTORE_PASS:}")
    private String tlsKeystorePass;

    @Value("${TLS_KEY_PASS:}")
    private String tlsKeyPass;

    @Value("${TLS_KEYSTORE:}")
    private String tlsKeyStore;

    @Value("${TLS_TRUSTSTORE:}")
    private String tlsTrustStore;

    @Value("${TLS_TRUSTSTORE_PASS:}")
    private String tlsTrustStorePass;

    /**
     * Get TLS key password.
     *
     * @return tlsKeyPass
     */
    @Bean
    public String tlsKeyPass() {
        return tlsKeyPass;
    }

    /**
     * Get TLS keystore password.
     *
     * @return tlsKeystorePass
     */
    @Bean
    public String tlsKeystorePass() {
        return tlsKeystorePass;
    }

    /**
     * Get TLS key store.
     *
     * @return tlsKeyStore
     */
    @Bean
    public String tlsKeyStore() {
        return tlsKeyStore;
    }

    /**
     * Get TLS key store password.
     *
     * @return tlsTrustStorePass
     */
    @Bean
    public String tlsTrustStorePass() {
        return tlsTrustStorePass;
    }

    /**
     * Get TLS trust store password.
     *
     * @return tlsTrustStore
     */
    @Bean
    public String tlsTrustStore() {
        return tlsTrustStore;
    }
}