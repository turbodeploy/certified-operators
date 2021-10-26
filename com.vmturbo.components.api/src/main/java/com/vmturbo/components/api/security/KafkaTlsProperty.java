package com.vmturbo.components.api.security;

import javax.annotation.Nonnull;

/**
 * Value object for Kafka TLS properties.
 */
public class KafkaTlsProperty {
    final Boolean tlsEnabled;
    final String tlsKeyStore;
    final String tlsKeystorePass;
    final String tlsKeyPass;
    private final String tlsTrustStore;
    private final String tlsTrustStorePass;
    private final String tlsEnabledProtocols;

    /**
     * Constructor.
     *
     * @param tlsEnabled is TLS enabled
     * @param tlsKeyStore TLS key store
     * @param tlsKeystorePass TLS key store password
     * @param tlsKeyPass TLS key password
     * @param tlsTrustStore TLS trust store
     * @param tlsTrustStorePass TLS trust store password
     * @param tlsEnabledProtocols TLS supported protocols
     */
    public KafkaTlsProperty(final boolean tlsEnabled, @Nonnull final String tlsKeyStore,
            @Nonnull final String tlsKeystorePass, @Nonnull final String tlsKeyPass,
            @Nonnull final String tlsTrustStore, @Nonnull final String tlsTrustStorePass,
            @Nonnull final String tlsEnabledProtocols) {
        this.tlsEnabled = tlsEnabled;
        this.tlsKeyStore = tlsKeyStore;
        this.tlsKeystorePass = tlsKeystorePass;
        this.tlsKeyPass = tlsKeyPass;
        this.tlsTrustStore = tlsTrustStore;
        this.tlsTrustStorePass = tlsTrustStorePass;
        this.tlsEnabledProtocols = tlsEnabledProtocols;
    }

    /**
     * Getter.
     *
     * @return tlsEnabled
     */
    public boolean getTlsEnabled() {
        return tlsEnabled;
    }

    /**
     * Getter.
     *
     * @return tlsKeyStore
     */
    public String getTlsKeyStore() {
        return tlsKeyStore;
    }

    /**
     * Getter.
     *
     * @return tlsKeystorePass
     */
    public String getTlsKeystorePass() {
        return tlsKeystorePass;
    }

    /**
     * Getter.
     *
     * @return tlsKeyPass
     */
    public String getTlsKeyPass() {
        return tlsKeyPass;
    }

    /**
     * Getter.
     *
     * @return tlsTrustStore
     */
    public String getTlsTrustStore() {
        return tlsTrustStore;
    }

    /**
     * Getter.
     *
     * @return tlsTrustStorePass
     */
    public String getTlsTrustStorePass() {
        return tlsTrustStorePass;
    }

    /**
     * Getter.
     *
     * @return tlsEnabledProtocols
     */
    public String getTlsEnabledProtocols() {
        return tlsEnabledProtocols;
    }
}