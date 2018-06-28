package com.vmturbo.kvstore;

import javax.annotation.Nonnull;

/**
 * Store to store and retrieve SAML configuration.
 * Currently it's back by console
 */
public interface ISAMLConfigurationStore {


    /**
     * Store SAML configuration.
     * @param key SAML configuration key
     * @param value SAML configuration value
     *
     */
    void put(@Nonnull final String key, @Nonnull final String value);

    /**
     * Get component namespace
     * @return current component namespace
     */
    String getNamespace();
}
