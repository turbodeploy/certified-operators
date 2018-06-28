package com.vmturbo.kvstore;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

/**
 * {@inheritDoc}
 */
public class SAMLConfigurationStore implements ISAMLConfigurationStore {
    public static final String API_PATH = "components/api/defaults/";
    public static final String VMTURBO = "vmturbo";
    private final ConsulKeyValueStore consulKeyValueStore;


    private final String namespace;

    public SAMLConfigurationStore(@Nonnull final String namespace,
                                  @Nonnull final String consulHost,
                                  @Nonnull final String consulPort,
                                  @Nonnull final long kvStoreRetryIntervalMillis,
                                  @Nonnull final TimeUnit milliseconds) {
        this.consulKeyValueStore = new ConsulKeyValueStore(VMTURBO,
                consulHost,
                consulPort,
                kvStoreRetryIntervalMillis,
                milliseconds);
        this.namespace = namespace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(@Nonnull final String key, @Nonnull final String value) {
        consulKeyValueStore.put(API_PATH + key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNamespace() {
        return namespace;
    }
}
