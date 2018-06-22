package com.vmturbo.kvstore;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

/**
 * {@inheritDoc}
 */
public class PublicKeyStore implements IPublicKeyStore {
    public static final String PUBLIC_KEY = "public_key";
    private final String namespace;
    private final ConsulKeyValueStore consulKeyValueStore;
    public PublicKeyStore(final String namespace,
                          final String consulHost,
                          final String consulPort,
                          final long kvStoreRetryIntervalMillis,
                          final TimeUnit milliseconds) {
        this.namespace = namespace;
        this.consulKeyValueStore = new ConsulKeyValueStore(PUBLIC_KEY,
                consulHost,
                consulPort,
                kvStoreRetryIntervalMillis,
                milliseconds);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNamespace() {
        return namespace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putPublicKey(@Nonnull final String value) {
        consulKeyValueStore.put(namespace, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getPublicKey(@Nonnull final String namespace) {
        return consulKeyValueStore.get(namespace);
    }

}
