package com.vmturbo.kvstore;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@inheritDoc}
 */
public class PublicKeyStore implements IPublicKeyStore {
    private static final Logger logger = LogManager.getLogger();
    public static final String PUBLIC_KEY = "public_key";
    private final String namespace;
    private final KeyValueStore keyValueStore;

    /**
     * Create a new {@link PublicKeyStore}.
     *
     * @param consulNamespacePrefix Given consulNamespacePrefix to be prepended to the given
     *                              namespace to construct consul keys.
     * @param namespace             Given Namespace to construct consul keys.
     * @param consulHost            Consul host.
     * @param consulPort            Consul port.
     * @param kvStoreTimeoutSeconds KV store timeout seconds.
     * @param seconds               Milliseconds time unit.
     */
    public PublicKeyStore(final String consulNamespacePrefix,
                          final String namespace,
                          final String consulHost,
                          final String consulPort,
                          final long kvStoreTimeoutSeconds,
                          final TimeUnit seconds) {
        this.namespace = namespace;
        this.keyValueStore = new ConsulKeyValueStore(consulNamespacePrefix,
                PUBLIC_KEY,
                consulHost,
                consulPort,
                kvStoreTimeoutSeconds,
                seconds);
    }

    /**
     * Create a new {@link PublicKeyStore} based on an existing KeyValueStore.
     *
     * @param keyValueStore         Given KeyValueStore to use for storing/retrieving public keys
     *                              namespace to construct consul keys.
     * @param namespace             Given Namespace to construct consul keys.
     */
    public PublicKeyStore(@Nonnull final KeyValueStore keyValueStore,
                          @Nonnull final String namespace) {
        this.namespace = Objects.requireNonNull(namespace);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
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
        keyValueStore.put(namespace, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getPublicKey(@Nonnull final String namespace) {
        return keyValueStore.get(namespace);
    }

    /**
     * USE WITH CAUTION - mainly for test idemnpotency.
     */
    public void removePublicKey() {
        logger.warn("Removing public key at namespace {}", namespace);
        keyValueStore.removeKey(namespace);
    }

}
