package com.vmturbo.kvstore;

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
    private final ConsulKeyValueStore consulKeyValueStore;

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
        this.consulKeyValueStore = new ConsulKeyValueStore(consulNamespacePrefix,
                PUBLIC_KEY,
                consulHost,
                consulPort,
                kvStoreTimeoutSeconds,
                seconds);
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

    /**
     * USE WITH CAUTION - mainly for test idemnpotency.
     */
    public void removePublicKey() {
        logger.warn("Removing public key at namespace {}", namespace);
        consulKeyValueStore.removeKey(namespace);
    }

}
