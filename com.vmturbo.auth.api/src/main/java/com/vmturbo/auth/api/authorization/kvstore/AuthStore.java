package com.vmturbo.auth.api.authorization.kvstore;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.kvstore.IPublicKeyStore;
import com.vmturbo.kvstore.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The consul-backed target store uses consul purely as a
 * backup for consistency across restarts.
 */
@ThreadSafe
public class AuthStore implements IAuthStore {

    /**
     * The logger
     */
    private final Logger logger = LogManager.getLogger(AuthStore.class);

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;

    /**
     * The key/value store for other component's public key
     */
    @GuardedBy("storeLock")
    private final IPublicKeyStore publicKeyStore;

    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock = new Object();


    /**
     * Constructs the KV store.
     *
     * @param keyValueStore The underlying store backend.
     * @param publicKeyStore The store to retrieve component's public key
     */
    public AuthStore(@Nonnull final KeyValueStore keyValueStore,
                     @Nonnull final IPublicKeyStore publicKeyStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.publicKeyStore = Objects.requireNonNull(publicKeyStore);
    }

    /**
     * Retrieves the public key.
     *
     * @return The public key.
     */
    public String retrievePublicKey() {
        synchronized (storeLock) {
            try {
                Optional<String> key = keyValueStore.get(KV_KEY);
                if (key.isPresent()) {
                    return key.get();
                }
                return null;
            } catch (Exception e) {
                logger.error("Error retrieving the public key from the KV store.");
                return null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> retrievePublicKey(final String namespace) {
        return publicKeyStore.getPublicKey(namespace);
    }
}
