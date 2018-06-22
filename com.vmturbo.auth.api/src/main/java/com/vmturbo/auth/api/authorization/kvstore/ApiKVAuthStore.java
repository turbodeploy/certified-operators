package com.vmturbo.auth.api.authorization.kvstore;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.kvstore.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The consul-backed target store uses consul purely as a
 * backup for consistency across restarts.
 */
@ThreadSafe
public class ApiKVAuthStore implements IApiAuthStore {

    /**
     * The logger
     */
    private final Logger logger = LogManager.getLogger(ApiKVAuthStore.class);

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;

    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock = new Object();

    /**
     * Constructs the KV store.
     *
     * @param keyValueStore The underlying store backend.
     */
    public ApiKVAuthStore(@Nonnull final KeyValueStore keyValueStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
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
}
