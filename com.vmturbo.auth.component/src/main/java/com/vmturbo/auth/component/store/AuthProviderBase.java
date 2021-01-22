package com.vmturbo.auth.component.store;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.kvstore.KeyValueStore;

/**
 * Auth provider base class.
 */
public class AuthProviderBase {
    /**
     * The KV AD prefix.
     */
    protected static final String PREFIX_AD = "ad/info";
    protected static final String PREFIX_GROUP = "groups/";
    /**
     * KV prefix for external group users.
     */
    protected static final String PREFIX_GROUP_USERS = "groupusers/";
    protected static final Gson GSON = new GsonBuilder().create();
    /**
     * The KV prefix.
     */
    @VisibleForTesting
    static final String PREFIX = "users/";
    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    protected final @Nonnull
    KeyValueStore keyValueStore_;
    /**
     * Locks for write operations on target storages.
     */
    protected final Object storeLock_ = new Object();

    /**
     * Constructor.
     * @param keyValueStore key value store.
     */
    public AuthProviderBase(@Nonnull final KeyValueStore keyValueStore) {
        keyValueStore_ = Objects.requireNonNull(keyValueStore);
    }

    /**
     * Retrieves the value for the key from the KV store.
     *
     * @param key The key.
     * @return The Optional value.
     */
    protected Optional<String> getKVValue(final @Nonnull String key) {
        synchronized (storeLock_) {
            return keyValueStore_.get(key);
        }
    }

    /**
     * Puts the value for the key into the KV store.
     *
     * @param key   The key.
     * @param value The value.
     */
    protected void putKVValue(final @Nonnull String key, final @Nonnull String value) {
        synchronized (storeLock_) {
            keyValueStore_.put(key, value);
        }
    }

    /**
     * Removes the value for the key in the KV store.
     * It's similar to: consul kv delete key
     *
     * @param key The key.
     */
    protected void removeKVKey(final @Nonnull String key) {
        synchronized (storeLock_) {
            keyValueStore_.removeKey(key);
        }
    }

    /**
     * Delete keys start with prefix from the key value store. No effect if the key is absent.
     * It's similar to: consul kv delete -recurse prefix.
     *
     * @param prefix The prefix in kv store.
     */
    protected void removeKVKeysWithPrefix(final @Nonnull String prefix) {
        synchronized (storeLock_) {
            keyValueStore_.removeKeysWithPrefix(prefix);
        }
    }
}
