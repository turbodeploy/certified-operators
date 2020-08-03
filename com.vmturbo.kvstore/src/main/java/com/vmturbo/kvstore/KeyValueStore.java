package com.vmturbo.kvstore;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * A backend-agnostic implementation for a simple string-based key-value store.
 *
 * All key-value store operations are guaranteed to retry in a blocking manner until they succeed.
 * We made this design choice under the assumption that the key-value store will be
 * highly available and will essentially never go down.
 *
 * <p>The intended use case is for small amounts of runtime data.
 * TODO (Gary Zeng, Oct 28, 2019) fix the "infinite retry" logic. See OM-51986 for details.
 */
public interface KeyValueStore {
    /**
     * Put a value in the key value store.
     *
     * @param key The key at which to put the value
     * @param value The value where the key should be put
     */
    void put(@Nonnull final String key, @Nonnull final String value);

    /**
     * Get a value as an {@link Optional<String>} from the key value store.
     *
     * @param key The key whose value should be retrieved.
     * @return The value associated with the key. If the value does not exist, returns empty.
     */
    @Nonnull Optional<String> get(@Nonnull final String key);

    /**
     * Get a value as a string from the key value store, or return a default if the key does not exist.
     *
     * @param key The key whose value should be retrieved.
     * @param defaultValue The default value to return if the key is not present.
     * @return The value associated with the key. If the value does not exist, returns the default value.
     */
    @Nonnull String get(@Nonnull final String key,
                        @Nonnull final String defaultValue);

    /**
     * Get the list of string values stored at keys that begin with a prefix.
     * Calling with an empty prefix is the equivalent of getting all values.
     *
     * @param keyPrefix The prefix that the keys must start with.
     * @return The map of key -> value where each key begins with the prefix.
     */
    @Nonnull
    Map<String, String> getByPrefix(@Nonnull final String keyPrefix);

    /**
     * Check if the store contains the given key. Returns true if the key exists, false otherwise.
     * Will return true even if the key contains only subkeys and is not associated with a value.
     *
     * @param key The key whose existence should be checked.
     * @return true if the key exists, false otherwise.
     */
    boolean containsKey(@Nonnull final String key);

    /**
     * Delete keys start with prefix from the key value store. No effect if the key is absent.
     * It's similar to: consul kv delete -recurse prefix
     *
     * @param prefix The key to be deleted
     */
    void removeKeysWithPrefix(@Nonnull final String prefix);


    /**
     * Delete a key from the key value store. No effect if the key is absent.
     * It's similar to: consul kv delete key
     *
     * @param key The key to be deleted
     */
    void removeKey(@Nonnull final String key);
}
