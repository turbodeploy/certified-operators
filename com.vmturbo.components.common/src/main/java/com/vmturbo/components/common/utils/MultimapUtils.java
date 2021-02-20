package com.vmturbo.components.common.utils;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;

/**
 * A set of utility methods for working with {@link com.google.common.collect.Multimap} variations.
 */
public class MultimapUtils {

    private MultimapUtils() {}

    /**
     * Mirrors the {@link java.util.Map#computeIfAbsent(Object, Function)} behavior for {@link ListMultimap}.
     * @param multimap The target {@link ListMultimap} instance.
     * @param key The target key.
     * @param compute The compute function, invoked if the {@code multimap} does not contain {@code key}.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return The value associated with {@code key}. May be null, depending on the behavior of {@code compute}.
     */
    @Nullable
    public static <K, V> List<V> computeIfAbsent(@Nonnull ListMultimap<K, V> multimap,
                                                       @Nonnull K key,
                                                       @Nonnull Function<K, List<V>> compute) {
        if (multimap.containsKey(key)) {
            return multimap.get(key);
        } else {
            final List<V> newValue = compute.apply(key);

            if (newValue != null) {
                multimap.putAll(key, newValue);
            }
            return newValue;
        }
    }

    /**
     * Mirrors the {@link java.util.Map#computeIfAbsent(Object, Function)} behavior for {@link SetMultimap}.
     * @param multimap The target {@link SetMultimap} instance.
     * @param key The target key.
     * @param compute The compute function, invoked if the {@code multimap} does not contain {@code key}.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return The value associated with {@code key}. May be null, depending on the behavior of {@code compute}.
     */
    @Nullable
    public static <K, V> Set<V> computeIfAbsent(@Nonnull SetMultimap<K, V> multimap,
                                                @Nonnull K key,
                                                @Nonnull Function<K, Set<V>> compute) {
        if (multimap.containsKey(key)) {
            return multimap.get(key);
        } else {
            final Set<V> newValue = compute.apply(key);

            if (newValue != null) {
                multimap.putAll(key, newValue);
            }
            return newValue;
        }
    }
}
