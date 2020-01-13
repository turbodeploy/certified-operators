package com.vmturbo.kvstore;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Map-backed implementation of KeyValueStore for use in test configurations.
 */
public class MapKeyValueStore implements KeyValueStore {

    private final Map<String, String> map = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        map.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public String get(String key, String defaultValue) {
        String val = map.get(key);
        return val == null ? defaultValue : val;
    }

    @Override
    public Map<String, String> getByPrefix(String keyPrefix) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(keyPrefix))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public boolean containsKey(@Nonnull final String key) {
        return map.containsKey(key);
    }

    @Override
    public void removeKeysWithPrefix(@Nonnull String prefix) {
        map.entrySet().removeIf(entry -> entry.getKey().startsWith(prefix));
    }

    @Override
    public void removeKey(@Nonnull final String key) {
        map.remove(key);
    }

    public void clear() {
        map.clear();
    }
}
