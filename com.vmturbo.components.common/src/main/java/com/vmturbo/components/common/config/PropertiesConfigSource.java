package com.vmturbo.components.common.config;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple map-based config source. Mutable. This class is used for testing, so there is no real
 * type-checking or type conversion support.
 */
public class PropertiesConfigSource implements IConfigSource {
    private final Map<String, Object> properties = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getProperty(final String name, final Class<T> type, final T defaultValue) {
        if (properties.containsKey(name)) {
            return (T)properties.get(name);
        }
        return defaultValue;
    }

    /**
     * Set an property value.
     * @param name the name of the property to set
     * @param newValue value to set it to
     * @param <T> the property type
     */
    public <T> void setProperty(final String name, final T newValue) {
        properties.put(name, newValue);
    }
}
