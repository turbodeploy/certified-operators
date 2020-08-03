package com.vmturbo.components.common.config;

/**
 * A simple source for configuration properties. Can be used to read configuration information
 * dynamically, rather than just at startup time, and is intended to support realtime updates to
 * configuration information.
 */
public interface IConfigSource {

    /**
     * Get a property value with specified type.
     * @param <T> not a param but checkstyle certainly thinks it is
     * @param name the property name to look up
     * @param type the java class type to return
     * @param defaultValue default value to return, if no specific value for the property is found
     * @return the resolved config value
     */
    <T> T getProperty(String name, Class<T> type, T defaultValue);

    // in the future, this interface will support reload and update events
}
