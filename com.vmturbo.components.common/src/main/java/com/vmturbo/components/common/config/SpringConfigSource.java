package com.vmturbo.components.common.config;

import org.springframework.core.env.Environment;

/**
 * A property source based on spring config.
 */
public class SpringConfigSource implements IConfigSource {

    private Environment environment;

    /**
     * Create spring config source based on the specified {@link Environment}.
     * @param environment a spring environment object containing the properties to read from.
     */
    public SpringConfigSource(Environment environment) {
        this.environment = environment;
    }

    @Override
    public <T> T getProperty(final String name, Class<T> type, final T defaultValue) {
        if (environment == null) {
            return defaultValue;
        }
        return environment.getProperty(name, type, defaultValue);
    }
}
