package com.vmturbo.components.common.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * A property source based on spring config.
 */
@Configuration
public class SpringConfigSource implements IConfigSource {

    // normal use case -- Environment is injected by spring. Alternatively, can be passed in via the
    // constructor arg.
    @Autowired
    protected Environment environment;

    /**
     * This class can optionally be bootstrapped directly with a {@link Environment}, instead of
     * relying on spring annotations/DI.
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
