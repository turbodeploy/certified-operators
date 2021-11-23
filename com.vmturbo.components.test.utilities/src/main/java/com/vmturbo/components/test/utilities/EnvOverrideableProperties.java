package com.vmturbo.components.test.utilities;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;


/**
 * Utility to provide default values for OS-level environment variables.
 * The purpose is to reduce the verbosity of "get this system property, or this default
 * if it doesn't exist" and improve code readability.
 */
@Immutable
public class EnvOverrideableProperties {

    /**
     * The default values to use when the system has no value
     * for the properties.
     */
    private final Map<String, String> defaultProperties;

    private EnvOverrideableProperties(@Nonnull final Map<String, String> defaultProperties) {
        this.defaultProperties = defaultProperties;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Retrieve the value of a property.
     *
     * @param name The name of the property. The name must be one of the names provided at
     *            build time (via {@link Builder#addProperty(String, String)}).
     * @return The current value of the property in the system's environment. If the property
     *     does not exist in the environment, returns the default provided at build time.
     *     Multiple calls with the same name may return different values if the value of the
     *     property changed in the system's environment between the calls.
     */
    @Nonnull
    public String get(@Nonnull final String name) {
        final String defaultValue = defaultProperties.get(name);
        if (defaultValue == null) {
            throw new IllegalArgumentException("Undefined property: " + name);
        }
        final String systemValue = System.getenv(name);
        return systemValue == null ? defaultValue : systemValue;
    }

    /**
     * Builder for the {@link EnvOverrideableProperties}.
     */
    public static class Builder  {

        private final ImmutableMap.Builder<String, String> defaultsBuilder = ImmutableMap.builder();

        @Nonnull
        public Builder addProperty(@Nonnull final String name, @Nonnull final String value) {
            defaultsBuilder.put(name, value);
            return this;
        }

        @Nonnull
        public EnvOverrideableProperties build() {
            return new EnvOverrideableProperties(defaultsBuilder.build());
        }
    }
}
