package com.vmturbo.components.common.utils;

import java.util.Optional;

import javax.annotation.Nonnull;

public class EnvironmentUtils {

    /**
     * Simple utility to fetch a value from the java Environment and convert to an int.
     *
     * @param propKeyToFetch the key for the value to fetch from the Java env
     * @return the int value of the string corresponding to the given 'propKeyToFetch'
     * @throws NumberFormatException if the string fetched from the Java env cannot be converted
     *                               to an int
     */
    public static int parseIntegerFromEnv(String propKeyToFetch) {
        final String propValue = System.getenv(propKeyToFetch);
        try {
            return Integer.valueOf(propValue);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Invalid property '" + propKeyToFetch + "' value:  >" +
                    propValue + "< - should be an integer");
        }
    }

    /**
     * Returns environment variable value.
     *
     * @param propertyName environment variable name
     * @return evironment variable value
     * @throws IllegalStateException if there is not such environment property set
     */
    @Nonnull
    public static String requireEnvProperty(@Nonnull String propertyName) {
        return getOptionalEnvProperty(propertyName)
            .orElseThrow(() -> new IllegalStateException("System or environment property \"" + propertyName + "\" must be set"));
    }

    /**
     * Return the value of a system property or environment value (in that order) with the specified
     * name, if one is defined, otherwise empty.
     *
     * @param propertyName The name of the system or environment property to check.
     * @return An Optional of the system property value or environment variable, if one was found.
     */
    public static Optional<String> getOptionalEnvProperty(@Nonnull String propertyName) {
        final String sysPropValue = System.getProperty(propertyName);
        if (sysPropValue != null) {
            return Optional.of(sysPropValue);
        }
        final String envPropValue = System.getenv(propertyName);
        if (envPropValue != null) {
            return Optional.of(envPropValue);
        }
        return Optional.empty();
    }
}
