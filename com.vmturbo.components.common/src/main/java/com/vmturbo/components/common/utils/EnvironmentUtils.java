package com.vmturbo.components.common.utils;

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
}