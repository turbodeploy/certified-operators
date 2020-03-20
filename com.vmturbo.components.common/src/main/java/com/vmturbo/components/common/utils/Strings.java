package com.vmturbo.components.common.utils;

import java.util.Objects;

import com.google.common.base.Joiner;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * General purpose string utility methods.
 * Note on naming: don't want to be confused or overlap with excellent apache commons `StringUtils`
 */
public class Strings {

    private Strings() {
    }

    private static final Logger logger = LogManager.getLogger();

    public static String concat(String... parts) {
        return Joiner.on("").skipNulls().join(parts);
    }

    public static String toString(Object obj) {
        return Objects.toString(obj, "");
    }

    public static Integer parseInteger(String integer) {
        return parseInteger(integer, null);
    }

    public static Integer parseInteger(String integer, Integer defaultValue) {
        try {
            return Integer.parseInt(StringUtils.trimToEmpty(integer));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse string to integer", e);
            return defaultValue;
        }
    }

    /**
     * Return the given string, truncated if needed so as not to exceed the given maximum length.
     *
     * @param s         string value
     * @param maxLength maximum allowed length
     * @return possibly truncated string value
     */
    public static String truncate(String s, int maxLength) {
        return s.length() > maxLength ? s.substring(0, maxLength) : s;
    }
}
