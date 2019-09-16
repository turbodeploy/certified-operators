package com.vmturbo.common.protobuf;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.text.WordUtils;

/**
 * Miscellaneous string utilities for use with protobuf objects.
 */
public class StringUtil {
    /**
     * Convert camel case (e.g. PhysicalMachine) into strings with the same
     * capitalization plus blank spaces (e.g. "Physical Machine"). It also splits numbers,
     * e.g. "May5" -> "May 5" and respects upper case runs, e.g. (PDFLoader -> "PDF Loader").
     *
     * The regex uses zero-length pattern matching with look-behind and look-forward, and is
     * taken from - http://stackoverflow.com/questions/2559759.
     *
     * @param str any string
     * @return see description
     */
    public static String getSpaceSeparatedWordsFromCamelCaseString(@Nonnull final String str) {
        return str.replaceAll(String.format("%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"),
                " ");
    }

    /**
     * Formats the given string by replacing underscores (if they exist) with spaces and returning
     * the new string in "Title Case" format.
     * e.g. VIRTUAL_MACHINE -> Virtual Machine.
     * e.g. SUSPEND -> Suspend.
     *
     * @param str The string that will be formatted.
     * @return The formatted string.
     */
    public static String beautifyString(@Nonnull final String str) {
        return WordUtils.capitalize(str.replace("_"," ").toLowerCase());
    }

}
