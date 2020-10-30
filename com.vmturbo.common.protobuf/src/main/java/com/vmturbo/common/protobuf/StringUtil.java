package com.vmturbo.common.protobuf;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.text.WordUtils;

/**
 * Miscellaneous string utilities for use with protobuf objects.
 */
public class StringUtil {
    /**
     * Pre-compile the pattern to make it a little faster.
     */
    private static Pattern camelCasePattern = Pattern.compile(String.format("%s|%s|%s",
            "(?<=[A-Z])(?=[A-Z][a-z])",
            "(?<=[^A-Z])(?=[A-Z])",
            "(?<=[A-Za-z])(?=[^A-Za-z])"));

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
        return camelCasePattern.matcher(str).replaceAll(" ");
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

    /**
     * Get a human readable description for a size given in bytes.
     * Provides precision to a single decimal point.
     *
     * @param sizeInBytes The size in bytes for which we should generate a human readable description.
     * @return a human readable description for a size given in bytes.
     */
    public static String getHumanReadableSize(long sizeInBytes) {
        if (sizeInBytes < 0) {
            return "-" + getHumanReadableSize(-sizeInBytes);
        }

        // Reference:
        // https://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
        if (sizeInBytes < 1024) {
            return sizeInBytes + " Bytes";
        }
        long value = sizeInBytes;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && sizeInBytes > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }

        if (value % 1024 == 0) {
            return String.format("%.0f %cB", value / 1024.0, ci.current());
        }
        return String.format("%.1f %cB", value / 1024.0, ci.current());
    }
}
