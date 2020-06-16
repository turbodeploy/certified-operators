package com.vmturbo.components.api;

import java.util.function.Supplier;

import com.google.common.base.Preconditions;

/**
 * Utility to help use a log4j2-like format for general strings (in exceptions, in toString()
 * methods, and anywhere else. Allows delayed/conditional evaluation of expensive strings.
 * For example: FormattedString.format("My {}", object);
 *
 * <p/>The ONLY acceptable delimiter is {}. There is no escaping delimiters - i.e. you can't do
 * \\{} and expect to see "{}" in the output - but you can use curly braces as long as there is
 * something between them (e.g: "I like to {", "I like to { curl }", "I like to { }")
 */
public class FormattedString implements Supplier<String> {
    private static final char DELIMITER_START = '{';
    private static final char DELIMITER_END = '}';

    private final String str;
    private final Object[] params;
    private final Supplier[] suppliers;

    /**
     * Create a {@link FormattedString} object with a template and a set of arguments.
     *
     * @param str The template.
     * @param params Parameters to insert into the template.
     */
    public FormattedString(String str, Object... params) {
        this.str = str;
        this.params = params;
        suppliers = null;
    }

    /**
     * Create a {@link FormattedString} object with a template and a set of suppliers.
     * Use this constructor if you may not want to evaluate the suppliers.
     *
     * @param str The template.
     * @param suppliers Suppliers of parameters to insert into the template.
     */
    public FormattedString(String str, Supplier... suppliers) {
        this.str = str;
        this.params = null;
        this.suppliers = suppliers;
    }

    /**
     * Format a string, replacing any "{}" with the provided parameters, in the order they appear.
     *
     * @param pattern The template string.
     * @param params The parameters.
     * @return A string equivalent to the input pattern with {} delimiters replaced by
     *         associated params.
     */
    public static String format(String pattern, Object... params) {
        // It will be AT LEAST as long as the string.
        final StringBuilder stringBuilder = new StringBuilder(pattern.length());
        int len = pattern.length();
        int paramIdx = 0;
        for (int i = 0; i < len; ++i) {
            final char curChar = pattern.charAt(i);
            if (curChar == DELIMITER_START) {
                int nextI = i + 1;
                if (nextI < len && pattern.charAt(nextI) == DELIMITER_END) {
                    stringBuilder.append(paramIdx < params.length ? params[paramIdx] : null);
                    paramIdx++;
                    // Skip the next character (which is the DELIMITER_END).
                    i++;
                } else {
                    stringBuilder.append(curChar);
                }
            } else {
                stringBuilder.append(curChar);
            }
        }
        return stringBuilder.toString();

    }

    @Override
    public String get() {
        Preconditions.checkArgument(params != null || suppliers != null);
        if (params == null) {
            Object[] objects = new Object[suppliers.length];
            for (int i = 0; i < suppliers.length; ++i ) {
                try {
                    objects[i] = suppliers[i].get();
                } catch (RuntimeException e) {
                    objects[i] = e;
                }
            }
            return format(str, objects);
        } else {
            return format(str, params);
        }
    }
}
