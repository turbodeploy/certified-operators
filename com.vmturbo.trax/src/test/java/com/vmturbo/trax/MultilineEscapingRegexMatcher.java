package com.vmturbo.trax;

import javax.annotation.Nonnull;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * A small helper class to assist in matching regular expressions.
 */
public class MultilineEscapingRegexMatcher extends TypeSafeMatcher<String> {

    private final String[] regexes;
    private String reason;

    private MultilineEscapingRegexMatcher(@Nonnull final String unescapedRegex) {
        regexes = escapeAndSingleLine(unescapedRegex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean matchesSafely(String s) {
        final String[] splits = s.split("\\n");
        if (splits.length != regexes.length) {
            reason = String.format("expression to contain %s lines but contained %s",
                regexes.length, splits.length);
            return false;
        }

        for (int i = 0; i < splits.length; i++) {
            if (!splits[i].matches(regexes[i])) {
                reason = String.format("'%s' to match '%s'", splits[i], regexes[i]);
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void describeTo(Description description) {
        description.appendText(reason);
    }

    private static String[] escapeAndSingleLine(@Nonnull String regex) {
        regex = regex.replaceAll("\\(", "\\\\("); // Replace braces and brackets
        regex = regex.replaceAll("\\)", "\\\\)");
        regex = regex.replaceAll("\\[", "\\\\[");
        regex = regex.replaceAll("\\]", "\\\\]");
        regex = regex.replaceAll("\\|", "\\\\|");
        regex = regex.replaceAll("\\_", "\\\\_");

        regex = regex.replaceAll(" \\/ ", " \\\\/ "); // Replaces arithmetic (surrounded by whitespace
        regex = regex.replaceAll(" \\+ ", " \\\\+ "); // because we don't want to replace the '+' in \d+)
        regex = regex.replaceAll(" \\* ", " \\\\* ");

        regex = regex.replaceAll("\\:", "\\\\:"); // Replace the colon

        return regex.split("\\n");
    }

    /**
     * Create a new MultilineEscapingRegexMatcher.
     *
     * @param pattern The pattern to match.
     * @return A new MultilineEscapingRegexMatcher.
     */
    public static MultilineEscapingRegexMatcher matchesPattern(@Nonnull final String pattern) {
        return new MultilineEscapingRegexMatcher(pattern);
    }
}
