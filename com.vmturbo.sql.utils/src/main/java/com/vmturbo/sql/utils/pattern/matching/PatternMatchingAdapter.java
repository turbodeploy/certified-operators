package com.vmturbo.sql.utils.pattern.matching;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;

/**
 * Adapter to support multiple databases with different pattern matching behavior.
 */
public interface PatternMatchingAdapter {
    /**
     * Create a SQL regex matching condition between a string field and a regex.
     *
     * @param fieldToMatch A string field.
     * @param regex The regex we want to match.
     * @param positiveMatch Whether we want positive or negative match.
     * @param caseSensitive Whether we want the matching to be case-sensitive or insensitive.
     * @return A SQL regex {@link Condition} matching the {@code fieldToMatch} to the {@code regex}.
     */
    @Nonnull
    Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive);

    /**
     * Create a SQL regex matching condition between a string field and a regex.
     *
     * @param fieldToMatch A string field.
     * @param regex The regex we want to match.
     * @param positiveMatch Whether we want positive or negative match.
     * @param caseSensitive Whether we want the matching to be case-sensitive or insensitive.
     * @param characterSet The MariaDB character set of the {@code fieldToMatch} in case it is other than "utf8mb4".
     * @return A SQL regex {@link Condition} matching the {@code fieldToMatch} to the {@code regex}.
     */
    @Nonnull
    Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive,
            @Nonnull String characterSet);
}
