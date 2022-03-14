package com.vmturbo.sql.utils.pattern.matching;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * Pattern matching in MariaDB.
 */
public class MariaDBPatternMatching implements PatternMatchingAdapter {
    private static final String CASE_INSENSITIVE_COLLATION = "utf8mb4_unicode_ci";
    private static final String CASE_SENSITIVE_COLLATION = "utf8mb4_bin";

    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        // To specify case-sensitivity for regex matching in MariaDB, we change the collation on the fly.
        // Casting "fieldToMatch" to have character set "utf8mb4", which is the valid in both
        // MariaDB and MySQL, in order to use the same collation regardless the character set.
        fieldToMatch = DSL
                .field("cast({0} as CHAR(255) CHARACTER SET utf8mb4)", String.class,
                        fieldToMatch)
                .collate(caseSensitive ? CASE_SENSITIVE_COLLATION : CASE_INSENSITIVE_COLLATION);
        return positiveMatch ? fieldToMatch.likeRegex(regex) : fieldToMatch.notLikeRegex(regex);
    }

    @Nonnull
    @Override
    public Condition contains(@Nonnull Field<String> field, @Nonnull String pattern,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        if (positiveMatch) {
            if (caseSensitive) {
                return field.contains(pattern);
            } else {
                return field.containsIgnoreCase(pattern);
            }
        } else {
            if (caseSensitive) {
                return field.notContains(pattern);
            } else {
                return field.notContainsIgnoreCase(pattern);
            }
        }
    }
}
