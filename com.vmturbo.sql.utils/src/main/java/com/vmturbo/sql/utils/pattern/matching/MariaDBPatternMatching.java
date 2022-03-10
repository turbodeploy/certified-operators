package com.vmturbo.sql.utils.pattern.matching;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;

/**
 * Pattern matching in MariaDB.
 */
public class MariaDBPatternMatching implements PatternMatchingAdapter {

    private static final String CASE_INSENSITIVE_COLLATION_SUFFIX = "_unicode_ci";
    private static final String CASE_SENSITIVE_COLLATION_SUFFIX = "_bin";

    private static final String DEFAULT_CASE_INSENSITIVE_COLLATION = "utf8mb4_unicode_ci";
    private static final String DEFAULT_CASE_SENSITIVE_COLLATION = "utf8mb4_bin";

    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        // To specify case-sensitivity for regex matching in MariaDB, we change the collation on the fly.
        if (caseSensitive) {
            fieldToMatch = fieldToMatch.collate(DEFAULT_CASE_SENSITIVE_COLLATION);
        } else {
            fieldToMatch = fieldToMatch.collate(DEFAULT_CASE_INSENSITIVE_COLLATION);
        }
        return positiveMatch ? fieldToMatch.likeRegex(regex) : fieldToMatch.notLikeRegex(regex);
    }

    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive,
            @Nonnull String characterSet) {
        // To specify case-sensitivity for regex matching in MariaDB, we change the collation on the fly.
        if (caseSensitive) {
            final String caseSensitiveCollation = characterSet + CASE_SENSITIVE_COLLATION_SUFFIX;
            fieldToMatch = fieldToMatch.collate(caseSensitiveCollation);
        } else {
            final String caseInsensitiveCollation = characterSet + CASE_INSENSITIVE_COLLATION_SUFFIX;
            fieldToMatch = fieldToMatch.collate(caseInsensitiveCollation);
        }
        return positiveMatch ? fieldToMatch.likeRegex(regex) : fieldToMatch.notLikeRegex(regex);
    }
}
