package com.vmturbo.sql.utils.pattern.matching;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * Pattern matching in Postgres.
 */
public class PostgresPatternMatching implements PatternMatchingAdapter {

    // Regex matching in Postgres is not supported for non-deterministic collations, so we collate to
    // "und-x-icu", which is the deterministic and case-sensitive equivalent of the text columns' collation.
    private static final String DETERMINISTIC_COLLATION = "und-x-icu";

    private static final String APPLY_COLLATION = String.format(" collate \"%s\"", DETERMINISTIC_COLLATION);

    // Positive match, case-sensitive regex matching in Postgres.
    private static final String PM_CS_REGEX = "{0} ~ {1}";

    // Positive match, case-insensitive regex matching in Postgres.
    private static final String PM_CI_REGEX = "{0} ~* {1}";

    // Negative match, case-sensitive regex matching in Postgres.
    private static final String NM_CS_REGEX = "{0} !~ {1}";

    // Negative match, case-insensitive regex matching in Postgres.
    private static final String NM_CI_REGEX = "{0} !~* {1}";


    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        String regexQuery;
        if (positiveMatch) {
            if (caseSensitive) {
                regexQuery = PM_CS_REGEX;
            } else {
                regexQuery = PM_CI_REGEX;
            }
        } else {
            if (caseSensitive) {
                regexQuery = NM_CS_REGEX;
            } else {
                regexQuery = NM_CI_REGEX;
            }
        }
        return DSL.condition(regexQuery + APPLY_COLLATION, fieldToMatch, regex);
    }

    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive,
            @Nonnull String characterSet) {
        // The character set is only relevant for MariaDB/MySQL pattern matching.
        return matchRegex(fieldToMatch, regex, positiveMatch, caseSensitive);
    }
}
