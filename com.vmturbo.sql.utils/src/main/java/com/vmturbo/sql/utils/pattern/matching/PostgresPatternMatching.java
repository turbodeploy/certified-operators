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

    // Apply the deterministic collation using "collate".
    private static final String APPLY_DETERMINISTIC_COLLATION =
            String.format(" collate \"%s\"", DETERMINISTIC_COLLATION);

    // Positive match, case-sensitive regex matching in Postgres.
    private static final String PM_CS_REGEX = "{0} ~ {1}";

    // Positive match, case-insensitive regex matching in Postgres.
    private static final String PM_CI_REGEX = "{0} ~* {1}";

    // Negative match, case-sensitive regex matching in Postgres.
    private static final String NM_CS_REGEX = "{0} !~ {1}";

    // Negative match, case-insensitive regex matching in Postgres.
    private static final String NM_CI_REGEX = "{0} !~* {1}";

    /**
     * Jooq method "contains" gets translated to the following query.
     *
     * <p>For Postgres, Jooq's "containsIgnoreCase" uses "ilike", while it uses "like lower(...)"
     * in other dialects. If that was the case, we could create the conditions using the "contains"
     * methods. However, static DSL methods use the "DEFAULT" SQL dialect, which uses the
     * "like lower(...)" approach, forcing us to emulate Postgres behavior by constructing the
     * condition using the String constants below.</p>
     */
    private static final String CONTAINS_OPERATION =
            "('%' || replace(replace(replace({1}, '!', '!!'), '%', '!%'), '_', '!_') || '%') escape '!'";

    // Positive match, case-sensitive "contains" query in Postgres.
    private static final String PM_CS_CONTAINS = "{0} like " + CONTAINS_OPERATION;

    // Positive match, case-insensitive "contains" query in Postgres.
    private static final String PM_CI_CONTAINS = "{0} ilike " + CONTAINS_OPERATION;

    // Negative match, case-sensitive "contains" query in Postgres.
    private static final String NM_CS_CONTAINS = "{0} not like " + CONTAINS_OPERATION;

    // Negative match, case-insensitive "contains" query in Postgres.
    private static final String NM_CI_CONTAINS = "{0} not ilike " + CONTAINS_OPERATION;

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
        return DSL.condition(regexQuery + APPLY_DETERMINISTIC_COLLATION, fieldToMatch, regex);
    }

    @Nonnull
    @Override
    public Condition contains(@Nonnull Field<String> field, @Nonnull String pattern,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        String containsQuery;
        if (positiveMatch) {
            if (caseSensitive) {
                containsQuery = PM_CS_CONTAINS;
            } else {
                containsQuery = PM_CI_CONTAINS;
            }
        } else {
            if (caseSensitive) {
                containsQuery = NM_CS_CONTAINS;
            } else {
                containsQuery = NM_CI_CONTAINS;
            }
        }
        return DSL.condition(containsQuery + APPLY_DETERMINISTIC_COLLATION, field, pattern);
    }
}
