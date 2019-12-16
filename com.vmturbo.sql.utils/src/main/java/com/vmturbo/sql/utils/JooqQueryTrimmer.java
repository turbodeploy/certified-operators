package com.vmturbo.sql.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.jooq.Query;
import org.jooq.exception.DataAccessException;

/**
 * Methods to trim jOOQ query strings that can appear in very unreasonable from in our logs.
 */
public class JooqQueryTrimmer {

    /** disallow instantiation. */
    private JooqQueryTrimmer() {
    }

    /**
     * This pattern matches individual value clauses within a JOOQ-generatedvalues statement. They
     * look like "(?, ?, ?)" but may have any number of question marks (including none).
     */
    protected static final Pattern JOOQ_VALUES_CLAUSE_PAT = Pattern.compile("[(](\\s*[?]\\s*,?)*\\s*[)]");

    /**
     * This method deals with a very unfortunate feature of some jOOQ query renderings exceptions,
     * In the case of a multi-valued insert statement with inline values, this ends up including
     * many repetitions (one per record) of something like "(?, ?, ?, ?)", which is completely
     * useless and should not be logged. This method strips all but the first of these from the
     * message, and also joins the initial and final fragments into a single line.
     * @param sql SQL string to be trimmed
     * @return trimmed SQL
     */
    public static String trimJooqQueryString(@Nonnull String sql) {
        // first make this a single-line string
        sql = sql.replaceAll("[\\r\\n]+", " ");
        // now match our pattern for lists of placeholders
        Matcher m = JOOQ_VALUES_CLAUSE_PAT.matcher(sql);
        // updated after first match with the end of that match, so we can grab that and whatever
        // preceded it.
        int prefixPos = -1;
        // updated after all subsequent matches with the end of each match, so we know where
        // trailing content starts
        int suffixPos = -1;
        while (m.find()) {
            if (prefixPos == -1) {
                prefixPos = m.end();
            } else {
                suffixPos = m.end();
            }
        }
        // if we didn't get at least two matches, then there's nothing to elide, so we send back
        // the original (but as a single-line string)
        return suffixPos == -1 ? sql
                // otherwise we pick up the first match and what came before, plus the trailing
                // content
                : sql.substring(0, prefixPos) + "... " + sql.substring(suffixPos);
    }

    /**
     * Renders a jOOQ query's SQL with trimming.
     *
     * @param query query whose trimmed SQL is needed
     * @return the trimmed SQL string
     */
    public static String trimJooqQuery(@Nonnull Query query) {
        return trimJooqQueryString(query.getSQL());
    }

    /**
     * Renders a query string embedded in the message of a jOOQ {@link DataAccessException}, with
     * trimming.
     *
     * @param e exception whose message should be trimmed
     * @return the trimmed exception message
     */
    @Nonnull
    public static String trimJooqErrorMessage(@Nonnull Exception e) {
        if (e instanceof DataAccessException) {
            // this is a JOOQ exception, possibly with the query SQL inlined into its message
            return trimJooqQueryString(e.toString());
        } else {
            return e.toString();
        }
    }

}
