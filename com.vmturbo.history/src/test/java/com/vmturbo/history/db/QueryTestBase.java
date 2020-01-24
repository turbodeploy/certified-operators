package com.vmturbo.history.db;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.ArrayUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;

import com.vmturbo.sql.utils.SQLDatabaseConfig.SQLConfigObject;

/**
 * Base class for tests of query builders.
 *
 * <p>Rendered SQL will use lower-case for all names (fields, tables, aliases) and upper-case for all SQL keywords.
 * Expected values must do likewise, or else tests will fail.</p>
 *
 * <p>The tests do not require a live database. They operate by performing pattern matches on the generated query,
 * checking that query parts are as expected.</p>
 */
public class QueryTestBase {

    /**
     * Set up Jooq for use by query builder.
     *
     * <p>Subclasses should call this in a @Before method.</p>
     */
    protected void setupJooq() {
        // the query builder uses HistordbIO#getJooqBuilder, so we need one of those minimally set up
        SQLConfigObject sqlConfigObject = new SQLConfigObject(
                "localhost", 0, "vmtdb",
                Optional.empty(), SQLDialect.MARIADB.name(), false, ImmutableMap.of(SQLDialect.MARIADB, ""));
        HistorydbIO historydbIO = new HistorydbIO(null, sqlConfigObject);
        HistorydbIO.setSharedInstance(historydbIO);
        // we customize some rendering settings to make our tests easier and more robust
        Settings settings = HistorydbIO.getJooqBuilder().settings();
        settings.setRenderSchema(false);
        settings.setRenderNameStyle(RenderNameStyle.LOWER);
        settings.setRenderKeywordStyle(RenderKeywordStyle.UPPER);
    }

    /**
     * Make sure that the query's SELECT fields are as expected.
     *
     * @param sql      SQL to be checked
     * @param distinct true of DISTINCT keyword should be present
     * @param fields   fields expected in the SELECT list (may include computed fields)
     */
    private void checkSelectFields(String sql, boolean distinct, String... fields) {
        if (fields.length > 0) {
            assertThat(sql, matchesPattern(makePattern(new String[]{"SELECT", distinct ? "DISTINCT" : null}, fields)));
        }
    }

    /**
     * Check that the tables listed in the FROM clause are as expected.
     *
     * @param sql    SQL to be checked
     * @param tables expected tables, including joins
     */
    private void checkTables(String sql, String... tables) {
        if (tables.length > 0) {
            assertThat(sql, matchesPattern(makePattern(new String[]{"FROM"}, tables)));
        } else {
            assertThat(sql, not(matchesPattern(makePattern(new String[]{"FROM"}))));
        }
    }

    /**
     * Check that WHERE conditions are as expected.
     *
     * <p>The provided conditions must be a conjunction (using AND) in the query. For now, any part of the condition
     * expression that represents a disjunction (OR) must be supplied as a single condition for checking.</p>
     *
     * <p>The checker expects the overall conjunction to be surrounded by parentheses if there is more than one
     * condition supplied, but not if there is only one.</p>
     *
     * @param sql        SQL to be checked
     * @param conditions expected conditions, assumed
     */
    private void checkConditions(String sql, String... conditions) {
        if (conditions.length > 0) {
            boolean paren = conditions.length > 1;
            String conjunction = String.join("\\s+AND\\s+", conditions);
            assertThat(sql, matchesPattern(makePattern(
                    new String[]{"WHERE", paren ? "\\(" : null, conjunction, paren ? "\\)" : null})));
        } else {
            assertThat(sql, not(matchesPattern(makePattern(new String[]{"WHERE"}))));
        }
    }

    /**
     * Check that the ORDER BY fields are as expected in the query.
     *
     * @param sql        SQL to be checked
     * @param sortFields expected sort fields in the ORDER BY clause, including ASC or DESC as appropriate.
     */
    private void checkOrderBy(String sql, String... sortFields) {
        if (sortFields.length > 0) {
            assertThat(sql, matchesPattern(makePattern(new String[]{"ORDER BY"}, sortFields)));
        } else {
            assertThat(sql, not(matchesPattern(makePattern(new String[]{"ORDER BY"}))));
        }
    }

    /**
     * Check that the LIMIT expression in the query is as expected.
     *
     * @param sql   the SQL to be checked
     * @param limit the expected limit, or 0 for none
     */
    private void checkLimit(String sql, int limit) {
        if (limit > 0) {
            assertThat(sql, matchesPattern(makePattern(new String[]{"LIMIT", Integer.toString(limit)})));
        } else {
            assertThat(sql, not(matchesPattern(makePattern(new String[]{"LIMIT"}))));
        }
    }

    /**
     * Get the SQL for the constructed query.
     *
     * @param query jOOQ {@link Query} object representing query to be checked
     * @return SQL rendered as expected by tests
     */
    private String getSQL(Query query) {
        // make sure everything is inlined in the generated SQL
        String sql = query.getSQL(ParamType.INLINED);
        // make it all one line to make pattern matching simpler
        return String.join(" ", sql.split("\n"));
    }

    /**
     * Create a {@link Pattern} for groups of expected tokens.
     *
     * <p>The elements of each group may included regular expression elements, so the test should do quoting as
     * needed.</p>
     *
     * <p>The grouping structure is not relevant to the constructed pattern; it's only  purpose is to make it easy for
     * check methods to combine their own tokens with tokens supplied by their callers.</p>
     *
     * @param groups groups of token strings
     * @return a {@link Pattern} that can be used to test an SQL query
     */
    private String makePattern(String[]... groups) {
        // we basicaly flatten the groups and create a regex that expects all the tokens in sequence, with one
        // or more space between tokens.
        String content = Stream.of(groups)
                .map(g -> Stream.of(g)
                        .filter(Objects::nonNull)
                        .collect(Collectors.joining("\\s+")))
                .collect(Collectors.joining("\\s+"));
        return "(^|\\s)" + content + "(\\s|$)";
    }


    /**
     * Create a {@link MatchesPattern} hamcrest matcher.
     *
     * @param regex regular expression to be matched
     * @return a matcher to match the supplied regex
     */
    private static Matcher<String> matchesPattern(String regex) {
        return new MatchesPattern(regex);
    }

    /**
     * Hamcrest matcher to match a regular expression.
     */
    public static class MatchesPattern extends BaseMatcher<String> {
        private final Pattern pattern;

        /**
         * Create a new matcher.
         *
         * @param regex regular expression to be matched
         */
        MatchesPattern(String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean matches(Object item) {
            return item instanceof String && pattern.matcher((String)item).find();
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("matches regex: '" + pattern.toString() + "'");
        }
    }

    /**
     * A class that captures all the expected parts of a SQL query and then checks that they are all present.
     */
    public class QueryChecker {

        private String[] selectFields = new String[0];
        private boolean distinct = false;
        private String[] tables = new String[0];
        private String[] conditions = new String[0];
        private String[] sortFields = new String[0];
        private int limit = 0;

        /**
         * Specify expected SELECT fields, in expected order.
         *
         * <p>This method replaces any previously specified SELECT fields.</p>
         *
         * @param selectFields fields, with AS aliases if expected
         * @return this query checker
         */
        public QueryChecker withSelectFields(String... selectFields) {
            this.selectFields = selectFields;
            return this;
        }

        /**
         * Specify whether DISTINCT keyword is expected in the query.
         *
         * @param distinct true if DISTINCT is expected
         * @return this query checker
         */
        public QueryChecker withDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        /**
         * Specify tables expected in the query, including JOIN tables with aliases, join types and join conditions as
         * needed.
         *
         * <p>This method replaces any previously specified tables.</p>
         *
         * @param tables tables, with AS, JOIN, and ON conditions as expected
         * @return this query checker
         */
        public QueryChecker withTables(String... tables) {
            this.tables = tables;
            return this;
        }

        /**
         * Specify expected WHERE conditions for query.
         *
         * <p>The conditions are presumed to form a top-level conjunction (AND expression. Any internal structure
         * must be provided within a complex top-level condition.</p>
         *
         * <p>This method replaces any previously specified conditions.</p>
         *
         * @param conditions expected conditions
         * @return this query checker
         */
        public QueryChecker withConditions(String... conditions) {
            this.conditions = conditions;
            return this;
        }

        /**
         * Specify conditions to be added to the current list of expected WHERE conditions.
         *
         * @param conditions conditions to be added
         * @return this query checker
         */
        public QueryChecker withMoreConditions(String... conditions) {
            this.conditions = ArrayUtils.addAll(this.conditions, conditions);
            return this;
        }

        /**
         * Specify fields expected to appear in the ORDER BY clause.
         *
         * <p>This method replaces any previously specified sort fields.</p>
         *
         * @param sortFields fields expected in ORDER BY, with ASC or DESC as expected
         * @return this query checker
         */
        public QueryChecker withSortFields(String... sortFields) {
            this.sortFields = sortFields;
            return this;
        }

        /**
         * Specify an expected LIMIT value for the query.
         *
         * @param limit limit value, or 0 for no LIMIT
         * @return this query checker
         */
        public QueryChecker withLimit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Check that a given query meets all the expectations specified for this query checker.
         *
         * @param query the jOOQ {@link Query} object to be checked
         */
        public void check(Query query) {
            String sql = getSQL(query);
            checkSelectFields(sql, distinct, selectFields);
            checkTables(sql, tables);
            checkConditions(sql, conditions);
            checkOrderBy(sql, sortFields);
            checkLimit(sql, limit);
        }
    }
}
