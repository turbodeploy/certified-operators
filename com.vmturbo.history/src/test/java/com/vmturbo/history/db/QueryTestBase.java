package com.vmturbo.history.db;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

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

    protected DSLContext dsl;

    /**
     * Set up Jooq for use by query builder.
     *
     * <p>Subclasses should call this in a @Before method.</p>
     */
    protected void setupJooq() {
        // the query builder uses a DSLContext, so we need one for MARIADB dialect
        this.dsl = DSL.using(SQLDialect.MARIADB);
        // we customize some rendering settings to make our tests easier and more robust
        Settings settings = dsl.settings();
        settings.withRenderFormatted(true);
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
    private void checkSelectFields(String sql, boolean distinct, List<String> fields) {
        if (!fields.isEmpty()) {
            assertThat(sql, matchesPattern(makePattern(
                    singleton("SELECT"),
                    opt(distinct, "DISTINCT"),
                    commas(fields))));
        }
    }

    /**
     * Check that the tables listed in the FROM clause are as expected.
     *
     * @param sql    SQL to be checked
     * @param tables expected tables, including joins
     */
    private void checkTables(String sql, List<String> tables) {
        if (tables.isEmpty()) {
            assertThat(sql, not(matchesPattern(makePattern(singleton("FROM")))));
        } else {
            assertThat(sql, matchesPattern(makePattern(singleton("FROM"), commas(tables))));
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
    private void checkConditions(String sql, List<String> conditions) {
        if (conditions.isEmpty()) {
            assertThat(sql, not(matchesPattern(makePattern(singleton("WHERE")))));
        } else {
            boolean paren = conditions.size() > 1;
            String conjunction = String.join("\\s+AND\\s+", conditions);
            assertThat(sql, matchesPattern(makePattern(
                    singleton("WHERE"),
                    opt(paren, "\\("),
                    singleton(conjunction),
                    opt(paren, "\\)"))));
        }
    }

    /**
     * Check that the ORDER BY fields are as expected in the query.
     *
     * @param sql        SQL to be checked
     * @param sortFields expected sort fields in the ORDER BY clause, including ASC or DESC as
     *                   appropriate.
     */
    private void checkOrderBy(String sql, List<String> sortFields) {
        if (sortFields.isEmpty()) {
            assertThat(sql, not(matchesPattern(makePattern(singleton("ORDER BY")))));
        } else {
            assertThat(sql, matchesPattern(makePattern(
                    singleton("ORDER BY"),
                    commas(sortFields))));
        }
    }

    private void checkGroupBy(String sql, List<String> groupByFields) {
        if (groupByFields.isEmpty()) {
            assertThat(sql, not(matchesPattern(makePattern(singleton("GROUP BY")))));
        } else {
            assertThat(sql, matchesPattern(makePattern(
                    singleton("GROUP BY"),
                    commas(groupByFields))));
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
            assertThat(sql, matchesPattern(makePattern(
                    singleton("LIMIT"),
                    singleton(Integer.toString(limit)))));
        } else {
            assertThat(sql, not(matchesPattern(makePattern(singleton("LIMIT")))));
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
        // we basically flatten the groups and create a regex that expects all the tokens in sequence, with one
        // or more space between tokens.
        String content = Stream.of(groups)
                .map(g -> Stream.of(g)
                        .filter(Objects::nonNull)
                        .collect(Collectors.joining("\\s+")))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("\\s+"));
        return "(^|\\s)" + content + "(\\s|$)";
    }

    private static String[] singleton(String string) {
        return new String[]{string};
    }

    private static String[] opt(boolean enabled, String... group) {
        return enabled ? group : new String[0];
    }

    private static String[] commas(List<String> strings) {
        return new String[]{String.join(",\\s*", strings)};
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
     * A class that captures all the expected parts of an SQL query and then checks that they are
     * all present.
     */
    public class QueryChecker {

        private final List<String> selectFields = new ArrayList<>();
        private boolean distinct = false;
        private final List<String> tables = new ArrayList<>();
        private final List<String> conditions = new ArrayList<>();
        private final List<String> sortFields = new ArrayList<>();
        private final List<String> groupByFields = new ArrayList<>();
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
            return withSelectFields(false, selectFields);
        }

        /**
         * Set or add to the fields that should appear in the SELECT list.
         *
         * @param reset        true if these fields should replace existing fields
         * @param selectFields select-list fields
         * @return this query checker
         */
        public QueryChecker withSelectFields(boolean reset, String... selectFields) {
            if (reset) {
                this.selectFields.clear();
            }
            this.selectFields.addAll(Arrays.asList(selectFields));
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
         * Specify tables expected in the query, including JOIN tables with aliases, join types and
         * join conditions as needed.
         *
         * <p>This method replaces any previously specified tables.</p>
         *
         * @param tables tables, with AS, JOIN, and ON conditions as expected
         * @return this query checker
         */
        public QueryChecker withTables(String... tables) {
            return withTables(false, tables);
        }

        /**
         * Set or add to the tables that are involved in the query.
         *
         * @param reset  true if this list should replace existing tables
         * @param tables tables to appear in the query
         * @return this query checker
         */
        public QueryChecker withTables(boolean reset, String... tables) {
            if (reset) {
                this.tables.clear();
            }
            this.tables.addAll(Arrays.asList(tables));
            return this;
        }

        /**
         * Specify expected WHERE conditions for query.
         *
         * <p>The conditions are presumed to form a top-level conjunction (AND expression. Any
         * internal structure must be provided within a complex top-level condition.</p>
         *
         * <p>This method replaces any previously specified conditions.</p>
         *
         * @param conditions expected conditions
         * @return this query checker
         */
        public QueryChecker withConditions(String... conditions) {
            return this.withConditions(false, conditions);
        }

        /**
         * Set or add to the conditions that should appear in the WHERE clause.
         *
         * @param reset      true if these should replace existing conditions
         * @param conditions conditions to appear in WHERE clause
         * @return this query checker
         */
        public QueryChecker withConditions(boolean reset, String... conditions) {
            if (reset) {
                this.conditions.clear();
            }
            this.conditions.addAll(Arrays.asList(conditions));
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
            return withSortFields(false, sortFields);
        }

        /**
         * Set or add to the list of fields to appear in ORDER BY clause.
         *
         * @param reset      true if these fields should replace existing fields
         * @param sortFields fields to appear in ORDER BY
         * @return this query checker
         */
        public QueryChecker withSortFields(boolean reset, String... sortFields) {
            if (reset) {
                this.sortFields.clear();
            }
            this.sortFields.addAll(Arrays.asList(sortFields));
            return this;
        }

        /**
         * Specify fields expected to appear in the GROUP BY clause.
         *
         * <p>This method replaces any previously specified GROUP BY fields.</p>
         *
         * @param groupByFields fields to appear in GROUP BY clause.
         * @return this query checker
         */
        public QueryChecker withGroupByFields(String... groupByFields) {
            return withGroupByFields(false, groupByFields);
        }

        /**
         * Set or add to fields to appear in GROUP BY clause.
         *
         * @param reset         true if these fields should replace existing fields
         * @param groupByFields fields to appear in GROUP BY
         * @return this query checker
         */
        public QueryChecker withGroupByFields(boolean reset, String... groupByFields) {
            this.groupByFields.addAll(Arrays.asList(groupByFields));
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
            checkGroupBy(sql, groupByFields);
            checkLimit(sql, limit);
        }
    }
}
