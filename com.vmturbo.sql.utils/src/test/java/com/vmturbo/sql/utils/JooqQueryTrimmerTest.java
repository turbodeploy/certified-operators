package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqErrorMessage;
import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqQuery;
import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqQueryString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDSLContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests of the {@link JooqQueryTrimmer} class.
 */
public class JooqQueryTrimmerTest {

    private DSLContext ctx;

    /**
     * Define a {@link DSLContext} that's set up similarly to what we use in production.
     *
     * <p>Note that other than the advertised trimming, there is no attempt to normalize spaces
     * appearing in the trimmed string. So the expected values appearing in these tests are somewhat
     * fragile.</p>
     */
    @Before
    public void before() {
        final Connection conn = mock(Connection.class);
        ctx = new DefaultDSLContext(conn, SQLDialect.MARIADB);
        ctx.settings().setRenderFormatted(true);
        ctx.settings().setRenderKeywordStyle(RenderKeywordStyle.UPPER);
    }

    /**
     * Test trimmer when input arrives as a string.
     */
    @Test
    public void testPlaceholderSequencesAreElided() {
        String sql = String.join("\n", Arrays.asList(
                "INSERT INTO table COLUMNS (x, y, z)",
                "VALUES (",
                " ?,",
                " ?,",
                " ?",
                "), (",
                " ?,",
                " ?,",
                " ?",
                "), (",
                " ?,",
                " ?,",
                " ?",
                ") ON DUPLICATE KEY IGNORE"));
        assertThat(trimJooqQueryString(sql),
                is("INSERT INTO table COLUMNS (x, y, z) VALUES (?, ?, ...[+1]), ...[+2] "
                        + "ON DUPLICATE KEY IGNORE"));
    }

    /**
     * Test that the trimmer works when provided a jOOQ query.
     */
    @Test
    public void testJooqQueryAsInput() {
        Query q = ctx.insertInto(DSL.table("table"),
                DSL.field("x", Integer.class),
                DSL.field("y", Integer.class),
                DSL.field("z", Integer.class))
                .values(1, 2, 3)
                .values(4, 5, 6)
                .values(7, 8, 9);
        assertThat(trimJooqQuery(q),
                is("INSERT INTO table (   x,    y,    z ) VALUES (?, ?, ...[+1]), ...[+2]"));
    }

    /**
     * Test that the trimmer works when provided a {@link DataAccessException}.
     */
    @Test
    public void testJooqExceptionAsInput() {
        Query q = ctx.insertInto(DSL.table("table"),
                DSL.field("x", Integer.class),
                DSL.field("y", Integer.class),
                DSL.field("z", Integer.class))
                .values(1, 2, 3)
                .values(4, 5, 6)
                .values(7, 8, 9);
        final DataAccessException e = new DataAccessException(
                "You're query is whack: " + q.getSQL());
        assertThat(trimJooqErrorMessage(e),
                is("org.jooq.exception.DataAccessException: "
                        + "You're query is whack: "
                        + "INSERT INTO table (   x,    y,    z ) VALUES (?, ?, ...[+1]), ...[+2]"));
    }

    /**
     * Test that non-jooq exceptions are not trimmed.
     */
    @Test
    public void testNonJooqExceptionAsInput() {
        Query q = ctx.insertInto(DSL.table("table"),
                DSL.field("x", Integer.class),
                DSL.field("y", Integer.class),
                DSL.field("z", Integer.class))
                .values(1, 2, 3)
                .values(4, 5, 6)
                .values(7, 8, 9);
        final IllegalArgumentException e = new IllegalArgumentException(
                "You're query is whack: " + q.getSQL());
        assertThat(trimJooqErrorMessage(e),
                is("java.lang.IllegalArgumentException: "
                        + "You're query is whack: INSERT INTO table (\n  x, \n  y, \n  z\n)\n"
                        + "VALUES (\n  ?, \n  ?, \n  ?\n), "
                        + "(\n  ?, \n  ?, \n  ?\n), "
                        + "(\n  ?, \n  ?, \n  ?\n)"));
    }


    /**
     * Test that the trimmer succeeds on input that includes very long placeholder lists without
     * experiencing stack overflow.
     *
     * @see <a href="https://vmturbo.atlassian.net/browse/OM-61038">Jira issue OM-61038</a>
     */
    @Test
    public void testHugePatternMatch() {
        final String placeHolders = "?" + StringUtils.repeat(", ?", 1000);
        final String sql = String.format("INSERT INTO table (a, b, c) VALUES (%s), (%s) "
                        + "ON DUPLICATE KEY IGNORE",
                placeHolders, placeHolders);
        // if this overflows stack, test will error out
        JooqQueryTrimmer.trimJooqQueryString(sql);
    }
}
