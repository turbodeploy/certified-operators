/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.jooq.Result;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;

import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * {@link TestDataProvider} helps to check that all SQL requests has been made with desired
 * parameters.
 */
public class TestDataProvider implements MockDataProvider {
    private final Queue<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponses;

    /**
     * Creates {@link TestDataProvider} instance.
     *
     * @param sqlRequestToResponses mapping from sql expression, parameters to
     *                 desired response.
     */
    public TestDataProvider(Queue<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponses) {
        this.sqlRequestToResponses = sqlRequestToResponses;
    }

    @Override
    public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
        final Object[] actual = ctx.bindings();
        if (sqlRequestToResponses.isEmpty()) {
            Assert.fail(String
                            .format("Unexpected SQL statement '%s' with '%s' parameters", ctx.sql(),
                                            Arrays.deepToString(actual)));
        }
        final Pair<Pair<String, List<?>>, ?> sqlRequestToResponse = sqlRequestToResponses.poll();
        final Pair<String, List<?>> sqlToParameters = sqlRequestToResponse.getFirst();
        final String sqlPrefix = sqlToParameters.getFirst();
        if (ctx.sql().startsWith(sqlPrefix)) {
            final List<?> expected = sqlToParameters.getSecond();

            Assert.assertEquals(String.format("Bindings are different:%nActual:%s%nExpected:%s",
                            Arrays.deepToString(actual), Arrays.deepToString(expected.toArray())),
                            Arrays.asList(actual), expected);
            final Object response = sqlRequestToResponse.getSecond();
            if (response instanceof SQLException) {
                throw (SQLException)response;
            }
            if (response instanceof Result) {
                return new MockResult[] {new MockResult(1, (Result<?>)response)};
            }
        }
        return new MockResult[0];
    }
}
