/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.jooq.Result;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.springframework.dao.DataAccessException;

/**
 * {@link TestDataProvider} helps to check that all SQL requests has been made with desired
 * parameters.
 */
public class TestDataProvider implements MockDataProvider {
    private final Queue<SqlWithResponse> sqlWithResponses;

    /**
     * Creates {@link TestDataProvider} instance.
     *
     * @param sqlWithResponses mapping from sql expression, parameters to
     *                         desired response.
     */
    public TestDataProvider(Queue<SqlWithResponse> sqlWithResponses) {
        this.sqlWithResponses = sqlWithResponses;
    }

    @Override
    public MockResult[] execute(MockExecuteContext ctx) throws DataAccessException {
        final Object[] actual = ctx.bindings();
        if (sqlWithResponses.isEmpty()) {
            Assert.fail(String
                    .format("Unexpected SQL statement '%s' with '%s' parameters", ctx.sql(),
                            Arrays.deepToString(actual)));
        }
        final SqlWithResponse sqlRequestToResponse = sqlWithResponses.poll();
        if (ctx.sql().startsWith(sqlRequestToResponse.getSqlPrefix())) {
            final List<?> expectedBoundValues = sqlRequestToResponse.getBoundValues();
            Assert.assertEquals(String.format("Bindings are different:%nActual:%s%nExpected:%s",
                            Arrays.deepToString(actual),
                            Arrays.deepToString(expectedBoundValues.toArray())),
                    Arrays.asList(actual), expectedBoundValues);
            final Object response = sqlRequestToResponse.getResponse();
            if (response instanceof DataAccessException) {
                throw (DataAccessException)response;
            }
            if (response instanceof Result) {
                Result<?> result = (Result<?>)response;
                return new MockResult[]{new MockResult(1, result)};
            }
        }
        return new MockResult[0];
    }

    /**
     * Class to record DB operations and responses that should be provided.
     *
     * <p>A given operation is identified by a prefix of the full SQL statement, along with values
     * for bound placeholders.</p>
     */
    public static class SqlWithResponse {

        private final String sqlPrefix;
        private final List<?> boundValues;
        private final Object response;

        /**
         * Create a new instance.
         *
         * @param sqlPrefix   a prefix of the SQL statement
         * @param boundValues values bound to placeholders
         * @param response    response value
         */
        public SqlWithResponse(String sqlPrefix, List<?> boundValues, Object response) {
            this.sqlPrefix = sqlPrefix;
            this.boundValues = boundValues;
            this.response = response;
        }

        public String getSqlPrefix() {
            return sqlPrefix;
        }

        public List<?> getBoundValues() {
            return boundValues;
        }

        public Object getResponse() {
            return response;
        }
    }
}
