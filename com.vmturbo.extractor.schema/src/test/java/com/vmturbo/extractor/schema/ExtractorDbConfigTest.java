package com.vmturbo.extractor.schema;

import java.sql.SQLException;
import java.util.Collections;

import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.support.ApplicationObjectSupport;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Test that DbEndpoints can be activated and provide proper DB access.
 */
public class ExtractorDbConfigTest extends ApplicationObjectSupport {
    private static final ExtractorDbConfig EXTRACTOR_SCHEMA_CONFIG = new ExtractorDbConfig();
    private static final DbEndpoint ingesterEndpoint = EXTRACTOR_SCHEMA_CONFIG.ingesterEndpoint();
    private static final DbEndpoint queryEndpoint = EXTRACTOR_SCHEMA_CONFIG.queryEndpoint();

    /**
     * Rule to manage configured endpoints for tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule =
            new DbEndpointTestRule("extractor", Collections.emptyMap(), ingesterEndpoint, queryEndpoint);

    /**
     * Test that the ingester endpoint can read and write in the database.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if any DB operation fails
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testIngesterCanReadAndWrite()
            throws UnsupportedDialectException, SQLException, InterruptedException {
        checkCanReadTables(ingesterEndpoint);
        checkCanWriteTables(ingesterEndpoint);
    }

    /**
     * Test that the query endpoint can read but not write in the database.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if any DB operation fails
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testQueryCanReadNotWrite()
            throws UnsupportedDialectException, SQLException, InterruptedException {
        checkCanReadTables(queryEndpoint);
        checkCannotWriteTables(queryEndpoint);
    }

    private void checkCanReadTables(DbEndpoint endpoint)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        Assert.assertEquals(0L, endpoint.dslContext().fetchValue("SELECT count(*) FROM entity"));
        Assert.assertEquals(0L, endpoint.dslContext().fetchValue("SELECT count(*) FROM metric"));
    }

    private void checkCanWriteTables(DbEndpoint endpoint)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        Assert.assertTrue(canWriteEntity(endpoint));
        Assert.assertTrue(canWriteMetric(endpoint));
    }

    private void checkCannotWriteTables(DbEndpoint endpoint)
            throws UnsupportedDialectException, InterruptedException {
        Assert.assertFalse(canWriteEntity(endpoint));
        Assert.assertFalse(canWriteMetric(endpoint));
    }

    private boolean canWriteEntity(DbEndpoint endpoint)
            throws UnsupportedDialectException, InterruptedException {
        try {
            endpoint.dslContext().execute("INSERT INTO entity "
                    + "VALUES (0, 0, 'VM', 'Vm1', null, null, null, '{}', now(), now())");
            return 1L == (long)endpoint.dslContext().fetchValue("SELECT count(*) FROM entity");

        } catch (DataAccessException | SQLException | BadSqlGrammarException e) {
            return false;
        }
    }

    private boolean canWriteMetric(final DbEndpoint endpoint)
            throws UnsupportedDialectException, InterruptedException {
        try {
            endpoint.dslContext().execute("INSERT INTO metric "
                    + "VALUES (now(), 0, 0, 'type', null, null, null, null, 0)");
            return 1L == (long)endpoint.dslContext().fetchValue("SELECT count(*) FROM metric");
        } catch (DataAccessException | SQLException | BadSqlGrammarException e) {
            return false;
        }
    }
}
