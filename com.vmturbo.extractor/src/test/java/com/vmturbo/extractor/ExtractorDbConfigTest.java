package com.vmturbo.extractor;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Test that DbEndpoints can be activated and provide proper DB access.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
public class ExtractorDbConfigTest {

    @Autowired
    private ExtractorDbConfig dbConfig;

    private DbEndpoint ingesterEndpoint;
    private DbEndpoint queryEndpoint;

    /**
     * Rule to manage configured endpoints for tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    /**
     * Set our endpoints up for management by the test rule.
     *
     * @throws InterruptedException if interrupted
     * @throws SQLException if there's a DB error
     * @throws UnsupportedDialectException if an endpoint is misconfigured
     */
    @Before
    public void before() throws InterruptedException, SQLException, UnsupportedDialectException {
        this.ingesterEndpoint = dbConfig.ingesterEndpoint();
        this.queryEndpoint = dbConfig.queryEndpoint();
        endpointRule.addEndpoints(ingesterEndpoint, queryEndpoint);
    }

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
            throws UnsupportedDialectException, InterruptedException {
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
