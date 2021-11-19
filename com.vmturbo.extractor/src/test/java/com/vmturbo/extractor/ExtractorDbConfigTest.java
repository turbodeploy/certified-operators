package com.vmturbo.extractor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.sql.utils.DbAdapter;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointConfig;
import com.vmturbo.sql.utils.DbEndpointResolver;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Test that DbEndpoints can be activated and provide proper DB access.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
// username property configuration is made to work with MySQL which has a username length restriction of 16.
@TestPropertySource(properties = {"enableReporting=true", "dbs.search.user=s", "dbs.search.host=localhost", "dbs.search.port=3306"})
public class ExtractorDbConfigTest {
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDbConfig dbConfig;

    @Autowired
    private Environment environment;

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

    /**
     * Test that the configuration ingester mysql endpoint.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException if any DB operation fails
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testIngesterMysqlEndpoint()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        DbEndpoint ingesterMySqlEndpoint = dbConfig.ingesterMySqlEndpoint();
        DbEndpointConfig config = ingesterMySqlEndpoint.getConfig();
        DbEndpointConfig templateConfig = config.getTemplate().getConfig();
        String url = String.format("jdbc:%s://%s:%s", DbAdapter.getJdbcProtocol(templateConfig),
                templateConfig.getHost(),
                DbEndpointResolver.getDefaultPort(templateConfig.getDialect()));
        //Deleting an anonymous user, since there is a problem with user access on older versions of MySQL/MariaDB.
        // More Details about the bug: https://bugs.mysql.com/bug.php?id=31061.
        // @throws SQLException failed because of no such user
        try (Connection conn = DriverManager.getConnection(url,
                environment.getProperty("dbs.mysqlDefault.rootUserName"),
                environment.getProperty("dbs.mysqlDefault.password"))) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("DROP USER ''@'localhost'");
            }
        } catch (SQLException e) {
            logger.warn("An error occurred during the operation of deleting an anonymous user.", e);
        }
        endpointRule.addEndpoints(ingesterMySqlEndpoint);
        Assert.assertEquals(SQLDialect.MYSQL, config.getDialect());
        Assert.assertEquals(3306, config.getPort().intValue());
        // migrations are not needed, data for new search is created once for 10 minutes
        Assert.assertEquals(0, config.getFlywayCallbacks().length);
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
                    + "(oid, type, name, environment, attrs, first_seen, last_seen)"
                    + "VALUES (0, 'VIRTUAL_MACHINE', 'Vm1', null, null, now(), now())");
            return 1L == (long)endpoint.dslContext().fetchValue("SELECT count(*) FROM entity");

        } catch (DataAccessException | SQLException | BadSqlGrammarException e) {
            return false;
        }
    }

    private boolean canWriteMetric(final DbEndpoint endpoint)
            throws UnsupportedDialectException, InterruptedException {
        try {
            endpoint.dslContext().execute("INSERT INTO metric "
                    + "(time, entity_oid, type, provider_oid, key, current, capacity, utilization, consumed, peak_current, peak_consumed, entity_type)"
                    + "VALUES (now(), 0, 'CPU', null, null, null, null, null, 0, 0, 0, 'VIRTUAL_MACHINE')");
            return 1L == (long)endpoint.dslContext().fetchValue("SELECT count(*) FROM metric");
        } catch (DataAccessException | SQLException | BadSqlGrammarException e) {
            return false;
        }
    }
}
