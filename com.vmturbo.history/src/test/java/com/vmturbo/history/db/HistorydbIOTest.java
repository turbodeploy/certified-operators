package com.vmturbo.history.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.history.stats.DbTestConfig;

/**
 * Tests for HistorydbIO
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class HistorydbIOTest {

    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private DbTestConfig dbTestConfig;
    private String testDbName;
    private HistorydbIO historydbIO;

    @Before
    public void setup() {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
    }

    @Test
    public void testConnectionTimeoutBeforeInitialization() throws VmtDbException {
        historydbIO.setQueryTimeoutSeconds(1234);
        assertEquals(1234, historydbIO.getQueryTimeoutSeconds());
    }

    @Test
    public void testSettingConnectionTimeoutAppliedBeforeInit() throws VmtDbException {
        try {
            historydbIO.setQueryTimeoutSeconds(1234);
            setupDatabase();
            assertEquals(1234, BasedbIO.getInternalConnectionPoolTimeoutSeconds());
        } finally {
            teardownDatabase();
        }
    }

    @Test
    public void testSettingConnectionTimeoutAppliedAfterInit() throws VmtDbException {
        try {
            setupDatabase();
            assertNotEquals(55, BasedbIO.getInternalConnectionPoolTimeoutSeconds());

            historydbIO.setQueryTimeoutSeconds(55);
            assertEquals(55, BasedbIO.getInternalConnectionPoolTimeoutSeconds());
        } finally {
            teardownDatabase();
        }
    }

    private void setupDatabase() throws VmtDbException {
        HistorydbIO.mappedSchemaForTests = testDbName;
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, testDbName);

        BasedbIO.setSharedInstance(historydbIO);

    }

    private void teardownDatabase() {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
        } catch (VmtDbException e) {
            logger.error("Problem dropping db: " + testDbName, e);
        }
    }
}