package com.vmturbo.history.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
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

    @Test
    public void testGetValueFieldSortByPriceIndex() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        Field<Double> field = historydbIO.getValueField(paginationParam, VmStatsLatest.VM_STATS_LATEST);
        assertEquals("avg_value", field.getName());
    }

    @Test
    public void testGetValueFieldSortByCUP() {
        final EntityStatsPaginationParams paginationParam = mock(EntityStatsPaginationParams.class);
        when(paginationParam.getSortCommodity()).thenReturn(StringConstants.CPU);
        Field<Double> field = historydbIO.getValueField(paginationParam, VmStatsLatest.VM_STATS_LATEST);
        // the filed type is org.jooq.impl.Expression which is not exposed. There seems no other way to validate
        // the output other than filed.toString.
        assertEquals("(\"vmtdb\".\"vm_stats_latest\".\"avg_value\" / \"vmtdb\".\"vm_stats_latest\".\"capacity\")"
            , field.toString());
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