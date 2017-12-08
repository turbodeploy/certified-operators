package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Properties;

import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.reports.db.BasedbIO;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.reports.util.DBConnectionPool;
import com.vmturbo.reports.util.SchemaUtil;

/**
 * Unit test for {@link ClusterStatsWriter}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ClusterStatsWriterTest.TestConfig.class)
public class ClusterStatsWriterTest {
    @Autowired
    private HistorydbIO historydbIO;

    private ClusterStatsWriter clusterStatsWriter;

    private static final String TEST_DB_NAME = "vmt_testdb_" + System.currentTimeMillis();

    @Before
    public void setup() throws Exception {
        HistorydbIO.mappedSchemaForTests = TEST_DB_NAME;
        System.out.println("Initializing DB - " + TEST_DB_NAME);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, TEST_DB_NAME);
        clusterStatsWriter = new ClusterStatsWriter(historydbIO);
    }

    @After
    public void after() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(TEST_DB_NAME);
            System.out.println("Dropped DB - " + TEST_DB_NAME);
        } catch (VmtDbException e) {
            System.out.println("Problem dropping db: " + TEST_DB_NAME);
        }
    }

    /**
     * Use ClusterStatsWrite to insert a record with headroom value to the cluster_stat_by_day table.
     *
     * @throws Exception
     */
    @Test
    public void testSaveClusterHeadroom() throws Exception {
        final long clusterOID = 1000L;
        final long headroomValue = 20L;
        clusterStatsWriter.saveClusterHeadroom(clusterOID, headroomValue);

        SelectConditionStep<ClusterStatsByDayRecord> selectStmt = historydbIO.getJooqBuilder()
                .selectFrom(CLUSTER_STATS_BY_DAY)
                .where(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE.eq("headroomVMs"));
        Result<ClusterStatsByDayRecord> statsRecords =
                (Result<ClusterStatsByDayRecord>) historydbIO.execute(BasedbIO.Style.IMMEDIATE,
                selectStmt);

        // One record created
        assertEquals(statsRecords.size(), 1);

        // Check value of headroom value
        assertTrue(statsRecords.get(0).getValue(CLUSTER_STATS_BY_DAY.VALUE)
                .compareTo(BigDecimal.valueOf(headroomValue)) == 0);
    }

    @Configuration
    public static class TestConfig {
        @Bean
        public static PropertySourcesPlaceholderConfigurer propertiesResolver() {
            final PropertySourcesPlaceholderConfigurer propertiesConfigureer
                    = new PropertySourcesPlaceholderConfigurer();

            Properties properties = new Properties();
            properties.setProperty("databaseName", TEST_DB_NAME);
            properties.setProperty("adapter", "mysql");
            properties.setProperty("hostName", "localhost");

            propertiesConfigureer.setProperties(properties);
            return propertiesConfigureer;
        }

        @Bean
        HistorydbIO historydbIO() {
            return new HistorydbIO();
        }
    }
}
