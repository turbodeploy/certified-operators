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

    private static final String XL_DB_MIGRATION_PATH = "db/xl-migrations";

    @Before
    public void setup() throws Exception {
        HistorydbIO.mappedSchemaForTests = TEST_DB_NAME;
        System.out.println("Initializing DB - " + TEST_DB_NAME);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, TEST_DB_NAME, XL_DB_MIGRATION_PATH);
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


    @Test
    public void testInsertClusterStatsByDayRecord() throws Exception {
        final long clusterOID = 1000L;
        final BigDecimal value1 = new BigDecimal(20).setScale(3);
        final BigDecimal value2 = new BigDecimal(40).setScale(3);
        final String propertyType = "type";
        final String propertySubtype = "subtype";
        clusterStatsWriter.insertClusterStatsByDayRecord(clusterOID, propertyType, propertySubtype,
                value1);

        SelectConditionStep<ClusterStatsByDayRecord> selectStmt = historydbIO.getJooqBuilder()
                .selectFrom(CLUSTER_STATS_BY_DAY)
                .where(CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE.eq(propertySubtype))
                .and(CLUSTER_STATS_BY_DAY.PROPERTY_TYPE.eq(propertyType))
                .and(CLUSTER_STATS_BY_DAY.INTERNAL_NAME.eq(Long.toString(clusterOID)));
        Result<ClusterStatsByDayRecord> statsRecords =
                (Result<ClusterStatsByDayRecord>) historydbIO.execute(BasedbIO.Style.IMMEDIATE,
                        selectStmt);

        // One record created
        assertEquals(1, statsRecords.size());

        assertEquals(value1, statsRecords.get(0).getValue(CLUSTER_STATS_BY_DAY.VALUE));

        // Insert the reord again with a new value
        clusterStatsWriter.insertClusterStatsByDayRecord(clusterOID, propertyType, propertySubtype,
                value2);

        statsRecords =
                (Result<ClusterStatsByDayRecord>) historydbIO.execute(BasedbIO.Style.IMMEDIATE,
                        selectStmt);

        // Still one record but it has the new value
        assertEquals(1, statsRecords.size());
        assertEquals(value2, statsRecords.get(0).getValue(CLUSTER_STATS_BY_DAY.VALUE));
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
