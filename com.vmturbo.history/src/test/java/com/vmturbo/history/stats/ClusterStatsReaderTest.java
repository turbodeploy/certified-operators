package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.reports.db.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.reports.db.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static com.vmturbo.reports.db.jooq.JooqUtils.dField;
import static com.vmturbo.reports.db.jooq.JooqUtils.str;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.jooq.Condition;
import org.jooq.InsertSetMoreStep;
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
import com.vmturbo.reports.db.abstraction.Tables;
import com.vmturbo.reports.db.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.reports.db.abstraction.tables.records.ClusterStatsByMonthRecord;
import com.vmturbo.reports.util.DBConnectionPool;
import com.vmturbo.reports.util.SchemaUtil;

/**
 * Unit test for {@link ClusterStatsReader}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ClusterStatsReaderTest.TestConfig.class)
public class ClusterStatsReaderTest {
    @Autowired
    private HistorydbIO historydbIO;

    private ClusterStatsReader clusterStatsReader;

    private static final String TEST_DB_NAME = "vmt_testdb_" + System.currentTimeMillis();

    private String clusterId1 = "1234567890";
    private String clusterId2 = "3333333333";

    @Before
    public void setup() throws Exception {
        HistorydbIO.mappedSchemaForTests = TEST_DB_NAME;
        System.out.println("Initializing DB - " + TEST_DB_NAME);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, TEST_DB_NAME);
        clusterStatsReader = new ClusterStatsReader(historydbIO);
        populateTestData();
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
     * Populate date for test cases.
     *
     * @throws VmtDbException
     */
    private void populateTestData() throws VmtDbException {
        String[] datesByDay = {"2017-12-15", "2017-12-14", "2017-12-13"};
        String[] commodityNames = {"headroomVMs", "numVMs"};

        for (int i = 0; i < datesByDay.length; i++) {
            for (int j = 0; j < commodityNames.length; j++) {
                InsertSetMoreStep<?> insertStmt = historydbIO.getJooqBuilder()
                    .insertInto(CLUSTER_STATS_BY_DAY)
                    .set(Tables.CLUSTER_STATS_BY_DAY.RECORDED_ON, Date.valueOf(datesByDay[i]))
                    .set(Tables.CLUSTER_STATS_BY_DAY.INTERNAL_NAME, clusterId1)
                    .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, commodityNames[j])
                    .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, commodityNames[j])
                    .set(Tables.CLUSTER_STATS_BY_DAY.VALUE, BigDecimal.valueOf(20));
                historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
            }
        }

        // a stat record from another cluster
        InsertSetMoreStep<?> insertStmt = historydbIO.getJooqBuilder()
                .insertInto(CLUSTER_STATS_BY_DAY)
                .set(Tables.CLUSTER_STATS_BY_DAY.RECORDED_ON, Date.valueOf(datesByDay[0]))
                .set(Tables.CLUSTER_STATS_BY_DAY.INTERNAL_NAME, clusterId2)
                .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, commodityNames[0])
                .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, commodityNames[1])
                .set(Tables.CLUSTER_STATS_BY_DAY.VALUE, BigDecimal.valueOf(20));
        historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);

        String[] datesByMonth = {"2017-10-01", "2017-11-01", "2017-12-01"};

        for (int i = 0; i < datesByMonth.length; i++) {
            for (int j = 0; j < commodityNames.length; j++) {
                insertStmt = historydbIO.getJooqBuilder()
                        .insertInto(CLUSTER_STATS_BY_MONTH)
                        .set(Tables.CLUSTER_STATS_BY_MONTH.RECORDED_ON, Date.valueOf(datesByMonth[i]))
                        .set(Tables.CLUSTER_STATS_BY_MONTH.INTERNAL_NAME, clusterId1)
                        .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_TYPE, commodityNames[j])
                        .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_SUBTYPE, commodityNames[j])
                        .set(Tables.CLUSTER_STATS_BY_MONTH.VALUE, BigDecimal.valueOf(20));
                historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
            }
        }

        // a stat record from another cluster
        insertStmt = historydbIO.getJooqBuilder()
                .insertInto(CLUSTER_STATS_BY_MONTH)
                .set(Tables.CLUSTER_STATS_BY_MONTH.RECORDED_ON, Date.valueOf(datesByMonth[0]))
                .set(Tables.CLUSTER_STATS_BY_MONTH.INTERNAL_NAME, clusterId2)
                .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_TYPE, commodityNames[0])
                .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_SUBTYPE, commodityNames[1])
                .set(Tables.CLUSTER_STATS_BY_MONTH.VALUE, BigDecimal.valueOf(20));
        historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
    }

    /**
     * Date range is provided.  The start and end dates are within the range in the dataset.
     * Show that the range is inclusive of both end dates.
     *
     * @throws VmtDbException
     */
    @Test
    public void testGetStatsRecordsByDayWithDateRange1() throws VmtDbException {
        List<String> commodityNames = Arrays.asList("headroomVMs", "numVMs");
        List<ClusterStatsByDayRecord> result =
                clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        Date.valueOf("2017-12-14").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        commodityNames);
        assertEquals(4, result.size());
    }

    /**
     * Date range is provided.  The start and end dates are outside of the range of the dataset.
     *
     * @throws VmtDbException
     */
    @Test
    public void testGetStatsRecordsByDayWithDateRange2() throws VmtDbException {
        List<String> commodityNames = Arrays.asList("headroomVMs", "numVMs");
        List<ClusterStatsByDayRecord> result =
                clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        Date.valueOf("2017-12-10").getTime(),
                        Date.valueOf("2017-12-18").getTime(),
                        commodityNames);
        assertEquals(6, result.size());
    }
    @Test
    public void testGetStatsRecordsByMonth() throws VmtDbException {
        List<String> commodityNames = Arrays.asList("headroomVMs", "numVMs");
        List<ClusterStatsByMonthRecord> result =
                clusterStatsReader.getStatsRecordsByMonth(Long.parseLong(clusterId1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        commodityNames);
        assertEquals(6, result.size());
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
