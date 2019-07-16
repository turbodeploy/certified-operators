package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.Set;

import org.jooq.InsertSetMoreStep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.SchemaUtil;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByMonthRecord;

/**
 * Unit test for {@link ClusterStatsReader}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class ClusterStatsReaderTest {

    @Autowired
    private DbTestConfig dbTestConfig;

    private String testDbName;

    private HistorydbIO historydbIO;

    private ClusterStatsReader clusterStatsReader;

    private String clusterId1 = "1234567890";
    private String clusterId2 = "3333333333";

    @Before
    public void setup() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.mappedSchemaForTests = testDbName;
        System.out.println("Initializing DB - " + testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, testDbName);
        clusterStatsReader = new ClusterStatsReader(historydbIO);
        populateTestData();
    }

    @After
    public void after() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
            System.out.println("Dropped DB - " + testDbName);
        } catch (VmtDbException e) {
            System.out.println("Problem dropping db: " + testDbName);
        }
    }

    /**
     * Populate date for test cases.
     *
     * @throws VmtDbException
     */
    private void populateTestData() throws VmtDbException {
        String[] datesByDay = {"2017-12-15", "2017-12-14", "2017-12-12"};
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

        // Insert only one commodity other than above for day "2017-12-13" in cluster 1
        InsertSetMoreStep<?> insertStmtOneComm = historydbIO.getJooqBuilder()
                .insertInto(CLUSTER_STATS_BY_DAY)
                .set(Tables.CLUSTER_STATS_BY_DAY.RECORDED_ON, Date.valueOf("2017-12-13"))
                .set(Tables.CLUSTER_STATS_BY_DAY.INTERNAL_NAME, clusterId1)
                .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, "CPU")
                .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, "CPU")
                .set(Tables.CLUSTER_STATS_BY_DAY.VALUE, BigDecimal.valueOf(20));
        historydbIO.execute(BasedbIO.Style.FORCED, insertStmtOneComm);

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
        Set<String> commodityNames = Sets.newHashSet("headroomVMs", "numVMs");
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
        Set<String> commodityNames = Sets.newHashSet("headroomVMs", "numVMs");
        List<ClusterStatsByDayRecord> result =
                clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        Date.valueOf("2017-12-10").getTime(),
                        Date.valueOf("2017-12-18").getTime(),
                        commodityNames);
        assertEquals(6, result.size());
    }

    @Test
    public void testGetStatsRecordsByMonth() throws VmtDbException {
        Set<String> commodityNames = Sets.newHashSet("headroomVMs", "numVMs");
        List<ClusterStatsByMonthRecord> result =
                clusterStatsReader.getStatsRecordsByMonth(Long.parseLong(clusterId1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        commodityNames);
        assertEquals(6, result.size());
    }

    @Test
    public void testGetStatsRecordsByDayWithSameDate() throws VmtDbException {
        Set<String> commodityNames = Sets.newHashSet("headroomVMs", "numVMs");

        // Scenario :
        // a) This date does not currently exist in db.
        // b) We pass same start and end date to mimic what UI does for "top N" widget.
        // c) Db contains data for {"2017-12-15", "2017-12-14", "2017-12-12"}.
        long t1 = Date.valueOf("2017-12-16").getTime();
        List<ClusterStatsByDayRecord> result =
                clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        t1,
                        t1,
                        commodityNames);

        // Db should return data from most recent date available i.e 2017-12-15.
        assertEquals(2, result.size());
        result.stream()
            .map(record -> record.getRecordedOn())
            .allMatch(date -> date.equals(Date.valueOf("2017-12-15")));

        t1 = Date.valueOf("2017-12-13").getTime();
        result = clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        t1, t1, commodityNames);
        // Db should return data from 2017-12-12 as that is most recent data on or before 2017-12-13 for these commodities.
        assertEquals(2, result.size());
        result.stream()
            .map(record -> record.getRecordedOn())
            .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));

        t1 = Date.valueOf("2017-12-12").getTime();
        result = clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                        t1, t1, commodityNames);
        // Db should return data from 2017-12-12 as thats the most recent one w.r.t given date.
        assertEquals(2, result.size());
        result.stream()
            .map(record -> record.getRecordedOn())
            .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));
    }
}
