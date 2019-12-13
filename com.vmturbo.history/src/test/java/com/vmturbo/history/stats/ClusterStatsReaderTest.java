package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.InsertSetMoreStep;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.BasedbIO.Style;
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
    private static boolean isFirstSetup = true;
    private static String testDbName;
    private static HistorydbIO historydbIO;

    @Autowired
    private DbTestConfig dbTestConfig;

    private ClusterStatsReader clusterStatsReader;

    private String clusterId1 = "1234567890";
    private String clusterId2 = "3333333333";
    private String clusterId3 = "1111111111";

    @Before
    public void setup() throws Exception {
        if (isFirstSetup) {
            //This is to avoid dropping and recreating the DB
            //We can't use BeforeClass for this because 'dbTestConfig' is Autowired
            testDbName = dbTestConfig.testDbName();
            historydbIO = dbTestConfig.historydbIO();
            HistorydbIO.mappedSchemaForTests = testDbName;
            HistorydbIO.setSharedInstance(historydbIO);
        }
        System.out.println("Initializing DB - " + testDbName);
        historydbIO.init(false, null, testDbName, Optional.empty());
        clusterStatsReader = new ClusterStatsReader(historydbIO);

        if (isFirstSetup) {
            populateTestData();
            populateTestDataForTotalHeadroomTests();
            isFirstSetup = false;
        }
    }

    @After
    public void after() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
    }

    @AfterClass
    public static void afterClass() throws Throwable {
        try {
            //Re-init DB once more so that we can drop it.
            historydbIO.init(false, null, testDbName, Optional.empty());
            DBConnectionPool.instance.getInternalPool().close();
            SchemaUtil.dropDb(testDbName);
            System.out.println("Dropped DB - " + testDbName);
        } catch (VmtDbException e) {
            System.out.println("Problem dropping db: " + testDbName);
        }

        isFirstSetup = true;
        testDbName = null;
        historydbIO = null;
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
     * Populate data for testing Total Headroom aggregation queries.
     *
     * @throws VmtDbException if any issue occurs.
     */
    private void populateTestDataForTotalHeadroomTests() throws VmtDbException {
        String[] datesByDay = {"2017-05-15", "2017-05-14", "2017-05-12"};
        String[] commodityNames = {"MemHeadroom", "CPUHeadroom", "StorageHeadroom"};
        String[] propertySubTypes = {"used", "capacity"};
        long[][] usedValues = {
            {50L, 200L, 300L},      //Total Headroom Used = 50
            {500L, 200L, 1400L},    //Total Headroom Used = 200
            {199L, 200L, 100L}      //Total Headroom Used = 100
        };
        long[][] capacityValues = {
            {1000L, 500L, 2000L},   //Total Headroom Capacity = 500
            {1000L, 500L, 2000L},   //Total Headroom Capacity = 500
            {1000L, 500L, 2000L}    //Total Headroom Capacity = 500
        };

        for (int i = 0; i < datesByDay.length; i++) {
            for (int j = 0; j < commodityNames.length; j++) {
                for (int k = 0; k < propertySubTypes.length; k++) {
                    long value = k == 0 ? usedValues[i][j] : capacityValues[i][j];
                    historydbIO.execute(Style.FORCED, historydbIO.getJooqBuilder()
                        .insertInto(CLUSTER_STATS_BY_DAY)
                        .set(Tables.CLUSTER_STATS_BY_DAY.RECORDED_ON, Date.valueOf(datesByDay[i]))
                        .set(Tables.CLUSTER_STATS_BY_DAY.INTERNAL_NAME, clusterId3)
                        .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_TYPE, commodityNames[j])
                        .set(Tables.CLUSTER_STATS_BY_DAY.PROPERTY_SUBTYPE, propertySubTypes[k])
                        .set(Tables.CLUSTER_STATS_BY_DAY.VALUE, BigDecimal.valueOf(value)));
                }
            }
        }

        String[] datesByMonth = {"2017-05-01", "2017-06-01", "2017-07-01"};

        for (int i = 0; i < datesByMonth.length; i++) {
            for (int j = 0; j < commodityNames.length; j++) {
                for (int k = 0; k < propertySubTypes.length; k++) {
                    long value = k == 0 ? usedValues[i][j] : capacityValues[i][j];
                    historydbIO.execute(Style.FORCED, historydbIO.getJooqBuilder()
                        .insertInto(CLUSTER_STATS_BY_MONTH)
                        .set(Tables.CLUSTER_STATS_BY_MONTH.RECORDED_ON, Date.valueOf(datesByMonth[i]))
                        .set(Tables.CLUSTER_STATS_BY_MONTH.INTERNAL_NAME, clusterId3)
                        .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_TYPE, commodityNames[j])
                        .set(Tables.CLUSTER_STATS_BY_MONTH.PROPERTY_SUBTYPE, propertySubTypes[k])
                        .set(Tables.CLUSTER_STATS_BY_MONTH.VALUE, BigDecimal.valueOf(value)));
                }
            }
        }
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws VmtDbException vmtdb exception
     */
    @Test
    public void testGetStatsRecordsByDayWithoutCommodityName() throws VmtDbException {
        List<ClusterStatsByDayRecord> result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                Date.valueOf("2017-12-14").getTime(),
                Date.valueOf("2017-12-15").getTime(),
                Collections.emptySet());
        assertEquals(4, result.size());
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * One commodity name is specified.
     *
     * @throws VmtDbException vmtdb exception
     */
    @Test
    public void testGetStatsRecordsByDayWithOneCommodityName() throws VmtDbException {
        Set<String> commodityNames = Sets.newHashSet("headroomVMs");
        List<ClusterStatsByDayRecord> result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId1),
                Date.valueOf("2017-12-14").getTime(),
                Date.valueOf("2017-12-15").getTime(),
                commodityNames);
        assertEquals(2, result.size());
    }

    /**
     * Date range is provided.  The start and end dates are within the range in the dataset.
     * Show that the range is inclusive of both end dates.
     *
     * @throws VmtDbException vmtdb exception
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
     * @throws VmtDbException vmtdb exception
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

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws VmtDbException vmtdb exception
     */
    @Test
    public void testGetStatsRecordsByMonthWithoutCommodityName() throws VmtDbException {
        List<ClusterStatsByMonthRecord> result =
            clusterStatsReader.getStatsRecordsByMonth(Long.parseLong(clusterId1),
                Date.valueOf("2017-09-10").getTime(),
                Date.valueOf("2017-12-15").getTime(),
                Collections.emptySet());
        assertEquals(6, result.size());
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws VmtDbException vmtdb exception
     */
    @Test
    public void testGetStatsRecordsByMonthWithOneCommodityName() throws VmtDbException {
        Set<String> commodityNames = Sets.newHashSet("headroomVMs");
        List<ClusterStatsByMonthRecord> result =
            clusterStatsReader.getStatsRecordsByMonth(Long.parseLong(clusterId1),
                Date.valueOf("2017-09-10").getTime(),
                Date.valueOf("2017-12-15").getTime(),
                commodityNames);
        assertEquals(3, result.size());
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


    /**
     * For the given time frame, there are 3 days worth of records for the commodities that make up
     * Total Headroom.  So, for each day, we expect 2 results (capacity & used) for the stat.
     * This test is to ensure requesting Total Headroom returns data.
     *
     * @throws VmtDbException vmtdb exception.
     */
    @Test
    public void testGetStatsRecordsByDayForTotalHeadroom() throws VmtDbException {
        //PREPARE
        Set<String> commodityNames = Sets.newHashSet("TotalHeadroom");

        //ACT
        List<ClusterStatsByDayRecord> result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId3),
                Date.valueOf("2017-05-12").getTime(),
                Date.valueOf("2017-05-15").getTime(),
                commodityNames);

        //VERIFY
        assertEquals(6, result.size());
    }

    /**
     * Requesting TotalHeadroom and StorageHeadroom should yield the correct number of results
     * and TotalHeadroom should records should contain the expected values based on the calculations
     * perform to aggregate stats for TotalHeadroom.  We also expect StorageHeadroom to return
     * expected results when requested together.
     *
     * @throws VmtDbException vmtdb exception.
     */
    @Test
    public void testGetStatsRecordsByDayForTotalHeadroomAndHasExpectedValues() throws VmtDbException {
        //PREPARE
        Set<String> commodityNames = Sets.newHashSet("TotalHeadroom", "StorageHeadroom");

        //ACT
        List<ClusterStatsByDayRecord> result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId3),
                Date.valueOf("2017-05-12").getTime(),
                Date.valueOf("2017-05-15").getTime(),
                commodityNames);

        //VERIFY that there are 12 records - 3 days * 2 commodities * 2 sub-types (used/capacity)
        assertEquals(12, result.size());

        //VERIFY that there are 3 TotalHeadroom CAPACITY records all equaling 500 VM
        Set<BigDecimal> totalHeadroomCapacityStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(500L).setScale(3, RoundingMode.FLOOR));
        List<ClusterStatsByDayRecord> foundStats = result
            .stream()
            .filter(record -> record.getPropertyType().equals("TotalHeadroom") && record.getPropertySubtype().equals("capacity"))
            .collect(Collectors.toList());
        assertEquals(3, foundStats.size());
        for (ClusterStatsByDayRecord r : foundStats) {
            assertTrue(totalHeadroomCapacityStatsExpected.contains(r.getValue().setScale(3, RoundingMode.FLOOR)));
        }

        //VERIFY that there are 3 TotalHeadroom USED records with values of 50, 200, and 100 VMs
        Set<BigDecimal> totalHeadroomUsedStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(50L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(200L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(100L).setScale(3, RoundingMode.FLOOR));
        Set<BigDecimal> foundStatsSet = result
            .stream()
            .filter(record -> record.getPropertyType().equals("TotalHeadroom") && record.getPropertySubtype().equals("used"))
            .map(record -> record.getValue().setScale(3, RoundingMode.FLOOR))
            .collect(Collectors.toSet());
        assertEquals(3, foundStatsSet.size());
        assertTrue(foundStatsSet.containsAll(totalHeadroomUsedStatsExpected));

        //VERIFY that there are 3 StorageHeadroom CAPACITY records all equaling 2000 VM
        Set<BigDecimal> storageHeadroomCapacityStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(2000).setScale(3, RoundingMode.FLOOR));
        foundStats = result
            .stream()
            .filter(record -> record.getPropertyType().equals("StorageHeadroom") && record.getPropertySubtype().equals("capacity"))
            .collect(Collectors.toList());
        assertEquals(3, foundStats.size());
        for (ClusterStatsByDayRecord r : foundStats) {
            assertTrue(storageHeadroomCapacityStatsExpected.contains(r.getValue().setScale(3, RoundingMode.FLOOR)));
        }

        //VERIFY that there are 3 StorageHeadroom USED records with values of 300, 1400, and 100 VMs
        Set<BigDecimal> storageHeadroomUsedStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(300L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(1400L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(100L).setScale(3, RoundingMode.FLOOR));
        foundStatsSet = result
            .stream()
            .filter(record -> record.getPropertyType().equals("StorageHeadroom") && record.getPropertySubtype().equals("used"))
            .map(record -> record.getValue().setScale(3, RoundingMode.FLOOR))
            .collect(Collectors.toSet());
        assertEquals(3, foundStats.size());
        assertTrue(foundStatsSet.containsAll(storageHeadroomUsedStatsExpected));
    }

    /**
     * Requesting TotalHeadroom for 05/12/2017 should yield a used value that is the same as
     * CPUHeadroom since that commodity headroom stat has the minimum of the three that make up
     * TotalHeadroom. MemHeadroom=199 CPUHeadroom=200  StorageHeadroom=100 so TotalHeadroom=100.
     *
     * @throws VmtDbException vmtdb exception.
     */
    @Test
    public void testGetStatsRecordsByDayForTotalHeadroomUsesCorrectMinValue() throws VmtDbException {
        //PREPARE
        Set<String> commodityNames = Sets.newHashSet("TotalHeadroom");

        //ACT
        List<ClusterStatsByDayRecord> result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId3),
                Date.valueOf("2017-05-12").getTime(),
                Date.valueOf("2017-05-13").getTime(),
                commodityNames);

        //VERIFY that 2 records are returned - 1 day * 1 commodity (TotalHeadroom) * 2 sub-types (used/capacity)
        assertEquals(2, result.size());

        //Get TotalHeadroom used
        ClusterStatsByDayRecord totalHeadroomRecord12 = result.get(0).getPropertySubtype().equals("used") ? result.get(0) : result.get(1);

        //PREPARE
        commodityNames = Sets.newHashSet("StorageHeadroom");

        //ACT
        result =
            clusterStatsReader.getStatsRecordsByDay(Long.parseLong(clusterId3),
                Date.valueOf("2017-05-12").getTime(),
                Date.valueOf("2017-05-13").getTime(),
                commodityNames);

        //VERIFY that 2 records are returned - 1 day * 1 commodity (StorageHeadroom) * 2 sub-types (used/capacity)
        assertEquals(2, result.size());

        //Get StorageHeadroom used
        ClusterStatsByDayRecord storageHeadroomRecord12 = result.get(0).getPropertySubtype().equals("used") ? result.get(0) : result.get(1);

        //VERIFY that TotalHeadroom used on this day is 100 and equal to StorageHeadroom since
        // this is the minimum headroom on that day.
        assertEquals(BigDecimal.valueOf(100.000).setScale(3, RoundingMode.FLOOR),
            totalHeadroomRecord12.getValue().setScale(3, RoundingMode.FLOOR));
        assertEquals(totalHeadroomRecord12.getValue(), storageHeadroomRecord12.getValue());
    }

    /**
     * Requesting TotalHeadroom and StorageHeadroom should yield the correct number of results
     * and TotalHeadroom should records should contain the expected values based on the calculations
     * perform to aggregate stats for TotalHeadroom.  We also expect StorageHeadroom to return
     * expected results when requested together.
     *
     * @throws VmtDbException vmtdb exception.
     */
    @Test
    public void testGetStatsRecordsByMonthForTotalHeadroomAndHasExpectedValues() throws VmtDbException {
        //PREPARE
        Set<String> commodityNames = Sets.newHashSet("TotalHeadroom", "StorageHeadroom");

        //ACT
        List<ClusterStatsByMonthRecord> result =
            clusterStatsReader.getStatsRecordsByMonth(Long.parseLong(clusterId3),
                Date.valueOf("2017-04-01").getTime(),
                Date.valueOf("2017-08-01").getTime(),
                commodityNames);

        //VERIFY that 12 records are returned - 3 months w/ records * 2 commodity (TotalHeadroom) * 2 sub-types (used/capacity)
        assertEquals(12, result.size());

        //VERIFY that there are 3 TotalHeadroom CAPACITY records all equaling 500 VM
        Set<BigDecimal> totalHeadroomCapacityStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(500L).setScale(3, RoundingMode.FLOOR));
        List<ClusterStatsByMonthRecord> foundStats = result
            .stream()
            .filter(record -> record.getPropertyType().equals("TotalHeadroom") && record.getPropertySubtype().equals("capacity"))
            .collect(Collectors.toList());
        assertEquals(3, foundStats.size());
        for (ClusterStatsByMonthRecord r : foundStats) {
            assertTrue(totalHeadroomCapacityStatsExpected.contains(r.getValue().setScale(3, RoundingMode.FLOOR)));
        }

        //VERIFY that there are 3 TotalHeadroom USED records with values of 50, 200, and 100 VMs
        Set<BigDecimal> totalHeadroomUsedStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(50L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(200L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(100L).setScale(3, RoundingMode.FLOOR));
        Set<BigDecimal> foundStatsSet = result
            .stream()
            .filter(record -> record.getPropertyType().equals("TotalHeadroom") && record.getPropertySubtype().equals("used"))
            .map(record -> record.getValue().setScale(3, RoundingMode.FLOOR))
            .collect(Collectors.toSet());
        assertEquals(3, foundStatsSet.size());
        assertTrue(foundStatsSet.containsAll(totalHeadroomUsedStatsExpected));

        //VERIFY that there are 3 StorageHeadroom CAPACITY records all equaling 2000 VM
        Set<BigDecimal> storageHeadroomCapacityStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(2000L).setScale(3, RoundingMode.FLOOR));
        foundStats = result
            .stream()
            .filter(record -> record.getPropertyType().equals("StorageHeadroom") && record.getPropertySubtype().equals("capacity"))
            .collect(Collectors.toList());
        assertEquals(3, foundStats.size());
        for (ClusterStatsByMonthRecord r : foundStats) {
            assertTrue(storageHeadroomCapacityStatsExpected.contains(r.getValue().setScale(3, RoundingMode.FLOOR)));
        }

        //VERIFY that there are 3 StorageHeadroom USED records with values of 300, 1400, and 100 VMs
        Set<BigDecimal> storageHeadroomUsedStatsExpected =
            Sets.newHashSet(BigDecimal.valueOf(300L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(1400L).setScale(3, RoundingMode.FLOOR),
                BigDecimal.valueOf(100L).setScale(3, RoundingMode.FLOOR));
        foundStatsSet = result
            .stream()
            .filter(record -> record.getPropertyType().equals("StorageHeadroom") && record.getPropertySubtype().equals("used"))
            .map(record -> record.getValue().setScale(3, RoundingMode.FLOOR))
            .collect(Collectors.toSet());
        assertEquals(3, foundStats.size());
        assertTrue(foundStatsSet.containsAll(storageHeadroomUsedStatsExpected));
    }

}
