package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.common.protobuf.utils.StringConstants.VALUE;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.AvailableTimestamps.AVAILABLE_TIMESTAMPS;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.jooq.Record;
import org.jooq.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.SchemaUtil;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.stats.ClusterStatsReader.ClusterStatsRecordReader;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.ClusterTimeRangeFactory;

/**
 * Unit test for {@link ClusterStatsReader}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ClusterStatsReaderTest {

    private static final String NUM_VMS = PropertySubType.NumVms.getApiParameterName();
    private static final String HEADROOM_VMS = PropertySubType.HeadroomVms.getApiParameterName();
    @Autowired
    private DbTestConfig dbTestConfig;

    private static String testDbName;

    private HistorydbIO historydbIO;

    private ClusterStatsReader clusterStatsReader;

    private static final String CLUSTER_ID_1 = "1234567890";
    private static final String CLUSTER_ID_2 = "3333333333";
    private static final String CLUSTER_ID_3 = "1024";
    private static final String CLUSTER_ID_4 = "2048";
    private static final String CLUSTER_ID_5 = "4096";
    private static final String CLUSTER_ID_6 = "8192";
    private TimeFrame timeFrame = TimeFrame.DAY;
    private static boolean populated = false;

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws Exception if any problem occurs
     */
    @Before
    public void setup() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        // we mock the time frame calculator, which normally computes time frame based on a given
        // time relative to current time. Instead we'll supply a timeframe to be used in each test,
        // and arrange for the calculator to return that timeframe.
        final TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);
        doAnswer(invocation -> timeFrame).when(timeFrameCalculator).millis2TimeFrame(anyLong());
        final ClusterTimeRangeFactory timeRangeFactory = new ClusterTimeRangeFactory(
                historydbIO, timeFrameCalculator);
        final ComputedPropertiesProcessorFactory computedPropertiesFactory =
                (statsFilter, recordsProcessor) ->
                        new ComputedPropertiesProcessor(statsFilter, recordsProcessor);
        clusterStatsReader = new ClusterStatsReader(historydbIO, timeRangeFactory, computedPropertiesFactory);
        System.out.println("Initializing DB - " + testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        // we don't need to recreate the database for each test
        historydbIO.init(false, null, testDbName, Optional.empty());
        // only populate the database for the first test
        if (!populated) {
            populateTestData();
            populated = true;
        }
    }

    /**
     * Tear down our database when tests are complete.
     *
     * @throws Throwable if there's a problem
     */
    @AfterClass
    public static void afterClass() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
            System.out.println("Dropped DB - " + testDbName);
        } catch (VmtDbException e) {
            System.out.println("Problem dropping db: " + testDbName);
        }
    }

    /**
     * Populate data for test cases.
     *
     * @throws VmtDbException       if there's a problem
     * @throws InterruptedException if interrupted
     */
    private void populateTestData() throws VmtDbException, InterruptedException {
        String[] latestTimes = {"2017-12-15 01:23:53", "2017-12-15 01:33:53"};
        String[] datesByDay = {"2017-12-15", "2017-12-14", "2017-12-12"};
        String[] propertyTypes = {HEADROOM_VMS, NUM_VMS};

        final ImmutableBulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10).maxPendingBatches(1).maxBatchRetries(1).maxRetryBackoffMsec(100)
                .build();
        try (SimpleBulkLoaderFactory loaders = new SimpleBulkLoaderFactory(historydbIO, config,
                Executors.newSingleThreadExecutor())) {
            for (final String time : latestTimes) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_LATEST,
                            time, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                }
            }

            for (final String date : datesByDay) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                            date, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                }
            }

            // Insert only one commodity other than above for day "2017-12-13" in cluster 1
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                    "2017-12-13", CLUSTER_ID_1, "CPU", "CPU", 20.0);

            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                    datesByDay[0], CLUSTER_ID_2, propertyTypes[0], propertyTypes[1], 20.0);

            String[] datesByMonth = {"2017-10-01", "2017-11-01", "2017-12-01"};

            for (final String date : datesByMonth) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH,
                            date, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                }
            }

            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH,
                    datesByMonth[0], CLUSTER_ID_2, propertyTypes[0], propertyTypes[1], 20.0);

            // memory utilization records
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_3,
                    StringConstants.MEM, StringConstants.USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_3,
                    StringConstants.MEM, StringConstants.CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_4,
                    StringConstants.MEM, StringConstants.USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_4,
                    StringConstants.MEM, StringConstants.CAPACITY, 300.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_5,
                    StringConstants.MEM, StringConstants.USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_5,
                    StringConstants.MEM, StringConstants.CAPACITY, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_6,
                    StringConstants.MEM, StringConstants.USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_6,
                    StringConstants.MEM, StringConstants.CAPACITY, 9.0);

            // CPU headroom records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_3,
                    StringConstants.CPU_HEADROOM, StringConstants.USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_3,
                    StringConstants.CPU_HEADROOM, StringConstants.CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_4,
                    StringConstants.CPU_HEADROOM, StringConstants.USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_4,
                    StringConstants.CPU_HEADROOM, StringConstants.CAPACITY, 300.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_5,
                    StringConstants.CPU_HEADROOM, StringConstants.USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_5,
                    StringConstants.CPU_HEADROOM, StringConstants.CAPACITY, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_6,
                    StringConstants.CPU_HEADROOM, StringConstants.USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, datesByMonth[0], CLUSTER_ID_6,
                    StringConstants.CPU_HEADROOM, StringConstants.CAPACITY, 9.0);

            // numCPUs records
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_3,
                    StringConstants.NUM_CPUS, StringConstants.NUM_CPUS, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_4,
                    StringConstants.NUM_CPUS, StringConstants.NUM_CPUS, 6.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_5,
                    StringConstants.NUM_CPUS, StringConstants.NUM_CPUS, 12.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, datesByMonth[0], CLUSTER_ID_6,
                    StringConstants.NUM_CPUS, StringConstants.NUM_CPUS, 2.0);

            insertAvailTimestamps(loaders, TimeFrame.LATEST, latestTimes);
            insertAvailTimestamps(loaders, TimeFrame.DAY, datesByDay);
            insertAvailTimestamps(loaders, TimeFrame.MONTH, datesByMonth);

        }
    }

    private <R extends Record> void insertStatsRecord(SimpleBulkLoaderFactory loaders,
            Table<R> table, String timeOrDate, String clusteerId,
            String propertyType, String propertySubtype, Double value) throws InterruptedException {
        R record = table.newRecord();
        if (!timeOrDate.contains(" ")) {
            timeOrDate += " 00:00:00";
        }
        record.setValue(table.field(RECORDED_ON, Timestamp.class), Timestamp.valueOf(timeOrDate));
        record.setValue(table.field(INTERNAL_NAME, String.class), clusteerId);
        record.setValue(table.field(PROPERTY_TYPE, String.class), propertyType);
        record.setValue(table.field(PROPERTY_SUBTYPE, String.class), propertySubtype);
        record.setValue(table.field(VALUE, Double.class), value);
        loaders.getLoader(table).insert(record);
    }

    private void insertAvailTimestamps(SimpleBulkLoaderFactory loaders,
            TimeFrame timeFrame, String... timeOrDates) throws InterruptedException {
        for (String timeOrDate : timeOrDates) {
            if (!timeOrDate.contains(" ")) {
                timeOrDate += " 00:00:00";
            }
            AvailableTimestampsRecord record = AVAILABLE_TIMESTAMPS.newRecord();
            record.setTimeStamp(Timestamp.valueOf(timeOrDate));
            record.setTimeFrame(timeFrame.name());
            record.setHistoryVariety(HistoryVariety.ENTITY_STATS.name());
            loaders.getLoader(AVAILABLE_TIMESTAMPS).insert(record);
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
        timeFrame = TimeFrame.DAY;
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-14").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
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
        timeFrame = TimeFrame.DAY;
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-14").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
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
        timeFrame = TimeFrame.DAY;
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS, NUM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-14").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
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
        timeFrame = TimeFrame.DAY;
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS, NUM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-10").getTime(),
                        Date.valueOf("2017-12-18").getTime(),
                        Optional.empty(),
                        commodityNames);
        assertEquals(6, result.size());
    }

    @Test
    public void testGetStatsRecordsByMonth() throws VmtDbException {
        timeFrame = TimeFrame.MONTH;
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS, NUM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
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
        timeFrame = TimeFrame.MONTH;
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
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
        timeFrame = TimeFrame.MONTH;
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
                        commodityNames);
        assertEquals(3, result.size());
    }

    @Test
    public void testGetStatsRecordsLatestAvailable() throws VmtDbException {
        timeFrame = TimeFrame.LATEST;
        Set<String> commodityNames = Sets.newHashSet(NUM_VMS);

        // Scenario :
        // a) The given time does not currently exist in db but is not far beyond available data
        // b) We pass same start and end date to mimic what UI does for "top N" widget.
        // c) Db contains LATEST data for {"2017-12-15 01:23:53", "2017-12-15 01:33:53"}
        // We should end up with the 1:33:53 records
        long t1 = Timestamp.valueOf("2017-12-15 01:53:00").getTime();
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        t1,
                        t1,
                        Optional.empty(),
                        commodityNames);

        // Db should return data from most recent date available i.e 2017-12-15.
        assertEquals(1, result.size());
        result.stream()
                .map(record -> record.getRecordedOn())
                .allMatch(date -> date.equals(Timestamp.valueOf("2017-12-15 01:33:53")));

        timeFrame = TimeFrame.DAY;
        t1 = Date.valueOf("2017-12-13").getTime();
        result = clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                t1, t1, Optional.empty(), commodityNames);
        // Db should return data from 2017-12-12 as that is most recent data on or before 2017-12-13 for these commodities.
        assertEquals(1, result.size());
        result.stream()
                .map(record -> record.getRecordedOn())
                .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));

        t1 = Date.valueOf("2017-12-12").getTime();
        result = clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                t1, t1, Optional.empty(), commodityNames);
        // Db should return data from 2017-12-12 as thats the most recent one w.r.t given date.
        assertEquals(1, result.size());
        result.stream()
                .map(record -> record.getRecordedOn())
                .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we have an empty scope (= all market) and we collect
     * and sort by mem stats.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersEmptyScope() throws Exception {
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), StringConstants.MEM,
                                                               false, 0, 100);
        final ClusterStatsResponse response = clusterStatsReader.getStatsRecords(request);

        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getSnapshotsCount());
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(response.getSnapshots(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(response.getSnapshots(1).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(response.getSnapshots(2).getOid()));
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(response.getSnapshots(3).getOid()));
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(StringConstants.MEM,
                                response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            Assert.assertTrue(response.getSnapshots(i).getStatSnapshots(0)
                                      .getStatRecords(0).getCapacity().getAvg() > 0.0);
            Assert.assertTrue(response.getSnapshots(i).getStatSnapshots(0)
                                      .getStatRecords(0).hasUsed());
            Assert.assertEquals("KB", response.getSnapshots(i).getStatSnapshots(0)
                                                   .getStatRecords(0).getUnits());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we have a scope with a single id and we collect
     * and sort by CPU headroom.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersHeadroom() throws Exception {
        final ClusterStatsRequest request = constructTestInput(
                ImmutableSet.of(Long.valueOf(CLUSTER_ID_3), Long.valueOf(CLUSTER_ID_5)),
                StringConstants.CPU_HEADROOM, true, 0, 2);
        final ClusterStatsResponse response = clusterStatsReader.getStatsRecords(request);

        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(2, response.getSnapshotsCount());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(response.getSnapshots(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(response.getSnapshots(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(StringConstants.CPU_HEADROOM,
                                response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            Assert.assertTrue(response.getSnapshots(i).getStatSnapshots(0)
                                .getStatRecords(0).hasUsed());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we look at whether pagination happens correctly
     * and we sort by numCPUs.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersWithOffsetAndLimit() throws Exception {
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(),
                                                               StringConstants.NUM_CPUS, false,
                                                               1, 2);
        final ClusterStatsResponse response = clusterStatsReader.getStatsRecords(request);

        Assert.assertEquals("3", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(2, response.getSnapshotsCount());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(response.getSnapshots(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(response.getSnapshots(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(StringConstants.NUM_CPUS,
                                response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            Assert.assertTrue(response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).hasUsed());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, the request does not have pagination parameters.
     * All records should be returned, sorted by their oid.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersMissingPagination() throws Exception {
        final CommodityRequest commodityRequest = CommodityRequest.newBuilder()
                                                    .setCommodityName(StringConstants.CPU_HEADROOM)
                                                    .build();
        final ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                                                .setStats(StatsFilter.newBuilder()
                                                            .addCommodityRequests(commodityRequest))
                                                .build();
        final ClusterStatsResponse response = clusterStatsReader.getStatsRecords(request);

        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getSnapshotsCount());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(response.getSnapshots(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(response.getSnapshots(1).getOid()));
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(response.getSnapshots(2).getOid()));
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(response.getSnapshots(3).getOid()));
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(StringConstants.CPU_HEADROOM,
                                response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            Assert.assertTrue(response.getSnapshots(i).getStatSnapshots(0)
                                        .getStatRecords(0).hasUsed());
        }
    }

    private ClusterStatsRequest constructTestInput(@Nonnull Collection<Long> scope, @Nonnull String statName,
                                                   boolean ascending, int offset, int limit) {
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                        .setStatName(statName))
                                    .build();
        final CommodityRequest commodityRequest = CommodityRequest.newBuilder()
                                                        .setCommodityName(statName)
                                                        .build();
        return ClusterStatsRequest.newBuilder()
                    .addAllClusterIds(scope)
                    .setStats(StatsFilter.newBuilder()
                                    .addCommodityRequests(commodityRequest))
                    .setPaginationParams(PaginationParameters.newBuilder()
                                            .setAscending(ascending)
                                            .setOrderBy(orderBy)
                                            .setCursor(Integer.toString(offset))
                                            .setLimit(limit))
                    .build();
    }
}
