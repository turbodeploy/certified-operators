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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import com.google.common.collect.Sets;
import org.jooq.Record;
import org.jooq.Table;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

    private String clusterId1 = "1234567890";
    private String clusterId2 = "3333333333";
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
        doAnswer(new Answer<TimeFrame>() {
            @Override
            public TimeFrame answer(final InvocationOnMock invocation) throws Throwable {
                return timeFrame;
            }
        }).when(timeFrameCalculator).millis2TimeFrame(anyLong());
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
                            time, clusterId1, propertyType, propertyType, 20.0);
                }
            }

            for (final String date : datesByDay) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                            date, clusterId1, propertyType, propertyType, 20.0);
                }
            }

            // Insert only one commodity other than above for day "2017-12-13" in cluster 1
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                    "2017-12-13", clusterId1, "CPU", "CPU", 20.0);

            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                    datesByDay[0], clusterId2, propertyTypes[0], propertyTypes[1], 20.0);

            String[] datesByMonth = {"2017-10-01", "2017-11-01", "2017-12-01"};

            for (final String date : datesByMonth) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH,
                            date, clusterId1, propertyType, propertyType, 20.0);
                }
            }

            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH,
                    datesByMonth[0], clusterId2, propertyTypes[0], propertyTypes[1], 20.0);

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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
                clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
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
        result = clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
                t1, t1, Optional.empty(), commodityNames);
        // Db should return data from 2017-12-12 as that is most recent data on or before 2017-12-13 for these commodities.
        assertEquals(1, result.size());
        result.stream()
                .map(record -> record.getRecordedOn())
                .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));

        t1 = Date.valueOf("2017-12-12").getTime();
        result = clusterStatsReader.getStatsRecords(Long.parseLong(clusterId1),
                t1, t1, Optional.empty(), commodityNames);
        // Db should return data from 2017-12-12 as thats the most recent one w.r.t given date.
        assertEquals(1, result.size());
        result.stream()
                .map(record -> record.getRecordedOn())
                .allMatch(date -> date.equals(Date.valueOf("2017-12-12")));
    }
}
