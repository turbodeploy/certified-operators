package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CPUS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.TOTAL_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
import static com.vmturbo.common.protobuf.utils.StringConstants.VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.VM_GROWTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.hamcrest.Matchers;
import org.jooq.Record;
import org.jooq.Table;
import org.junit.After;
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
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.SchemaUtil;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
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

    private static final long ONE_DAY_IN_MILLIS = 86_400_000;

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

    /**
     * These timestamps are used in the "big" database population.
     */
    private static final Timestamp LATEST_DATE_TIMESTAMP =
                                        new Timestamp(1_577_923_200_000L); // Jan 2nd, 2020
    private static final Timestamp PREVIOUS_DATE_TIMESTAMP =
                                        new Timestamp(1_577_836_800_000L); // Jan 1st, 2020
    private static final Timestamp NEXT_DATE_TIMESTAMP =
                                        new Timestamp(1_578_009_600_000L); // Jan 3rd, 2020
    private static final Timestamp PREVIOUS_COMMODITY_STATS_TIMESTAMP =
                                        new Timestamp(1_577_923_080_000L);  // two minutes before latest

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
        doAnswer(invocation -> TimeFrame.DAY).when(timeFrameCalculator).millis2TimeFrame(anyLong());
        final ClusterTimeRangeFactory timeRangeFactory = new ClusterTimeRangeFactory(
                historydbIO, timeFrameCalculator);
        final ComputedPropertiesProcessorFactory computedPropertiesFactory =
                ComputedPropertiesProcessor::new;
        clusterStatsReader = new ClusterStatsReader(historydbIO, timeRangeFactory,
                computedPropertiesFactory, 500);
        System.out.println("Initializing DB - " + testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        // we don't need to recreate the database for each test
        historydbIO.init(false, null, testDbName, Optional.empty());
    }

    /**
     * Performed after each unit test, to clean up data to avoid conflict with other tests.
     *
     * @throws SQLException db error
     * @throws VmtDbException db error
     */
    @After
    public void after() throws SQLException, VmtDbException {
        // clear data in cluster tables before each test so it doesn't affect each other
        clearClusterTableStats();
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
     * Populate data for test cases: minimal DB with two clusters.
     *
     * @throws VmtDbException       if there's a problem
     * @throws InterruptedException if interrupted
     */
    private void populateTestDataSmall() throws VmtDbException, InterruptedException {
        final Timestamp[] latestTimes = {Timestamp.valueOf("2017-12-15 01:23:53"),
                                         Timestamp.valueOf("2017-12-15 01:33:53")};
        final Timestamp[] datesByDay = {Timestamp.valueOf("2017-12-15 00:00:00"),
                                        Timestamp.valueOf("2017-12-14 00:00:00"),
                                        Timestamp.valueOf("2017-12-12 00:00:00")};
        String[] propertyTypes = {HEADROOM_VMS, NUM_VMS};

        final ImmutableBulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10).maxPendingBatches(1).maxBatchRetries(1).maxRetryBackoffMsec(100)
                .build();
        try (SimpleBulkLoaderFactory loaders = new SimpleBulkLoaderFactory(historydbIO, config,
                Executors.newSingleThreadExecutor())) {
            for (final Timestamp time : latestTimes) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_LATEST,
                            time, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                }
            }

            for (final Timestamp date : datesByDay) {
                for (final String propertyType : propertyTypes) {
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY,
                            date, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                    insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH,
                            date, CLUSTER_ID_1, propertyType, propertyType, 20.0);
                }
            }
        }
    }

    /**
     * Populate data for test cases: database with 4 clusters.
     *
     * @throws VmtDbException       if there's a problem
     * @throws InterruptedException if interrupted
     */
    private void populateTestDataBig() throws VmtDbException, InterruptedException {
        final ImmutableBulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10).maxPendingBatches(1).maxBatchRetries(1).maxRetryBackoffMsec(100)
                .build();
        try (SimpleBulkLoaderFactory loaders = new SimpleBulkLoaderFactory(historydbIO, config,
                Executors.newSingleThreadExecutor())) {

            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_MONTH, Timestamp.valueOf("2017-12-1 00:00:00"),
                              CLUSTER_ID_2, HEADROOM_VMS, NUM_VMS, 20.0);

            // memory utilization records in "latest stats"
            final Timestamp minuteAgo = new Timestamp(System.currentTimeMillis() - 60_000);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_3, MEM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_3, MEM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_4, MEM, USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_4, MEM, CAPACITY, 300.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_5, MEM, USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_5, MEM, CAPACITY, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_6, MEM, USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_6, MEM, CAPACITY, 9.0);

            // count records in "latest"
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 8.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_4, NUM_VMS, NUM_VMS, 6.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_4, NUM_HOSTS, NUM_HOSTS, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_5, NUM_VMS, NUM_VMS, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_5, NUM_HOSTS, NUM_HOSTS, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_6, NUM_VMS, NUM_VMS, 4.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, minuteAgo, CLUSTER_ID_6, NUM_HOSTS, NUM_HOSTS, 1.0);

            // memory utilization records in "stats by day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_3,
                              MEM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_3,
                              MEM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_4,
                              MEM, USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_4,
                              MEM, CAPACITY, 300.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_5,
                              MEM, USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_5,
                              MEM, CAPACITY, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_6,
                              MEM, USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP, CLUSTER_ID_6,
                              MEM, CAPACITY, 9.0);

            // some past mem records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP,
                              CLUSTER_ID_3, MEM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_COMMODITY_STATS_TIMESTAMP,
                              CLUSTER_ID_3, MEM, CAPACITY, 20.0);

            // CPU headroom records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              CPU_HEADROOM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              CPU_HEADROOM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              CPU_HEADROOM, USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              CPU_HEADROOM, CAPACITY, 300.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              CPU_HEADROOM, USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              CPU_HEADROOM, CAPACITY, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              CPU_HEADROOM, USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              CPU_HEADROOM, CAPACITY, 9.0);

            // some past CPU headroom records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_DATE_TIMESTAMP, CLUSTER_ID_3,
                              CPU_HEADROOM, USED, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, PREVIOUS_DATE_TIMESTAMP, CLUSTER_ID_3,
                              CPU_HEADROOM, CAPACITY, 20.0);

            // mem headroom records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              MEM_HEADROOM, USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              MEM_HEADROOM, CAPACITY, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              MEM_HEADROOM, USED, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              MEM_HEADROOM, CAPACITY, 30.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              MEM_HEADROOM, USED, 8.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              MEM_HEADROOM, CAPACITY, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              MEM_HEADROOM, USED, 0.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              MEM_HEADROOM, CAPACITY, 10.0);

            // storage headroom records
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              STORAGE_HEADROOM, USED, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              STORAGE_HEADROOM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              STORAGE_HEADROOM, USED, 4.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              STORAGE_HEADROOM, CAPACITY, 5.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              STORAGE_HEADROOM, USED, 3.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              STORAGE_HEADROOM, CAPACITY, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              STORAGE_HEADROOM, USED, 6.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              STORAGE_HEADROOM, CAPACITY, 100.0);

            // numCPUs records
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_DATE_TIMESTAMP, CLUSTER_ID_3,
                              NUM_CPUS, NUM_CPUS, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_DATE_TIMESTAMP, CLUSTER_ID_4,
                              NUM_CPUS, NUM_CPUS, 6.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_DATE_TIMESTAMP, CLUSTER_ID_5,
                              NUM_CPUS, NUM_CPUS, 12.0);
            insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_DATE_TIMESTAMP, CLUSTER_ID_6,
                              NUM_CPUS, NUM_CPUS, 2.0);
        }
    }

    private final long nowMillis = System.currentTimeMillis();
    private final long todayMillis = (nowMillis / ONE_DAY_IN_MILLIS) * ONE_DAY_IN_MILLIS;
    private final long yesterdayMillis = todayMillis - ONE_DAY_IN_MILLIS;
    private final long tomorrowMillis = todayMillis + ONE_DAY_IN_MILLIS;

    /**
     * Populate data with current date, to test headroom projection.
     * This method may disable the test, if the test is executed too close to an end of date.
     *
     * @return false means the test should be disabled
     * @throws VmtDbException if there's a DB problem
     * @throws InterruptedException if the connection to the DB is interrupted
     */
    private boolean populateTestDataNow() throws VmtDbException, InterruptedException {
        // This test depends on the condition: yesterday < today < now < tomorrow
        // If now is too close to the next change of date
        // or in the unlikely case that now is exactly equal to a change of date,
        // then disable the test
        if (nowMillis == todayMillis || tomorrowMillis - nowMillis < 120_000) {
            return false;
        }

        final Timestamp today = new Timestamp(todayMillis);
        final Timestamp yesterday = new Timestamp(yesterdayMillis);

        final ImmutableBulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10).maxPendingBatches(1).maxBatchRetries(1).maxRetryBackoffMsec(100)
                .build();
        try (SimpleBulkLoaderFactory loaders = new SimpleBulkLoaderFactory(historydbIO, config,
                Executors.newSingleThreadExecutor())) {
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, yesterday, CLUSTER_ID_3,
                    CPU_HEADROOM, USED, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, yesterday, CLUSTER_ID_3,
                    CPU_HEADROOM, CAPACITY, 30.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, today, CLUSTER_ID_3,
                    CPU_HEADROOM, USED, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, today, CLUSTER_ID_3,
                    CPU_HEADROOM, CAPACITY, 30.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, today, CLUSTER_ID_3,
                    VM_GROWTH, VM_GROWTH, 300.0);
        }
        return true;
    }

    private <R extends Record> void insertStatsRecord(SimpleBulkLoaderFactory loaders,
            Table<R> table, Timestamp timeOrDate, String clusteerId,
            String propertyType, String propertySubtype, Double value) throws InterruptedException {
        R record = table.newRecord();
        record.setValue(table.field(RECORDED_ON, Timestamp.class), timeOrDate);
        record.setValue(table.field(INTERNAL_NAME, String.class), clusteerId);
        record.setValue(table.field(PROPERTY_TYPE, String.class), propertyType);
        record.setValue(table.field(PROPERTY_SUBTYPE, String.class), propertySubtype);
        record.setValue(table.field(VALUE, Double.class), value);
        loaders.getLoader(table).insert(record);
    }

    /**
     * Clear all cluster stats tables.
     *
     * @throws SQLException sql error
     * @throws VmtDbException db error
     */
    private void clearClusterTableStats() throws SQLException, VmtDbException {
        try (Connection conn = historydbIO.connection()) {
            Stream.of(CLUSTER_STATS_BY_MONTH, CLUSTER_STATS_BY_DAY, CLUSTER_STATS_BY_HOUR,
                    CLUSTER_STATS_LATEST).forEach(table -> historydbIO.using(conn).truncateTable(table).execute());
        }
    }

    /**
     * Test projection feature.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testHeadroomProjections() throws Exception {
        if (!populateTestDataNow()) {
            return;
        }
        final ClusterStatsRequest request = constructTestInputWithDates(
                Optional.of(new Timestamp(todayMillis)),
                Optional.of(new Timestamp(tomorrowMillis)), true);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(4, entityStats.get(0).getStatSnapshotsCount());
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(CPU_HEADROOM, entityStats.get(0).getStatSnapshots(i)
                                                    .getStatRecords(0).getName());
        }

        // check values and epochs for the four records:
        // there should be one historical, one current (now, extra current snapshot),
        // and two projected (tomorrow + day after tomorrow, due to days difference + 1)
        final EntityStats stats = entityStats.get(0);

        // First & second record are projections
        Assert.assertEquals(StatEpoch.PROJECTED, stats.getStatSnapshots(0).getStatEpoch());
        Assert.assertEquals(0, stats.getStatSnapshots(0).getStatRecords(0).getUsed().getAvg(), 0.1);
        Assert.assertEquals(StatEpoch.PROJECTED, stats.getStatSnapshots(1).getStatEpoch());
        Assert.assertEquals(10, stats.getStatSnapshots(1).getStatRecords(0).getUsed().getAvg(), 0.1);

        // Third record is "current" and the values agree with that of the latest historical record
        Assert.assertEquals(StatEpoch.CURRENT, stats.getStatSnapshots(2).getStatEpoch());
        Assert.assertEquals(20, stats.getStatSnapshots(2).getStatRecords(0).getUsed().getAvg(), 0.1);
        // fourth records are "historical" and they come from the database
        Assert.assertEquals(StatEpoch.HISTORICAL, stats.getStatSnapshots(3).getStatEpoch());
        Assert.assertEquals(20, stats.getStatSnapshots(3).getStatRecords(0).getUsed().getAvg(), 0.1);
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws Exception if there's a problem
     */
    @Test
    public void testGetStatsRecordsByDayWithoutCommodityName() throws Exception {
        populateTestDataSmall();
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-14").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.empty(),
                        Collections.emptySet());
        assertEquals(4, result.size());
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset. One
     * commodity name is specified.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByDayWithOneCommodityName() throws Exception {
        populateTestDataSmall();
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
     * Date range is provided.  The start and end dates are within the range in the dataset. Show
     * that the range is inclusive of both end dates.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByDayWithDateRange1() throws Exception {
        populateTestDataSmall();
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
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByDayWithDateRange2() throws Exception {
        populateTestDataSmall();
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS, NUM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-12-10").getTime(),
                        Date.valueOf("2017-12-18").getTime(),
                        Optional.empty(),
                        commodityNames);
        assertEquals(6, result.size());
    }

    /**
     * Check that a request requiring month timeframe operates correctly.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByMonth() throws Exception {
        populateTestDataSmall();
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS, NUM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.of(TimeFrame.MONTH),
                        commodityNames);
        assertEquals(6, result.size());
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByMonthWithoutCommodityName() throws Exception {
        populateTestDataSmall();
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.of(TimeFrame.MONTH),
                        Collections.emptySet());
        assertEquals(6, result.size());
    }

    /**
     * Date range is provided. The start and end dates are within the range in the dataset.
     * CommodityNames is empty. Show that all types of commodities will be returned.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsByMonthWithOneCommodityName() throws Exception {
        populateTestDataSmall();
        Set<String> commodityNames = Sets.newHashSet(HEADROOM_VMS);
        List<ClusterStatsRecordReader> result =
                clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                        Date.valueOf("2017-09-10").getTime(),
                        Date.valueOf("2017-12-15").getTime(),
                        Optional.of(TimeFrame.MONTH),
                        commodityNames);
        assertEquals(3, result.size());
    }

    /**
     * Test time range resolution in a scenario that mimics UI's Top-N widget.
     *
     * @throws Exception if there's an issue
     */
    @Test
    public void testGetStatsRecordsLatestAvailable() throws Exception {
        populateTestDataSmall();
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
                        Optional.of(TimeFrame.LATEST),
                        commodityNames);

        // Db should return data from most recent date available i.e 2017-12-15.
        assertEquals(1, result.size());
        assertTrue(result.stream()
                .map(ClusterStatsRecordReader::getRecordedOn)
                .allMatch(date -> date.equals(Timestamp.valueOf("2017-12-15 01:33:53"))));

        t1 = Date.valueOf("2017-12-13").getTime();
        result = clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                t1, t1, Optional.of(TimeFrame.DAY), commodityNames);
        // Db should return data from 2017-12-12 as that is most recent data on or before 2017-12-13 for these commodities.
        assertEquals(1, result.size());
        assertTrue(result.stream()
                .map(ClusterStatsRecordReader::getRecordedOn)
                .allMatch(date -> date.equals(Timestamp.valueOf("2017-12-12 00:00:00"))));

        t1 = Date.valueOf("2017-12-12").getTime();
        result = clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(Long.parseLong(CLUSTER_ID_1),
                t1, t1, Optional.of(TimeFrame.DAY), commodityNames);
        // Db should return data from 2017-12-12 as thats the most recent one w.r.t given date.
        assertEquals(1, result.size());
        assertTrue(result.stream()
                .map(ClusterStatsRecordReader::getRecordedOn)
                .allMatch(date -> date.equals(Timestamp.valueOf("2017-12-12 00:00:00"))));
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
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), MEM,
                                                               false, 0, 100);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }

        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(4, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(2).getOid()));
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(entityStats.get(3).getOid()));
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(MEM,
                entityStats.get(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).getCapacity().getAvg() > 0.0);
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).hasUsed());
            Assert.assertEquals("KB", entityStats.get(i).getStatSnapshots(0)
                                                   .getStatRecords(0).getUnits());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     *
     * <p>In this test we check that market-scoped cluster stats request works correctly when a
     * computed stat is requested.</p>
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersRatioStat() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), NUM_VMS_PER_HOST,
                false, 0, 100);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }

        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(4, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(2).getOid()));
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(entityStats.get(3).getOid()));
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(NUM_VMS_PER_HOST,
                    entityStats.get(i).getStatSnapshots(0)
                            .getStatRecords(0).getName());
        }
        Assert.assertEquals(10.0, entityStats.get(0).getStatSnapshots(0)
                .getStatRecords(0).getUsed().getAvg(), 0.0);
        Assert.assertEquals(8.0, entityStats.get(1).getStatSnapshots(0)
                .getStatRecords(0).getUsed().getAvg(), 0.0);
        Assert.assertEquals(6.0, entityStats.get(2).getStatSnapshots(0)
                .getStatRecords(0).getUsed().getAvg(), 0.0);
        Assert.assertEquals(4.0, entityStats.get(3).getStatSnapshots(0)
                .getStatRecords(0).getUsed().getAvg(), 0.0);
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we have a scope with a single id and we collect
     * and sort by CPU headroom.  Past headrooms should not count
     * in the sorting and should not appear in the result, since this
     * is a request without start/end date (which means fetch only the
     * latest stats).
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersHeadroom() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(
                ImmutableSet.of(Long.valueOf(CLUSTER_ID_3), Long.valueOf(CLUSTER_ID_5)),
                CPU_HEADROOM, true, 0, 2);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(2, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(entityStats.get(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(1, entityStats.get(i).getStatSnapshotsCount());
            Assert.assertEquals(CPU_HEADROOM,
                                entityStats.get(i).getStatSnapshots(0)
                                                .getStatRecords(0).getName());
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).hasUsed());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we are asking for both a headroom and a commodity.
     * Even though these are stored in different DB tables, they should
     * both be in the end result.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetClustersHeadroomAndMem() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(
                Collections.singletonList(Long.valueOf(CLUSTER_ID_3)),
                CPU_HEADROOM, true, 0, 2, MEM);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (!responseChunk.hasPaginationResponse()) {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));

        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());
        final List<String> fetchedStats =
                entityStats.get(0).getStatSnapshotsList().stream()
                    .flatMap(s -> s.getStatRecordsList().stream())
                    .map(StatRecord::getName)
                    .collect(Collectors.toList());
        Assert.assertThat(fetchedStats, Matchers.containsInAnyOrder(CPU_HEADROOM, MEM));
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
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(),
                                                               NUM_CPUS, false,
                                                               1, 2);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertEquals("3", paginationResponse.getNextCursor());
        Assert.assertEquals(2, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(1, entityStats.get(i).getStatSnapshotsCount());
            Assert.assertEquals(NUM_CPUS,
                                entityStats.get(i).getStatSnapshots(0)
                                            .getStatRecords(0).getName());
            assertTrue(entityStats.get(i).getStatSnapshots(0)
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
        populateTestDataBig();
        final CommodityRequest commodityRequest = CommodityRequest.newBuilder()
                                                    .setCommodityName(CPU_HEADROOM)
                                                    .build();
        final ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                                                .setStats(StatsFilter.newBuilder()
                                                            .addCommodityRequests(commodityRequest))
                                                .build();
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(4, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(entityStats.get(1).getOid()));
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(2).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(3).getOid()));
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(1, entityStats.get(i).getStatSnapshotsCount());
            Assert.assertEquals(CPU_HEADROOM,
                                entityStats.get(i).getStatSnapshots(0)
                                        .getStatRecords(0).getName());
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).hasUsed());
        }
    }

    /**
     * Tests that createClusterStatsResponseList effectively chunks the entityStats and the
     * pagination parameters.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testCreateClusterStatsResponseList() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), MEM,
            false, 0, 100);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        Assert.assertEquals(1,
            response.stream().filter(ClusterStatsResponse::hasPaginationResponse).count());
        Assert.assertEquals(1,
            response.stream().filter(ClusterStatsResponse::hasSnapshotsChunk).count());
    }

    /**
     * A cluster stats request with an end date should have a start date.
     *
     * @throws Exception expected
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEndDateWithNoStartDate() throws Exception {
        populateTestDataBig();
        clusterStatsReader.getStatsRecords(constructTestInputWithDates(
                Optional.empty(), Optional.of(LATEST_DATE_TIMESTAMP), false));
    }

    /**
     * The end date in a cluster stats request should not be earlier than the start date.
     *
     * @throws Exception expected
     */
    @Test(expected = IllegalArgumentException.class)
    public void testStartAfterEnd() throws Exception {
        populateTestDataBig();
        clusterStatsReader.getStatsRecords(constructTestInputWithDates(
                Optional.of(NEXT_DATE_TIMESTAMP), Optional.of(LATEST_DATE_TIMESTAMP), false));
    }

    /**
     * If the start date in a cluster stats request is in the future, only projected stats will be
     * returned.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testStartAfterNow() throws Exception {
        if (!populateTestDataNow()) {
            return;
        }
        final Timestamp future = new Timestamp(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
        ClusterStatsRequest request = constructTestInputWithDates(Optional.of(future), Optional.of(future), true);
        List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        assertEquals(2, response.size());
        response.stream().filter(ClusterStatsResponse::hasSnapshotsChunk).forEach(stat ->
                stat.getSnapshotsChunk().getSnapshotsList().forEach(entityStats ->
                        entityStats.getStatSnapshotsList().forEach(statSnapshot ->
                                assertEquals(StatEpoch.PROJECTED, statSnapshot.getStatEpoch()))));
    }

    /**
     * If only a start date is provided in a cluster stats request, behave as if
     * the end time is now.  This test is valid when executed after 2017 in a computer
     * with accurate watch.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testNoEndDateMeansNow() throws Exception {
        populateTestDataBig();
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(constructTestInputWithDates(
                    Optional.of(PREVIOUS_DATE_TIMESTAMP), Optional.empty(), false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(LATEST_DATE_TIMESTAMP.getTime(),
                            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
        Assert.assertEquals(PREVIOUS_DATE_TIMESTAMP.getTime(),
                            entityStats.get(0).getStatSnapshots(1).getSnapshotDate());
    }

    /**
     * In a cluster stats request bring only relevant historical data.
     * Headroom stats version.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testStartAndEndDate() throws Exception {
        populateTestDataBig();
        final List<ClusterStatsResponse> response =
                clusterStatsReader.getStatsRecords(constructTestInputWithDates(
                        Optional.of(PREVIOUS_DATE_TIMESTAMP), Optional.of(PREVIOUS_DATE_TIMESTAMP),
                        false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(PREVIOUS_DATE_TIMESTAMP.getTime(),
                            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
    }

    /**
     * In a cluster stats request bring only relevant historical data.
     * Non-headroom stats version.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testStartAndEndDateNonHeadroom() throws Exception {
        populateTestDataBig();
        final Timestamp fiveMinutesAgo = new Timestamp(System.currentTimeMillis() - 300_000);
        final Timestamp now = new Timestamp(System.currentTimeMillis() - 1);
        final List<ClusterStatsResponse> response =
                clusterStatsReader.getStatsRecords(
                        constructTestInputWithDates(Optional.of(fiveMinutesAgo), Optional.of(now), MEM,
                                false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshotsCount());
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we sort by a stat that does not exist in the results.
     * Even though sorting is not possible, we still like to get the
     * cluster stats back.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersSortByNonExistent() throws Exception {
        populateTestDataBig();
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                            .setStatName(MEM))
                                    .build();
        final CommodityRequest commodityRequest = CommodityRequest.newBuilder()
                                                        .setCommodityName(CPU_HEADROOM)
                                                        .build();
        final ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                                                .setStats(StatsFilter.newBuilder()
                                                                .addCommodityRequests(commodityRequest))
                                                .setPaginationParams(PaginationParameters.newBuilder()
                                                                        .setAscending(true)
                                                                        .setOrderBy(orderBy))
                                                .build();
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }

        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(4, entityStats.size());
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(CPU_HEADROOM,
                                entityStats.get(i).getStatSnapshots(0)
                                    .getStatRecords(0).getName());
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).getCapacity().getAvg() > 0.0);
            assertTrue(entityStats.get(i).getStatSnapshots(0)
                    .getStatRecords(0).hasUsed());
        }
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we have a scope with a single id and we collect
     * and sort by total headroom.  The extra attributes needed to
     * calculate total headroom (i.e., storage, CPU, and memory headroom)
     * should not appear in the result.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetClustersTotalHeadroom() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(
                Collections.emptySet(), TOTAL_HEADROOM, true, 0, 4);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        PaginationResponse paginationResponse = null;
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (responseChunk.hasPaginationResponse()) {
                paginationResponse = responseChunk.getPaginationResponse();
            } else {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertFalse(paginationResponse.hasNextCursor());
        Assert.assertEquals(4, entityStats.size());

        final double epsilon = 0.1;

        // First should be cluster 6 with 0/9 total headroom
        Assert.assertEquals(CLUSTER_ID_6, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot1 = entityStats.get(0).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot1.getName());
        Assert.assertEquals(0.0, statSnapshot1.getUsed().getAvg(), epsilon);
        Assert.assertEquals(9.0, statSnapshot1.getCapacity().getAvg(), epsilon);

        // Second should be cluster 3 with 1/10 total headroom
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        Assert.assertEquals(1, entityStats.get(1).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot2 = entityStats.get(1).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot2.getName());
        Assert.assertEquals(1.0, statSnapshot2.getUsed().getAvg(), epsilon);
        Assert.assertEquals(10.0, statSnapshot2.getCapacity().getAvg(), epsilon);

        // Third should be cluster 5 with 3/9 total headroom
        Assert.assertEquals(CLUSTER_ID_5, Long.toString(entityStats.get(2).getOid()));
        Assert.assertEquals(1, entityStats.get(2).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot3 = entityStats.get(2).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot3.getName());
        Assert.assertEquals(3.0, statSnapshot3.getUsed().getAvg(), epsilon);
        Assert.assertEquals(9.0, statSnapshot3.getCapacity().getAvg(), epsilon);

        // Last should be cluster 4 with 4/5 total headroom
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(3).getOid()));
        Assert.assertEquals(1, entityStats.get(3).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot4 = entityStats.get(3).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot4.getName());
        Assert.assertEquals(4.0, statSnapshot4.getUsed().getAvg(), epsilon);
        Assert.assertEquals(5.0, statSnapshot4.getCapacity().getAvg(), epsilon);
    }

    private ClusterStatsRequest constructTestInput(
            @Nonnull Collection<Long> scope, @Nonnull String orderByStat,
            boolean ascending, int offset, int limit,
            @Nonnull String... additionalStats) {
        final OrderBy orderBy = OrderBy.newBuilder()
                                    .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                        .setStatName(orderByStat))
                                    .build();
        final CommodityRequest orderByCommodityRequest = CommodityRequest.newBuilder()
                                                                .setCommodityName(orderByStat)
                                                                .build();
        final StatsFilter.Builder statsFilterBuilder = StatsFilter.newBuilder()
                                                            .addCommodityRequests(orderByCommodityRequest);
        Arrays.stream(additionalStats)
                .map(s -> CommodityRequest.newBuilder().setCommodityName(s))
                .forEach(statsFilterBuilder::addCommodityRequests);
        return ClusterStatsRequest.newBuilder()
                    .addAllClusterIds(scope)
                    .setStats(statsFilterBuilder)
                    .setPaginationParams(PaginationParameters.newBuilder()
                                            .setAscending(ascending)
                                            .setOrderBy(orderBy)
                                            .setCursor(Integer.toString(offset))
                                            .setLimit(limit))
                    .build();
    }

    private ClusterStatsRequest constructTestInputWithDates(@Nonnull Optional<Timestamp> startDate,
            @Nonnull Optional<Timestamp> endDate, @Nonnull String stat, boolean requestProjected) {
        final CommodityRequest.Builder commodityRequest = CommodityRequest.newBuilder()
                                                                .setCommodityName(stat);
        final StatsFilter.Builder statsFilterBuilder = StatsFilter.newBuilder()
                                                            .addCommodityRequests(commodityRequest);
        startDate.ifPresent(t -> statsFilterBuilder.setStartDate(t.getTime()));
        endDate.ifPresent(t -> statsFilterBuilder.setEndDate(t.getTime()));
        statsFilterBuilder.setRequestProjectedHeadroom(requestProjected);
        return ClusterStatsRequest.newBuilder()
                    .addClusterIds(Long.parseLong(CLUSTER_ID_3))
                    .setStats(statsFilterBuilder)
                    .build();
    }

    private ClusterStatsRequest constructTestInputWithDates(@Nonnull Optional<Timestamp> startDate,
            @Nonnull Optional<Timestamp> endDate, boolean requestProjected) {
        return constructTestInputWithDates(startDate, endDate, CPU_HEADROOM, requestProjected);
    }
}
