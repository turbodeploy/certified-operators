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
import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth.CLUSTER_STATS_BY_MONTH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
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

import org.jooq.Record;
import org.jooq.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
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
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
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
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;

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

    private static HistorydbIO historydbIO;

    private ClusterStatsReader clusterStatsReader;

    private TimeFrameCalculator timeFrameCalculator;

    private static final String CLUSTER_ID_1 = "1234567890";
    private static final String CLUSTER_ID_2 = "3333333333";
    private static final String CLUSTER_ID_3 = "1024";
    private static final String CLUSTER_ID_4 = "2048";

    /**
     * These timestamps are used in the "big" database population.
     */
    private static final Timestamp NOW =
        Timestamp.valueOf("2020-01-03 18:41:00");

    private static final Timestamp LATEST_TIMESTAMP1 =
        Timestamp.valueOf("2020-01-03 18:31:00");
    private static final Timestamp LATEST_TIMESTAMP2 =
        Timestamp.valueOf("2020-01-03 17:51:00");

    private static final Timestamp HOUR_TIMESTAMP1 =
        Timestamp.valueOf("2020-01-03 18:00:00");
    private static final Timestamp HOUR_TIMESTAMP2 =
        Timestamp.valueOf("2020-01-03 17:00:00");

    private static final Timestamp DAY_TIMESTAMP1 =
        Timestamp.valueOf("2020-01-02 00:00:00");
    private static final Timestamp DAY_TIMESTAMP2 =
        Timestamp.valueOf("2020-01-01 00:00:00");

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws Exception if any problem occurs
     */
    @Before
    public void setup() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();

        final Clock clock = mock(Clock.class);
        final RetentionPeriodFetcher retentionPeriodFetcher =
            mock(RetentionPeriodFetcher.class);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(NOW.getTime()));
        Mockito.when(retentionPeriodFetcher.getRetentionPeriods())
            .thenReturn(RetentionPeriods.BOUNDARY_RETENTION_PERIODS);
        timeFrameCalculator = new TimeFrameCalculator(clock, retentionPeriodFetcher);

        final ClusterTimeRangeFactory clusterTimeRangeFactory = new ClusterTimeRangeFactory(
                historydbIO, timeFrameCalculator);
        final DefaultTimeRangeFactory defaultTimeRangeFactory = new DefaultTimeRangeFactory(
            historydbIO, timeFrameCalculator, 15, TimeUnit.MINUTES);

        final ComputedPropertiesProcessorFactory computedPropertiesFactory =
                ComputedPropertiesProcessor::new;
        clusterStatsReader = new ClusterStatsReader(historydbIO, clusterTimeRangeFactory,
                defaultTimeRangeFactory, computedPropertiesFactory, 500);
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
        try (Connection conn = historydbIO.getRootConnection()) {
            SchemaUtil.dropDb(testDbName, conn);
            SchemaUtil.dropUser(historydbIO.getUserName(), conn);
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
     * @throws InterruptedException if interrupted
     */
    private void populateTestDataBig() throws InterruptedException {
        populateTestDataBig(true);
    }

    /**
     * Populate data for test cases: database with 4 clusters.
     *
     * @param insertLatest insert latest records or not
     * @throws InterruptedException if interrupted
     */
    private void populateTestDataBig(final boolean insertLatest) throws InterruptedException {
        final ImmutableBulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10).maxPendingBatches(1).maxBatchRetries(1).maxRetryBackoffMsec(100)
                .build();
        try (SimpleBulkLoaderFactory loaders = new SimpleBulkLoaderFactory(historydbIO, config,
                Executors.newSingleThreadExecutor())) {

            if (insertLatest) {
                insertTimeStampRecord(loaders, TimeFrame.LATEST, LATEST_TIMESTAMP1, Timestamp.valueOf("2020-01-03 21:00:00"));
                insertTimeStampRecord(loaders, TimeFrame.LATEST, LATEST_TIMESTAMP2, Timestamp.valueOf("2020-01-03 21:00:00"));

                // cluster_stats_latest
                // Mem used and Mem capacity records in "cluster_stats_latest"
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_3, MEM, USED, 10.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_3, MEM, CAPACITY, 20.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_4, MEM, USED, 100.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_4, MEM, CAPACITY, 300.0);

                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP2, CLUSTER_ID_3, MEM, USED, 100.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP2, CLUSTER_ID_3, MEM, CAPACITY, 200.0);

                // numVMs and numHosts records in "cluster_stats_latest"
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 8.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 1.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_4, NUM_VMS, NUM_VMS, 6.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_4, NUM_HOSTS, NUM_HOSTS, 1.0);

                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP2, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 80.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP2, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 10.0);

                // numCPUs records in "cluster_stats_latest"
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_2, NUM_CPUS, NUM_CPUS, 15.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_3, NUM_CPUS, NUM_CPUS, 10.0);
                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP1, CLUSTER_ID_4, NUM_CPUS, NUM_CPUS, 6.0);

                insertStatsRecord(loaders, CLUSTER_STATS_LATEST, LATEST_TIMESTAMP2, CLUSTER_ID_3, NUM_CPUS, NUM_CPUS, 100.0);
            }

            insertTimeStampRecord(loaders, TimeFrame.HOUR, HOUR_TIMESTAMP1, Timestamp.valueOf("2020-01-06 18:00:00"));
            insertTimeStampRecord(loaders, TimeFrame.HOUR, HOUR_TIMESTAMP2, Timestamp.valueOf("2020-01-06 17:00:00"));
            insertTimeStampRecord(loaders, TimeFrame.DAY, DAY_TIMESTAMP1, Timestamp.valueOf("2020-03-01 00:00:00"));
            insertTimeStampRecord(loaders, TimeFrame.DAY, DAY_TIMESTAMP2, Timestamp.valueOf("2020-02-28 00:00:00"));

            // cluster_stats_by_hour
            // Mem used and Mem capacity records in "cluster_stats_by_hour"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_3, MEM, USED, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_3, MEM, CAPACITY, 2.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_4, MEM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_4, MEM, CAPACITY, 30.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP2, CLUSTER_ID_3, MEM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP2, CLUSTER_ID_3, MEM, CAPACITY, 20.0);

            // numVMs and numHosts records in "cluster_stats_by_hour"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 80.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_4, NUM_VMS, NUM_VMS, 60.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_4, NUM_HOSTS, NUM_HOSTS, 10.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP2, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 8.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP2, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 1.0);

            // numCPUs records in "cluster_stats_by_hour"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_3, NUM_CPUS, NUM_CPUS, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP1, CLUSTER_ID_4, NUM_CPUS, NUM_CPUS, 60.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_HOUR, HOUR_TIMESTAMP2, CLUSTER_ID_3, NUM_CPUS, NUM_CPUS, 10.0);


            // cluster_stats_by_day
            // Mem used and Mem capacity records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3, MEM, USED, 15.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3, MEM, CAPACITY, 25.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4, MEM, USED, 150.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4, MEM, CAPACITY, 350.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3, MEM, USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3, MEM, CAPACITY, 200.0);

            // numVMs and numHosts records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 5.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4, NUM_VMS, NUM_VMS, 60.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4, NUM_HOSTS, NUM_HOSTS, 10.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3, NUM_VMS, NUM_VMS, 18.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3, NUM_HOSTS, NUM_HOSTS, 11.0);

            // CPUHeadroom records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              CPU_HEADROOM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              CPU_HEADROOM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              CPU_HEADROOM, USED, 100.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              CPU_HEADROOM, CAPACITY, 300.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              CPU_HEADROOM, USED, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              CPU_HEADROOM, CAPACITY, 2.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              CPU_HEADROOM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              CPU_HEADROOM, CAPACITY, 30.0);

            // MemHeadroom records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              MEM_HEADROOM, USED, 9.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              MEM_HEADROOM, CAPACITY, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              MEM_HEADROOM, USED, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              MEM_HEADROOM, CAPACITY, 50.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              MEM_HEADROOM, USED, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              MEM_HEADROOM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              MEM_HEADROOM, USED, 4.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              MEM_HEADROOM, CAPACITY, 5.0);

            // StorageHeadroom records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              STORAGE_HEADROOM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_3,
                              STORAGE_HEADROOM, CAPACITY, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              STORAGE_HEADROOM, USED, 30.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1, CLUSTER_ID_4,
                              STORAGE_HEADROOM, CAPACITY, 300.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              STORAGE_HEADROOM, USED, 1.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_3,
                              STORAGE_HEADROOM, CAPACITY, 2.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              STORAGE_HEADROOM, USED, 10.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2, CLUSTER_ID_4,
                              STORAGE_HEADROOM, CAPACITY, 30.0);

            // VMGrowth records in "cluster_stats_by_day"
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1,
                              CLUSTER_ID_3, VM_GROWTH, VM_GROWTH, 25.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP1,
                              CLUSTER_ID_4, VM_GROWTH, VM_GROWTH, 35.0);

            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2,
                              CLUSTER_ID_3, VM_GROWTH, VM_GROWTH, 20.0);
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2,
                              CLUSTER_ID_4, VM_GROWTH, VM_GROWTH, 30.0);


            // a stat record from another cluster
            insertStatsRecord(loaders, CLUSTER_STATS_BY_DAY, DAY_TIMESTAMP2,
                CLUSTER_ID_2, VM_GROWTH, VM_GROWTH, 10.0);
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

    private void insertTimeStampRecord(SimpleBulkLoaderFactory loaders, TimeFrame timeFrame,
                                       Timestamp timestamp, Timestamp expiresAt) throws InterruptedException {
        AvailableTimestampsRecord record = AVAILABLE_TIMESTAMPS.newRecord();
        record.setValue(AVAILABLE_TIMESTAMPS.field("history_variety", String.class), HistoryVariety.ENTITY_STATS.name());
        record.setValue(AVAILABLE_TIMESTAMPS.field("time_frame", String.class), timeFrame.name());
        record.setValue(AVAILABLE_TIMESTAMPS.field("time_stamp", Timestamp.class), timestamp);
        record.setValue(AVAILABLE_TIMESTAMPS.field("expires_at", Timestamp.class), expiresAt);
        loaders.getLoader(AVAILABLE_TIMESTAMPS).insert(record);
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
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), Collections.singleton(MEM),
            Optional.of(LATEST_TIMESTAMP1), Optional.of(NOW), false);
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
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        for (int i = 0; i < 2; i++) {
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
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), Collections.singleton(NUM_VMS_PER_HOST),
            Optional.of(LATEST_TIMESTAMP1), Optional.of(NOW), false);
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
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(NUM_VMS_PER_HOST,
                    entityStats.get(i).getStatSnapshots(0)
                            .getStatRecords(0).getName());
        }
        Assert.assertEquals(6.0, entityStats.get(0).getStatSnapshots(0)
                .getStatRecords(0).getUsed().getAvg(), 0.0);
        Assert.assertEquals(8.0, entityStats.get(1).getStatSnapshots(0)
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
                ImmutableSet.of(Long.valueOf(CLUSTER_ID_3), Long.valueOf(CLUSTER_ID_4)),
                Collections.singleton(CPU_HEADROOM), Optional.of(DAY_TIMESTAMP2), Optional.of(NOW), false);
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
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(2,
                entityStats.get(i).getStatSnapshotsCount());
            Assert.assertEquals(CPU_HEADROOM,
                entityStats.get(i).getStatSnapshots(0).getStatRecords(0).getName());
            Assert.assertTrue(entityStats.get(i).getStatSnapshots(0).getStatRecords(0).hasUsed());
            Assert.assertEquals(DAY_TIMESTAMP1.getTime(),
                entityStats.get(i).getStatSnapshots(0).getSnapshotDate());
            Assert.assertEquals(DAY_TIMESTAMP2.getTime(),
                entityStats.get(i).getStatSnapshots(1).getSnapshotDate());
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
            Collections.singletonList(Long.valueOf(CLUSTER_ID_3)), Arrays.asList(CPU_HEADROOM, MEM),
            Optional.of(DAY_TIMESTAMP1), Optional.of(NOW), false);
        final List<ClusterStatsResponse> response = clusterStatsReader.getStatsRecords(request);
        List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            if (!responseChunk.hasPaginationResponse()) {
                entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
            }
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshots(0).getStatRecordsCount());
        final List<String> fetchedStats =
                entityStats.get(0).getStatSnapshotsList().stream()
                    .flatMap(s -> s.getStatRecordsList().stream())
                    .map(StatRecord::getName)
                    .collect(Collectors.toList());
        Assert.assertThat(fetchedStats, containsInAnyOrder(CPU_HEADROOM, MEM));
    }

    /**
     * Tests {@link ClusterStatsReader#getStatsRecords}.
     * In this test, we are asking for both a headroom and a commodity.
     * Even though these are stored in different DB tables, they should
     * both be in the end result.
     * Time range not provided.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetClustersHeadroomAndMemNoTimeRange() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(
            Collections.singletonList(Long.valueOf(CLUSTER_ID_3)), Arrays.asList(CPU_HEADROOM, MEM),
            Optional.empty(), Optional.empty(), false);
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

        final StatSnapshot snapshot1 = entityStats.get(0).getStatSnapshots(0);
        Assert.assertEquals(1, snapshot1.getStatRecordsCount());
        Assert.assertEquals(DAY_TIMESTAMP1.toInstant().toEpochMilli(), snapshot1.getSnapshotDate());
        Assert.assertEquals(CPU_HEADROOM, snapshot1.getStatRecordsList().get(0).getName());

        final StatSnapshot snapshot2 = entityStats.get(0).getStatSnapshots(1);
        Assert.assertEquals(1, snapshot2.getStatRecordsCount());
        Assert.assertEquals(LATEST_TIMESTAMP1.toInstant().toEpochMilli(), snapshot2.getSnapshotDate());
        Assert.assertEquals(MEM, snapshot2.getStatRecordsList().get(0).getName());
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
        final ClusterStatsRequest request = constructTestInputWithDates(Collections.emptySet(), NUM_CPUS, false,
            1, 1, Optional.of(LATEST_TIMESTAMP1), Optional.of(NOW));
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
        Assert.assertEquals("2", paginationResponse.getNextCursor());
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(NUM_CPUS,
            entityStats.get(0).getStatSnapshots(0).getStatRecords(0).getName());
        Assert.assertEquals(10.0f,
            entityStats.get(0).getStatSnapshots(0).getStatRecords(0).getUsed().getAvg(), 1e-7);
    }

    /**
     * Get last 2 hours Mem records.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetMemLatest() throws Exception {
        populateTestDataBig();
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(constructTestInput(
                Collections.singletonList(Long.parseLong(CLUSTER_ID_3)), Collections.singletonList(MEM),
                Optional.of(LATEST_TIMESTAMP2), Optional.of(NOW), false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(LATEST_TIMESTAMP1.getTime(),
            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
        Assert.assertEquals(LATEST_TIMESTAMP2.getTime(),
            entityStats.get(0).getStatSnapshots(1).getSnapshotDate());
    }

    /**
     * Get last 24 hours Mem records.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetMemHour() throws Exception {
        populateTestDataBig();
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(constructTestInput(
                Collections.singletonList(Long.parseLong(CLUSTER_ID_3)), Collections.singletonList(MEM),
                Optional.of(HOUR_TIMESTAMP2), Optional.of(NOW), false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(HOUR_TIMESTAMP1.getTime(),
            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
        Assert.assertEquals(HOUR_TIMESTAMP2.getTime(),
            entityStats.get(0).getStatSnapshots(1).getSnapshotDate());
    }

    /**
     * Get last 7 days Mem records.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetMemDay() throws Exception {
        populateTestDataBig();
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(constructTestInput(
                Collections.singletonList(Long.parseLong(CLUSTER_ID_3)), Collections.singletonList(MEM),
                Optional.of(DAY_TIMESTAMP2), Optional.of(NOW), false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(DAY_TIMESTAMP1.getTime(),
            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
        Assert.assertEquals(DAY_TIMESTAMP2.getTime(),
            entityStats.get(0).getStatSnapshots(1).getSnapshotDate());
    }

    /**
     * If user asks for latest stats but there's no latest stats, return hourly stats.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetMemNoLatest() throws Exception {
        populateTestDataBig(false);
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(constructTestInput(
                Collections.singletonList(Long.parseLong(CLUSTER_ID_3)), Collections.singletonList(MEM),
                Optional.of(LATEST_TIMESTAMP2), Optional.of(NOW), false));
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshotsCount());
        Assert.assertEquals(HOUR_TIMESTAMP1.getTime(),
            entityStats.get(0).getStatSnapshots(0).getSnapshotDate());
    }

    /**
     * If user asks for all stats without time range provided, return cluster headroom related stats
     * from cluster_stats_by_day table and other stats from cluster_stats_latest table.
     * There are Mem stats in both latest and by_day table.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllStatsNoTimeRange() throws Exception {
        populateTestDataBig(true);
        final List<ClusterStatsResponse> response =
            clusterStatsReader.getStatsRecords(ClusterStatsRequest.newBuilder()
                .addClusterIds(Long.valueOf(CLUSTER_ID_3))
                .build());
        final List<EntityStats> entityStats = new ArrayList<>();
        for (ClusterStatsResponse responseChunk : response) {
            entityStats.addAll(responseChunk.getSnapshotsChunk().getSnapshotsList());
        }
        Assert.assertEquals(1, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(2, entityStats.get(0).getStatSnapshotsCount());

        final StatSnapshot snapshot1 = entityStats.get(0).getStatSnapshots(0);
        Assert.assertEquals(4, snapshot1.getStatRecordsCount());
        Assert.assertEquals(DAY_TIMESTAMP1.toInstant().toEpochMilli(), snapshot1.getSnapshotDate());
        Assert.assertThat(snapshot1.getStatRecordsList().stream().map(StatRecord::getName).collect(Collectors.toList()),
            containsInAnyOrder(MEM_HEADROOM, CPU_HEADROOM, STORAGE_HEADROOM, VM_GROWTH));

        final StatSnapshot snapshot2 = entityStats.get(0).getStatSnapshots(1);
        Assert.assertEquals(4, snapshot2.getStatRecordsCount());
        Assert.assertEquals(LATEST_TIMESTAMP1.toInstant().toEpochMilli(), snapshot2.getSnapshotDate());
        Assert.assertThat(snapshot2.getStatRecordsList().stream().map(StatRecord::getName).collect(Collectors.toList()),
            containsInAnyOrder(MEM, NUM_VMS, NUM_HOSTS, NUM_CPUS));
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
        Assert.assertEquals(2, entityStats.size());
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
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
     * Tests that createClusterStatsResponseList effectively chunks the entityStats and the
     * pagination parameters.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testCreateClusterStatsResponseList() throws Exception {
        populateTestDataBig();
        final ClusterStatsRequest request = constructTestInput(Collections.emptySet(), CPU_HEADROOM,
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
                Optional.empty(), Optional.of(LATEST_TIMESTAMP1), false));
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
                Optional.of(NOW), Optional.of(LATEST_TIMESTAMP1), false));
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
        Assert.assertEquals(2, entityStats.size());
        for (int i = 0; i < 2; i++) {
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
        Assert.assertEquals(2, entityStats.size());

        final double epsilon = 0.1;

        // First should be cluster 4 with 20/50 total headroom
        Assert.assertEquals(CLUSTER_ID_4, Long.toString(entityStats.get(0).getOid()));
        Assert.assertEquals(1, entityStats.get(0).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot1 = entityStats.get(0).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot1.getName());
        Assert.assertEquals(20.0, statSnapshot1.getUsed().getAvg(), epsilon);
        Assert.assertEquals(50.0, statSnapshot1.getCapacity().getAvg(), epsilon);

        // Second should be cluster 3 with 9/10 total headroom
        Assert.assertEquals(CLUSTER_ID_3, Long.toString(entityStats.get(1).getOid()));
        Assert.assertEquals(1, entityStats.get(1).getStatSnapshots(0).getStatRecordsCount());
        final StatRecord statSnapshot2 = entityStats.get(1).getStatSnapshots(0).getStatRecords(0);
        Assert.assertEquals(TOTAL_HEADROOM, statSnapshot2.getName());
        Assert.assertEquals(9.0, statSnapshot2.getUsed().getAvg(), epsilon);
        Assert.assertEquals(10.0, statSnapshot2.getCapacity().getAvg(), epsilon);
    }

    private ClusterStatsRequest constructTestInput(
            @Nonnull Collection<Long> scope, @Nonnull String orderByStat,
            boolean ascending, int offset, int limit,
            @Nonnull String... additionalStats) {
        return constructTestInputWithDates(scope, orderByStat, ascending, offset, limit,
            Optional.empty(), Optional.empty(), additionalStats);
    }

    private ClusterStatsRequest constructTestInput(
        @Nonnull Collection<Long> scope, @Nonnull Collection<String> stats,
        @Nonnull Optional<Timestamp> startDate, @Nonnull Optional<Timestamp> endDate, boolean requestProjected) {
        final StatsFilter.Builder statsFilter = StatsFilter.newBuilder();
        stats.forEach(stat -> statsFilter
            .addCommodityRequests(CommodityRequest.newBuilder().setCommodityName(stat)));
        startDate.ifPresent(t -> statsFilter.setStartDate(t.getTime()));
        endDate.ifPresent(t -> statsFilter.setEndDate(t.getTime()));
        statsFilter.setRequestProjectedHeadroom(requestProjected);
        return ClusterStatsRequest.newBuilder()
            .addAllClusterIds(scope)
            .setStats(statsFilter)
            .build();
    }

    private ClusterStatsRequest constructTestInputWithDates(
            @Nonnull Collection<Long> scope, @Nonnull String orderByStat,
            boolean ascending, int offset, int limit,
            @Nonnull Optional<Timestamp> startDate, @Nonnull Optional<Timestamp> endDate,
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
        startDate.ifPresent(t -> statsFilterBuilder.setStartDate(t.getTime()));
        endDate.ifPresent(t -> statsFilterBuilder.setEndDate(t.getTime()));
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
            @Nonnull Optional<Timestamp> endDate, boolean requestProjected) {
        return constructTestInput(Collections.singletonList(Long.parseLong(CLUSTER_ID_3)),
            Collections.singletonList(CPU_HEADROOM), startDate, endDate, requestProjected);
    }
}
