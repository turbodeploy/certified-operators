package com.vmturbo.history.listeners;

import static com.vmturbo.commons.TimeFrame.DAY;
import static com.vmturbo.commons.TimeFrame.HOUR;
import static com.vmturbo.commons.TimeFrame.MONTH;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.listeners.RollupProcessor.VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.listeners.RollupProcessor.RollupType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.VolumeAttachmentHistory;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to test the rollup processor and the stored procs it depends on.
 */
@RunWith(Parameterized.class)
public class RollupProcessorTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public RollupProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws SQLException If a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10)
                .maxBatchRetries(1)
                .maxRetryBackoffMsec(1000)
                .maxPendingBatches(1)
                .build();
        loaders = new SimpleBulkLoaderFactory(dsl, config, Executors.newSingleThreadExecutor());
        rollupProcessor = new RollupProcessor(dsl, dsl, Executors.newFixedThreadPool(8));
        RetentionPolicy.init(dsl);
        IdentityGenerator.initPrefix(1L);
    }

    private static final String UUID_FIELD = "uuid";
    private static final String PRODUCER_UUID_FIELD = "producer_uuid";
    private static final String PROPERTY_TYPE_FIELD = "property_type";
    private static final String PROPERTY_SUBTYPE_FIELD = "property_subtype";
    private static final String COMMODITY_KEY_FIELD = "commodity_key";
    private static final String RELATION_FIELD = "relation";
    private static final String CAPACITY_FIELD = "capacity";
    private static final String EFFECTIVE_CAPACITY_FIELD = "effective_capacity";
    private static final String AVG_VALUE_FIELD = "avg_value";
    private static final String MAX_VALUE_FIELD = "max_value";
    private static final String MIN_VALUE_FIELD = "min_value";
    private static final String SNAPSHOT_TIME_FIELD = "snapshot_time";
    private static final String SAMPLES_FIELD = "samples";

    private static final String INTERNAL_NAME_FIELD = "internal_name";
    private static final String RECORDED_ON_FIELD = "recorded_on";
    private static final String VALUE_FIELD = "value";

    /**
     * Seed for random number generator.
     *
     * <p>There's considerable awkwardness in this test due to the fact that the database type of
     * value- and capacity-related columns in the stats tables is DECIMAL(15,3), but we have a jOOQ
     * conversion to Double configured for the component. This makes it difficult to compare values
     * of internally computed averages to those computed in the database. We use approximate
     * equality tests to help address this, but even then, the test with different values could
     * suffer false failures. While there's value in creating a stream of values with internal
     * randomness (mostly in terms of conciseness of test specification), there's no value in having
     * different random values from one execution to the next. So we fix the seed to a value that is
     * known to work. If a change to this test class causes some tests to fail for no other likely
     * reason, one thing to try is changing this seed value to find one that works with the other
     * code changes.</p>
     */
    private static final long RANDOM_SEED = 0L;

    private static String testDbName;
    private SimpleBulkLoaderFactory loaders;
    private RollupProcessor rollupProcessor;
    private final Random rand = new Random(RANDOM_SEED);

    /**
     * Delete any records inserted during this test.
     *
     * <p>We truncate all the tables identified as ouptut tables in the bulk loader stats object,
     * as well as all associated rollup tables.</p>
     *
     * <p>If any other tables are populated by a given test, that test should clean them up.</p>
     *
     * @throws InterruptedException if interrupted
     */
    @After
    public void after() throws InterruptedException {
        loaders.close(null);
    }

    /**
     * Perform a test of rollups by inserting records into a time series over a span of 26 hours
     * crossing a month (and therefore day) boundary in the process, and checking that all rollup
     * tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     * @throws DataAccessException       on db error
     */
    @Test
    @CleanupOverrides(truncate = {PmStatsByHour.class, PmStatsByDay.class, PmStatsByMonth.class})
    public void testRollups() throws InterruptedException, DataAccessException {
        PmStatsLatestRecord template1 =
                createTemplateForStatsTimeSeries(Tables.PM_STATS_LATEST, "CPU",
                        PropertySubType.Used.getApiParameterName(), null);
        StatsTimeSeries<PmStatsLatestRecord> ts1 = new StatsTimeSeries<>(
                Tables.PM_STATS_LATEST, template1, 100_000.0,
                Instant.parse("2019-01-31T22:01:35Z"), TimeUnit.MINUTES.toMillis(10), false);
        BulkLoader<PmStatsLatestRecord> loader = loaders.getLoader(Tables.PM_STATS_LATEST);
        // run through the 22:00 hour
        ts1.cycle(6, loader);
        final Aggregator hourly10PM = ts1.reset(HOUR);
        // run through the 23:00 hour, which also closes out Jan 31 and the month of January
        ts1.cycle(6, loader);
        final Aggregator hourly11PM = ts1.reset(HOUR);
        final Aggregator dailyJan31 = ts1.reset(DAY);
        final Aggregator monthlyJan = ts1.reset(MONTH);
        // run through first two hours of Feb 1, skipping a few cycles and grabbing
        // hourly aggregators
        ts1.cycle(3, loader, false);
        ts1.skipCycle();
        ts1.cycle(2, loader);
        final Aggregator hourly12AM = ts1.reset(HOUR);
        ts1.cycle(1, loader, false);
        ts1.skipCycle();
        ts1.skipCycle();
        ts1.cycle(3, loader);
        final Aggregator hourly1AM = ts1.reset(HOUR);
        // now run through the next 22 hours, skipping most cycles cuz they slow the test with
        // little or now benefit
        for (int i = 0; i < 22; i++) {
            ts1.cycle(1, loader);
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
        }
        // and finally do daily and monthly rollups for Feb 1
        final Aggregator dailyFeb1 = ts1.reset(DAY);
        final Aggregator monthlyFeb = ts1.reset(MONTH);
        // now check the rollup data for the aggregators we grabbed
        checkRollups(HOUR, template1, hourly10PM, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(HOUR, template1, hourly11PM, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(HOUR, template1, hourly12AM, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(HOUR, template1, hourly1AM, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(DAY, template1, dailyJan31, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(DAY, template1, dailyFeb1, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(MONTH, template1, monthlyJan, Tables.PM_STATS_LATEST, null, null, null);
        checkRollups(MONTH, template1, monthlyFeb, Tables.PM_STATS_LATEST, null, null, null);
    }

    /**
     * Perform a test of rollups by inserting records into a time series over a span of 26 hours
     * crossing a month (and therefore day) boundary in the process, and checking that all rollup
     * tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     * @throws DataAccessException       on db error
     */
    @Test
    @CleanupOverrides(
            truncate = {ClusterStatsByHour.class, ClusterStatsByDay.class, ClusterStatsByMonth.class})
    public void testClusterRollup() throws InterruptedException {
        ClusterStatsLatestRecord template1 =
            createTemplateForClusterStatsTimeSeries(Tables.CLUSTER_STATS_LATEST, "CPU",
                PropertySubType.Used.getApiParameterName());
        StatsTimeSeries<ClusterStatsLatestRecord> ts1 = new StatsTimeSeries<>(
            Tables.CLUSTER_STATS_LATEST, template1, 100_000.0,
            Instant.parse("2019-01-31T22:01:35Z"), TimeUnit.MINUTES.toMillis(10), true);
        BulkLoader<ClusterStatsLatestRecord> loader = loaders.getLoader(Tables.CLUSTER_STATS_LATEST);
        // run through the 22:00 hour
        ts1.cycle(6, loader);
        final Aggregator hourly10PM = ts1.reset(HOUR);
        // run through the 23:00 hour, which also closes out Jan 31 and the month of January
        ts1.cycle(6, loader);
        final Aggregator hourly11PM = ts1.reset(HOUR);
        final Aggregator dailyJan31 = ts1.reset(DAY);
        final Aggregator monthlyJan = ts1.reset(MONTH);
        // run through first two hours of Feb 1, skipping a few cycles and grabbing
        // hourly aggregators
        ts1.cycle(3, loader, false);
        ts1.skipCycle();
        ts1.cycle(2, loader);
        final Aggregator hourly12AM = ts1.reset(HOUR);
        ts1.cycle(1, loader, false);
        ts1.skipCycle();
        ts1.skipCycle();
        ts1.cycle(3, loader);
        final Aggregator hourly1AM = ts1.reset(HOUR);
        // now run through the next 22 hours, skipping most cycles cuz they slow test with little
        // if any benefit
        for (int i = 0; i < 22; i++) {
            ts1.cycle(1, loader);
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
            ts1.skipCycle();
        }
        // and finally do daily and monthly rollups for Feb 1
        final Aggregator dailyFeb1 = ts1.reset(DAY);
        final Aggregator monthlyFeb = ts1.reset(MONTH);
        // now check the rollup data for the aggregators we grabbed
        checkClusterRollup(HOUR, template1, hourly10PM, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(HOUR, template1, hourly11PM, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(HOUR, template1, hourly12AM, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(HOUR, template1, hourly1AM, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(DAY, template1, dailyJan31, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(DAY, template1, dailyFeb1, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(MONTH, template1, monthlyJan, Tables.CLUSTER_STATS_LATEST, null, null, null);
        checkClusterRollup(MONTH, template1, monthlyFeb, Tables.CLUSTER_STATS_LATEST, null, null, null);
    }

    private static final long VOLUME_OID = 11111L;
    private static final long VOLUME_OID_2 = 22222L;
    private static final long VM_OID = 33333L;
    private static final long VM_OID_2 = 44444L;

    /**
     * Test that retention processing removes the record related to the only volume from the
     * volume_attachment_history table that is older than retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsRemoval() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long outsideRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, outsideRetentionPeriod,
                outsideRetentionPeriod);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> records =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        Assert.assertFalse(records.isEmpty());

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        Assert.assertTrue(recordsAfterPurge.isEmpty());
    }

    /**
     * Test that retention processing does not remove any records related to the volume from the
     * volume_attachment_history table as it has one entry discovered within the retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsNoRemovals() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long withinRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD - 1);
        final long outsideRetentionPeriod =
                currentTime - TimeUnit.DAYS.toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, VM_OID, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, withinRetentionPeriod,
                withinRetentionPeriod);

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_OID));
        final Record3<Long, Long, Date> record = recordsAfterPurge.iterator().next();
        Assert.assertEquals(VOLUME_OID, (long)record.component1());
        Assert.assertEquals(VM_OID, (long)record.component2());
    }

    /**
     * Test that retention processing removes records related to one volume but retains records
     * related to another volume as the former has no entries within the retention period while the
     * latter has one entry within the last retention period.
     *
     * @throws DataAccessException if error encountered during insertion.
     */
    @Test
    public void testPurgeVolumeAttachmentHistoryRecordsOneRemoval() throws DataAccessException {
        final long currentTime = System.currentTimeMillis();
        final long withinRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD - 1);
        final long outsideRetentionPeriod = currentTime - TimeUnit.DAYS
                .toMillis(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD + 1);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, VM_OID, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID, 0L, withinRetentionPeriod,
                withinRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID_2, VM_OID_2, outsideRetentionPeriod,
                outsideRetentionPeriod);
        insertIntoVolumeAttachmentHistoryTable(VOLUME_OID_2, 0, outsideRetentionPeriod,
                outsideRetentionPeriod);

        final Logger logger = LogManager.getLogger();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        rollupProcessor.performRetentionProcessing(timer, false);

        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(dsl);
        final List<Record3<Long, Long, Date>> recordsAfterPurge =
                reader.getVolumeAttachmentHistory(Stream.of(VOLUME_OID, VOLUME_OID_2)
                        .collect(Collectors.toList()));
        final Record3<Long, Long, Date> record = recordsAfterPurge.iterator().next();
        Assert.assertEquals(VOLUME_OID, (long)record.component1());
        Assert.assertEquals(VM_OID, (long)record.component2());
    }

    private void insertIntoVolumeAttachmentHistoryTable(final long volumeOid, final long vmOid,
            final long lastAttachedTime,
            final long lastDiscoveredTime)
            throws DataAccessException {
        dsl.insertInto(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.VOLUME_OID,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.VM_OID,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.LAST_ATTACHED_DATE,
                        VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY.LAST_DISCOVERED_DATE)
                .values(volumeOid, vmOid, new Date(lastAttachedTime),
                        new Date(lastDiscoveredTime))
                .execute();
    }

    /**
     * Check that a rollup record's fields are all as expected.
     *
     * <p>Some fields are checked against the same fields in a "template" record for the time
     * series. Others area checked against values from an aggregator that covers all the records
     * that should contribute to this particular rollup record.</p>
     *
     * @param timeFrame       hourly, daily, or monthly
     * @param template        template record for this time series
     * @param aggregator      aggregator for this time series for the period of this rollup record
     * @param table           underlying stats "latest" table
     * @param uuid            uuid for time-series, or null to not include uuid condition
     * @param propertyType    property type for time-series, or null to omit property type
     *                        condition
     * @param propertySubtype property subtype for time-series, or null to omit subtype condition
     * @param <R>             underlying record type
     * @throws DataAccessException on db error
     * @throws DataAccessException   on db error
     */
    private <R extends Record> void checkRollups(
            TimeFrame timeFrame, R template, Aggregator aggregator, Table<R> table,
            @Nullable String uuid, @Nullable String propertyType, @Nullable String propertySubtype)
            throws DataAccessException {
        Timestamp snapshot = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(EntityType.fromTable(table).get(), timeFrame);
        Record rollup = retrieveRecord(rollupTable, snapshot, uuid, propertyType, propertySubtype, false);
        checkField(template, rollup, UUID_FIELD, String.class);
        checkField(template, rollup, PRODUCER_UUID_FIELD, String.class);
        checkField(template, rollup, PROPERTY_TYPE_FIELD, String.class);
        checkField(template, rollup, PROPERTY_SUBTYPE_FIELD, String.class);
        checkField(template, rollup, RELATION_FIELD, RelationType.class);
        checkField(template, rollup, COMMODITY_KEY_FIELD, String.class);
        checkField(aggregator.getAvg(), rollup, AVG_VALUE_FIELD, Double.class);
        checkField(aggregator.getMin(), rollup, MIN_VALUE_FIELD, Double.class);
        checkField(aggregator.getMax(), rollup, MAX_VALUE_FIELD, Double.class);
        checkField(aggregator.getMaxObservedCapacity(), rollup, CAPACITY_FIELD, Double.class);
        checkField(aggregator.getMaxObservedEffectiveCapacity(), rollup, EFFECTIVE_CAPACITY_FIELD, Double.class);
        checkField(aggregator.getSamples(), rollup, SAMPLES_FIELD, Integer.class);
    }

    private <R extends Record> void checkClusterRollup(
            TimeFrame timeFrame, R template, Aggregator aggregator, Table<R> table,
            @Nullable String uuid, @Nullable String propertyType, @Nullable String propertySubtype)
            throws DataAccessException {
        Timestamp snapshot = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(EntityType.fromTable(table).get(), timeFrame);
        Record rollup = retrieveRecord(rollupTable, snapshot, uuid, propertyType, propertySubtype, true);
        checkField(template, rollup, INTERNAL_NAME_FIELD, String.class);
        checkField(template, rollup, PROPERTY_TYPE_FIELD, String.class);
        checkField(template, rollup, PROPERTY_SUBTYPE_FIELD, String.class);
        checkField(aggregator.getAvg(), rollup, VALUE_FIELD, Double.class);
    }

    /**
     * Check whether a rollup field value matches the value of the same field in the template
     * record.
     *
     * @param template  template record
     * @param record    rollup record
     * @param fieldName name of field to check
     * @param type      field value class
     * @param <T>       field value type
     */
    private <T> void checkField(Record template, Record record, String fieldName, Class<T> type) {
        final Object expected = template.getValue(fieldName);
        if (expected != null) {
            assertThat((T)expected, isA(type));
        }
        checkField((T)expected, record, fieldName, type);
    }

    /**
     * Check whether a rollup field value matches a given value.
     *
     * @param expected  expected field value
     * @param record    rollup record
     * @param fieldName name of field to check
     * @param type      field value class
     * @param <T>       field value type
     */
    private <T> void checkField(T expected, Record record, String fieldName, Class<T> type) {
        Object actual = record.getValue(fieldName);
        if (actual != null) {
            assertThat((T)actual, isA(type));
        }
        if (type == Double.class) {
            assertThat((Double)expected, closeTo((Double)actual, 0.01));
        } else {
            assertEquals(expected, actual);
        }
    }

    /**
     * Retrieve a rollup record from the database.
     *
     * <p>If multiple records are selected, this operation causes the overall test to fail.</p>
     *
     * @param rollupTable     rollup table containing the desired record
     * @param snapshot        snapshot_time value of the desired record
     * @param uuid            uuid, or null to not include a uuid condition
     * @param propertyType    property type, or null to not include a property type condition
     * @param propertySubtype property subtype, or null to not include a property subtype condition
     * @param isClusterStats  whether it is cluster stats or not
     * @return the rollup record
     * @throws DataAccessException   on db error
     */
    private Record retrieveRecord(
            Table<?> rollupTable, Timestamp snapshot,
            @Nullable String uuid, @Nullable String propertyType, @Nullable String propertySubtype,
            boolean isClusterStats)
            throws DataAccessException {
        List<Condition> conditions = new ArrayList<>();
        conditions.add(getTimestampField(rollupTable,
            isClusterStats ? RECORDED_ON_FIELD : SNAPSHOT_TIME_FIELD).eq(snapshot));
        if (uuid != null) {
            conditions.add(getStringField(rollupTable,
                    isClusterStats ? INTERNAL_NAME_FIELD : UUID_FIELD).eq(uuid));
        }
        if (propertyType != null) {
            conditions.add(getStringField(rollupTable, PROPERTY_TYPE_FIELD).eq(propertyType));
        }
        if (propertySubtype != null) {
            conditions.add(getStringField(rollupTable, PROPERTY_SUBTYPE_FIELD).eq(propertySubtype));
        }
        Result<?> records = dsl.selectFrom(rollupTable)
                .where(conditions)
                .fetch();
            assertEquals(1, records.size());
            return records.get(0);
    }

    /**
     * Get the rollup snapshot_time for the given timeFrame and a time within the rollup interval.
     *
     * @param timeFrame         rollup time frame - hourly, daily, or monthly
     * @param instantInInterval a time covered by the desired rollup period
     * @return the snapshot_time for the rollup record that would cover the given time
     */
    private Timestamp getRollupSnapshot(TimeFrame timeFrame, Instant instantInInterval) {
        switch (timeFrame) {
            case HOUR:
                return RollupType.BY_HOUR.getRollupTime(Timestamp.from(instantInInterval));
            case DAY:
                return RollupType.BY_DAY.getRollupTime(Timestamp.from(instantInInterval));
            case MONTH:
                return RollupType.BY_MONTH.getRollupTime(Timestamp.from(instantInInterval));
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Get the rollup table for the given entity type and the given time frame.
     *
     * @param entityType the {@link EntityType} for the desired table
     * @param timeFrame  rollup timeframe
     * @return the rolulp table
     */
    private Table<?> getRollupTable(EntityType entityType, TimeFrame timeFrame) {
        switch (timeFrame) {
            case HOUR:
                return entityType.getHourTable().get();
            case DAY:
                return entityType.getDayTable().get();
            case MONTH:
                return entityType.getMonthTable().get();
            default:
                throw new IllegalArgumentException();
        }
    }


    /**
     * Create a template record for a new time series.
     *
     * <p>The record includes values for all the fields that constitute the "identity" of the
     * time series.</p>
     *
     * @param t               "latest" entity stats table
     * @param propertyType    property type for the time series
     * @param propertySubtype property subtype for the time series
     * @param commodityKey    commodity key for the time series
     * @param <R>             record type
     * @return the template record
     */
    private <R extends Record> R createTemplateForStatsTimeSeries(
            Table<R> t, String propertyType, String propertySubtype, String commodityKey) {
        R record = t.newRecord();
        // generate a unique uuid for this time series and, either a random producer id or null
        record.setValue(getStringField(t, UUID_FIELD), getRandUuid());
        String producer = rand.nextBoolean() ? getRandUuid() : null;
        // set identity field values
        record.setValue(getStringField(t, PRODUCER_UUID_FIELD), producer);
        record.setValue(getStringField(t, PROPERTY_TYPE_FIELD), propertyType);
        record.setValue(getStringField(t, PROPERTY_SUBTYPE_FIELD), propertySubtype);
        record.setValue(getStringField(t, COMMODITY_KEY_FIELD), commodityKey);
        // set relation field depending on whether we ended up with a producer
        record.setValue(getRelationTypeField(t, RELATION_FIELD),
                producer == null ? RelationType.COMMODITIESBOUGHT : RelationType.COMMODITIES);
        return record;
    }

    /**
     * Create a template record for a new time series.
     *
     * <p>The record includes values for all the fields that constitute the "identity" of the
     * time series.</p>
     *
     * @param t               "latest" entity stats table
     * @param propertyType    property type for the time series
     * @param propertySubtype property subtype for the time series
     * @param <R>             record type
     * @return the template record
     */
    private <R extends Record> R createTemplateForClusterStatsTimeSeries(
        Table<R> t, String propertyType, String propertySubtype) {
        R record = t.newRecord();
        // generate a unique uuid for this time series and, either a random producer id or null
        record.setValue(getStringField(t, INTERNAL_NAME_FIELD), getRandUuid());
        // set identity field values
        record.setValue(getStringField(t, PROPERTY_TYPE_FIELD), propertyType);
        record.setValue(getStringField(t, PROPERTY_SUBTYPE_FIELD), propertySubtype);
        return record;
    }

    /**
     * Create a uuid value, based on a newly generated long.
     *
     * @return the uuid value
     */
    private String getRandUuid() {
        return Long.toString(IdentityGenerator.next());
    }

    /**
     * Class to generate records for a stats time series.
     *
     * <p>All records in the class will be based on a common template record, ensuring that they
     * belong to a single time series.</p>
     *
     * @param <R> type of underlying "latest" stats record type
     */
    private class StatsTimeSeries<R extends Record> implements Supplier<R> {

        final Table<R> table;
        final Double baseValue;
        Instant snapshot;
        final long cycleTimeMsec;
        final R templateRecord;
        Map<TimeFrame, Aggregator> aggregators = new HashMap<>();
        boolean isClusterStats;

        /**
         * Create a new instance.
         *
         * @param table          "latest" stats table
         * @param templateRecord record providing ts identity values
         * @param baseValue      base value for time series; actual values will be within 5% of
         *                       this
         * @param baseSnapshot   snapshot_time of first generated record
         * @param cycleTimeMsec  duration in millis between snapshot_times in consecutive records
         * @param isClusterStats whether it is cluster stats or not
         */
        StatsTimeSeries(
                Table<R> table, R templateRecord,
                Double baseValue, Instant baseSnapshot, long cycleTimeMsec,
                boolean isClusterStats) {
            this.table = table;
            this.templateRecord = templateRecord;
            this.baseValue = baseValue;
            this.snapshot = baseSnapshot;
            this.cycleTimeMsec = cycleTimeMsec;
            this.isClusterStats = isClusterStats;
            initAggregators();
        }

        /**
         * Create hourly, daily, and monthly aggregators.
         *
         * <p>It is up to the caller to close out these aggregators, by calling
         * {@link #reset(TimeFrame)}, after the last record has been generated in any given
         * timeframe interval.</p>
         */
        private void initAggregators() {
            for (TimeFrame timeFrame : TimeFrame.values()) {
                switch (timeFrame) {
                    case HOUR:
                    case DAY:
                    case MONTH:
                        aggregators.put(timeFrame, new Aggregator());
                        break;
                    default:
                        // not interested in other timeframes
                        break;
                }
            }
        }

        /**
         * Close out an aggregator and return it, replacing it with a new aggregator to handle any
         * records still to be generated.
         *
         * @param timeFrame hourly, daily, or monthly aggregator to reset
         * @return the closed-out aggregator, no longer active
         */
        private Aggregator reset(TimeFrame timeFrame) {
            Aggregator result = aggregators.get(timeFrame);
            aggregators.put(timeFrame, new Aggregator());
            return result;
        }

        /**
         * Generate a new record for the time series, and update all current aggregators.
         *
         * <p>Identity values are copied from the template record. Value-based fields are set to a
         * randomly created value within 5% of the base value. Snapshot time starts with the base
         * snapshot and isadvance by cycle duration for each subsequent record (or when a cycle is
         * skipped).</p>
         *
         * @return newly generated records
         */
        @Override
        public R get() {
            // clone the template
            R r = templateRecord.into(table);
            Double value;
            if (isClusterStats) {
                value = baseValue;
                r.set(getTimestampField(table, RECORDED_ON_FIELD), Timestamp.from(snapshot));
                r.set(getDoubleField(table, VALUE_FIELD), value);
            } else {
                r.set(getTimestampField(table, SNAPSHOT_TIME_FIELD), Timestamp.from(snapshot));
                // compute a value that is randomly +-5% from base and set it in all value fields
                value = baseValue * (0.95 + rand.nextFloat() / 10);
                r.set(getDoubleField(table, AVG_VALUE_FIELD), value);
                r.set(getDoubleField(table, MAX_VALUE_FIELD), value);
                r.set(getDoubleField(table, MIN_VALUE_FIELD), value);
                // set capacity and effective capacity as fixed muliples of the provided value
                // (we don't really care what they are, just that we're correctly aggregating them
                r.set(getDoubleField(table, CAPACITY_FIELD), value * 2);
                r.set(getDoubleField(table, EFFECTIVE_CAPACITY_FIELD), value * 4);
            }

            // update all active aggregators
            aggregators.values().forEach(agg -> agg.observe(value, snapshot));
            // set up for next cycle
            advanceSnapshot();
            return r;
        }

        /**
         * Add the cycle time to the current snapshot time.
         *
         * <p>The value will be used in the next generated record, unless that one is skipped.</p>
         */
        private void advanceSnapshot() {
            snapshot = snapshot.plus(cycleTimeMsec, ChronoUnit.MILLIS);
        }

        /**
         * Generate records for a given number of cycles.
         *
         * <p>An hourly rollup is performed after each record is generated.</p>
         *
         * @param n      number of records to generate
         * @param loader bulk loader for inserting generated records
         * @throws InterruptedException if interrupted
         */
        void cycle(int n, BulkLoader<R> loader) throws InterruptedException {
            cycle(n, loader, true);
        }

        /**
         * Generate records for a given number of cycles, and perform daily/monthly rollups after
         * the last one.
         *
         * @param n          number of records to generate
         * @param loader     bulk loader for inserting generated records
         * @param lastInHour true if the last cycle closes out an hour, so daily/monthly rollups
         *                   should happen
         * @throws InterruptedException if interrupted
         */
        void cycle(int n, BulkLoader<R> loader, boolean lastInHour) throws InterruptedException {
            for (int i = 0; i < n; i++) {
                loader.insert(get());
                loaders.flushAll();
                rollupProcessor.performHourRollups(singletonList(table),
                        aggregators.get(HOUR).getLatestSnapshot());
            }
            if (lastInHour) {
                rollupProcessor.performDayMonthRollups(singletonList(table),
                        aggregators.get(HOUR).getLatestSnapshot(), false);
            }
        }

        /**
         * Skip a cycle; advance the snapshot time without generating a record.
         */
        void skipCycle() {
            advanceSnapshot();
        }
    }

    /**
     * Class that keeps track of avg, min, and max values, number of samples and value (only for cluster stats).
     *
     * <p>Aggregators independently compute these values so they can be compared to what ends up
     * in rollup records produced by the stored proc.</p>
     */
    private static class Aggregator {
        private Double value = 0.0;
        private int samples = 0;
        private Double avg = 0.0;
        private Double min = Double.MAX_VALUE;
        private Double max = Double.MIN_VALUE;
        private Instant latestSnapshot;
        private Double maxObservedCapacity = Double.MIN_VALUE;
        private Double maxObservedEffectiveCapacity = Double.MIN_VALUE;

        /**
         * Incorporate a new sample.
         *
         * @param value    observed value
         * @param snapshot snapshot time where this value appeared
         */
        void observe(Double value, Instant snapshot) {
            this.value = value;
            this.avg = round((avg * samples + value) / (++samples), 1000);
            this.max = Math.max(max, value);
            this.min = Math.min(min, value);
            this.maxObservedCapacity = Math.max(maxObservedCapacity, value * 2);
            this.maxObservedEffectiveCapacity = Math.max(maxObservedEffectiveCapacity, value * 4);
            this.latestSnapshot = snapshot;
        }

        /**
         * Get the value for cluster stats.
         *
         * @return value
         */
        Double getValue() {
            return value;
        }

        /**
         * Get the number of samples added to this aggregator.
         *
         * @return sample count
         */
        int getSamples() {
            return samples;
        }

        /**
         * Get the average of all samples.
         *
         * @return average
         */
        Double getAvg() {
            return avg;
        }

        /**
         * Get the minimum of all samples.
         *
         * @return min value
         */
        Double getMin() {
            return min;
        }

        /**
         * Return the maximum of all samples.
         *
         * @return max value
         */
        Double getMax() {
            return max;
        }

        /**
         * Get the maximum of all observed capacities.
         *
         * @return max capacity
         */
        Double getMaxObservedCapacity() {
            return maxObservedCapacity;
        }

        /**
         * Get the maximum of all observed effective capacities.
         *
         * @return max effective capacity
         */
        Double getMaxObservedEffectiveCapacity() {
            return maxObservedEffectiveCapacity;
        }

        /**
         * Return the latest snapshot time of a sample added to this aggregator.
         *
         * @return latest snapshot time
         */
        Instant getLatestSnapshot() {
            return latestSnapshot;
        }
    }

    private static double round(double value, int scale) {
        return (double)Math.round(value * scale) / scale;
    }
}
