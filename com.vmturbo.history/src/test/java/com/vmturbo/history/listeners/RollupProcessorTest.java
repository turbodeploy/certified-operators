package com.vmturbo.history.listeners;

import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.listeners.RollupProcessor.RollupType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.DbTestConfig;

/**
 * Class to test the rollup processor and the stored procs it depends on.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class RollupProcessorTest {

    private static final String UUID_FIELD = "uuid";
    private static final String PRODUCER_UUID_FIELD = "producer_uuid";
    private static final String PROPERTY_TYPE_FIELD = "property_type";
    private static final String PROPERTY_SUBTYPE_FIELD = "property_subtype";
    private static final String COMMODITY_KEY_FIELD = "commodity_key";
    private static final String RELATION_FIELD = "relation";
    private static final String CAPACITY_FIELD = "capacity";
    private static final String AVG_VALUE_FIELD = "avg_value";
    private static final String MAX_VALUE_FIELD = "max_value";
    private static final String MIN_VALUE_FIELD = "min_value";
    private static final String SNAPSHOT_TIME_FIELD = "snapshot_time";
    private static final String SAMPLES_FIELD = "samples";

    @Autowired
    private DbTestConfig dbTestConfig;

    private static HistorydbIO historydbIO;
    private static String testDbName;
    private SimpleBulkLoaderFactory loaders;
    private Random rand = new Random();
    private RollupProcessor rollupProcessor;

    /**
     * Create a history database to be used by all tests.
     *
     * @throws VmtDbException if an error occurs during migrations
     */
    @Before
    public void before() throws VmtDbException {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        historydbIO.init(false, null, testDbName, Optional.empty());
        BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10)
                .maxBatchRetries(1)
                .maxRetryBackoffMsec(1000)
                .maxPendingBatches(1)
                .build();
        loaders = new SimpleBulkLoaderFactory(
                historydbIO, config, Executors.newSingleThreadExecutor());
        rollupProcessor = new RollupProcessor(historydbIO, Executors.newSingleThreadExecutor());
        IdentityGenerator.initPrefix(1L);
    }

    /**
     * Delete any records inserted during this test.
     *
     * <p>We truncate all the tables identified as ouptut tables in the bulk loader stats object,
     * as well as all associated rollup tables.</p>
     *
     * <p>If any other tables are populated by a given test, that test should clean them up.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       on db error
     * @throws SQLException         on db error
     */
    @After
    public void after() throws InterruptedException, VmtDbException, SQLException {
        loaders.close(null);
        for (Table<?> t : loaders.getStats().getOutTables()) {
            truncateTable(t);
            EntityType type = EntityType.fromTable(t);
            if (type != null) {
                truncateTable(type.getHourTable());
                truncateTable(type.getDayTable());
                truncateTable(type.getMonthTable());
            }
        }
    }

    /**
     * Remove all records from the given table.
     *
     * @param table table to be truncated
     * @throws VmtDbException if a db error occurs
     * @throws SQLException   if a db error occurs
     */
    private void truncateTable(Table<?> table) throws VmtDbException, SQLException {
        try (Connection conn = historydbIO.connection()) {
            historydbIO.using(conn).truncate(table).execute();
        }
    }

    /**
     * Discard the test database.
     *
     * @throws VmtDbException if an error occurs
     */
    @AfterClass
    public static void afterClass() throws VmtDbException {
        historydbIO.execute("DROP DATABASE " + testDbName);
    }

    /**
     * Perform a test of rollups by inserting records into a time series over a span of 26 hours
     * crossing a month (and therefore day) boundary in the process, and checking that all rollup
     * tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       on db error
     * @throws SQLException         on db error
     */
    @Test
    public void testRollups() throws InterruptedException, VmtDbException, SQLException {
        PmStatsLatestRecord template1 =
                createTemplateForStatsTimeSeries(Tables.PM_STATS_LATEST, "CPU", "used", null);
        StatsTimeSeries<PmStatsLatestRecord> ts1 = new StatsTimeSeries<>(
                Tables.PM_STATS_LATEST, template1, 100_000.0, 50_000.0,
                Instant.parse("2019-01-31T22:01:35Z"), TimeUnit.MINUTES.toMillis(10));
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
        // now run through the next 22 hours
        for (int i = 0; i < 22; i++) {
            ts1.cycle(6, loader);
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
     * @param propertyType    property type for time-series, or null to omit property type condition
     * @param propertySubtype property subtype for time-series, or null to omit subtype condition
     * @param <R>             underlying record type
     * @throws VmtDbException on db error
     * @throws SQLException   on db error
     */
    private <R extends Record> void checkRollups(
            TimeFrame timeFrame, R template, Aggregator aggregator, Table<R> table,
            @Nullable String uuid, @Nullable String propertyType, @Nullable String propertySubtype)
            throws VmtDbException, SQLException {
        Timestamp snapshot = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(EntityType.fromTable(table), timeFrame);
        Record rollup = retrieveRecord(rollupTable, snapshot, uuid, propertyType, propertySubtype);
        checkField(template, rollup, UUID_FIELD, String.class);
        checkField(template, rollup, PRODUCER_UUID_FIELD, String.class);
        checkField(template, rollup, PROPERTY_TYPE_FIELD, String.class);
        checkField(template, rollup, PROPERTY_SUBTYPE_FIELD, String.class);
        checkField(template, rollup, RELATION_FIELD, RelationType.class);
        checkField(template, rollup, COMMODITY_KEY_FIELD, String.class);
        checkField(aggregator.getAvg(), rollup, AVG_VALUE_FIELD, Double.class);
        checkField(aggregator.getMin(), rollup, MIN_VALUE_FIELD, Double.class);
        checkField(aggregator.getMax(), rollup, MAX_VALUE_FIELD, Double.class);
        checkField(aggregator.getSamples(), rollup, SAMPLES_FIELD, Integer.class);
    }

    /**
     * Check whether a rollup field value matches the value of the same field in the template record.
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
        assertEquals(expected, actual);
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
     * @return the rollup record
     * @throws VmtDbException on db error
     * @throws SQLException   on db error
     */
    private Record retrieveRecord(
            Table<?> rollupTable, Timestamp snapshot,
            @Nullable String uuid, @Nullable String propertyType, @Nullable String propertySubtype)
            throws VmtDbException, SQLException {
        List<Condition> conditions = new ArrayList<>();
        conditions.add(getTimestampField(rollupTable, SNAPSHOT_TIME_FIELD).eq(snapshot));
        if (uuid != null) {
            conditions.add(getStringField(rollupTable, UUID_FIELD).eq(uuid));
        }
        if (propertyType != null) {
            conditions.add(getStringField(rollupTable, PROPERTY_TYPE_FIELD).eq(propertyType));
        }
        if (propertySubtype != null) {
            conditions.add(getStringField(rollupTable, PROPERTY_SUBTYPE_FIELD).eq(propertySubtype));
        }
        try (Connection conn = historydbIO.connection()) {
            Result<?> records = historydbIO.using(conn)
                    .selectFrom(rollupTable)
                    .where(conditions)
                    .fetch();
            assertEquals(1, records.size());
            return records.get(0);
        }
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
                return entityType.getHourTable();
            case DAY:
                return entityType.getDayTable();
            case MONTH:
                return entityType.getMonthTable();
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

        private final Table<R> table;
        private final Double capacity;
        private final Double baseValue;
        private Instant snapshot;
        private final long cycleTimeMsec;
        private final R templateRecord;
        Map<TimeFrame, Aggregator> aggregators = new HashMap<>();

        /**
         * Create a new instance.
         *
         * @param table          "latest" stats table
         * @param templateRecord record providing ts identity values
         * @param capacity       capacity appearing throughout time series
         * @param baseValue      base value for time series; actual values will be within 5% of this
         * @param baseSnapshot   snapshot_time of first generated record
         * @param cycleTimeMsec  duration in millis between snapshot_times in consecutive records
         */
        StatsTimeSeries(
                Table<R> table, R templateRecord,
                Double capacity, Double baseValue, Instant baseSnapshot, long cycleTimeMsec) {
            this.table = table;
            this.templateRecord = templateRecord;
            this.capacity = capacity;
            this.baseValue = baseValue;
            this.snapshot = baseSnapshot;
            this.cycleTimeMsec = cycleTimeMsec;
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
            r.set(getTimestampField(table, SNAPSHOT_TIME_FIELD), Timestamp.from(snapshot));
            r.setValue(getDoubleField(table, CAPACITY_FIELD), capacity);
            // compute a value that is randomly +-5% from base and set it in all value fields
            double value = baseValue * (0.95 + rand.nextInt(10_000) / 100_000);
            r.set(getDoubleField(table, AVG_VALUE_FIELD), value);
            r.set(getDoubleField(table, MAX_VALUE_FIELD), value);
            r.set(getDoubleField(table, MIN_VALUE_FIELD), value);
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
                rollupProcessor.performHourRollups(singletonList(Tables.PM_STATS_LATEST),
                        aggregators.get(HOUR).getLatestSnapshot());
            }
            if (lastInHour) {
                rollupProcessor.performDayMonthRollups(singletonList(Tables.PM_STATS_LATEST),
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
     * Class that keeps track of avg, min, and max values, as well as number of samples.
     *
     * <p>Aggregators independently compute these values so they can be compared to what ends up
     * in rollup records produced by the stored proc.</p>
     */
    private static class Aggregator {
        private int samples = 0;
        private double avg = 0.0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private Instant latestSnapshot;

        /**
         * Incorporate a new sample.
         *
         * @param value    observed value
         * @param snapshot snapshot time where this value appeared
         */
        void observe(Double value, Instant snapshot) {
            this.avg = (avg * samples + value) / (++samples);
            this.max = Math.max(max, value);
            this.min = Math.min(min, value);
            this.latestSnapshot = snapshot;
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
        double getAvg() {
            return avg;
        }

        /**
         * Get the minimum of all samples.
         *
         * @return min value
         */
        double getMin() {
            return min;
        }

        /**
         * Return the maximum of all samples.
         *
         * @return max value
         */
        double getMax() {
            return max;
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
}
