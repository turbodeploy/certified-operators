package com.vmturbo.history.listeners;

import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.listeners.RollupProcessor.RollupType;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsLatest;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Base class for rollup tests.
 */
@RunWith(Parameterized.class)
public abstract class RollupTestBase extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    protected final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public RollupTestBase(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        this.dsl = super.getDslContext();
    }

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
    protected final Random rand = new Random(RANDOM_SEED);
    PartmanHelper partmanHelper = mock(PartmanHelper.class);

    SimpleBulkLoaderFactory loaders;
    BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
            .batchSize(10)
            .maxBatchRetries(1)
            .maxRetryBackoffMsec(1000)
            .maxPendingBatches(1)
            .flushTimeoutSecs(10)
            .build();
    private RollupProcessor rollupProcessor;

    /**
     * Per-test initialization.
     */
    @Before
    public void rolllupTestBaseBefore() {
        loaders = new SimpleBulkLoaderFactory(dsl, config, partmanHelper,
                () -> Executors.newSingleThreadExecutor());
        rollupProcessor = new RollupProcessor(dsl, dsl, partmanHelper,
                () -> Executors.newFixedThreadPool(8));
        IdentityGenerator.initPrefix(1L);
        RetentionPolicy.init(dsl);
    }

    /**
     * Create a template record for a new time series.
     *
     * <p>The record includes values for all the fields that constitute the "identity" of the
     * time series.</p>
     *
     * @param t        "latest" entity stats table
     * @param idValues identity values
     * @param <R>      record type
     * @return the template record
     */
    protected static <R extends Record> R createTemplate(Table<R> t, IdentityValues<R> idValues) {
        R record = t.newRecord();
        // set identity field values
        idValues.set(record);
        return record;
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
    protected <T> void checkField(Record template, Record record, String fieldName, Class<T> type) {
        final T expected = template.getValue(DSL.field(fieldName, type));
        if (expected != null) {
            assertThat(expected, isA(type));
        }
        checkField(expected, record, fieldName, type);
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
    protected <T> void checkField(T expected, Record record, String fieldName, Class<T> type) {
        T actual = record.getValue(DSL.field(fieldName, type));
        if (actual != null) {
            assertThat(actual, isA(type));
        }
        if (type == Double.class && actual != null) {
            assertThat((Double)expected, closeTo((Double)actual, 0.01));
        } else {
            assertEquals(expected, actual);
        }
    }

    /**
     * Class to generate records for a stats time series.
     *
     * <p>All records in the class will be based on a common template record, ensuring that they
     * belong to a single time series.</p>
     *
     * @param <R> type of underlying "latest" stats record type
     */
    protected class StatsTimeSeries<R extends Record> implements Supplier<R> {

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
         */
        StatsTimeSeries(
                Table<R> table, R templateRecord,
                Double baseValue, Instant baseSnapshot, long cycleTimeMsec) {
            this.table = table;
            this.templateRecord = templateRecord;
            this.baseValue = baseValue;
            this.snapshot = baseSnapshot;
            this.cycleTimeMsec = cycleTimeMsec;
            this.isClusterStats = table instanceof ClusterStatsLatest;
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
        protected Aggregator reset(TimeFrame timeFrame) {
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
                r.set(getTimestampField(table, StringConstants.RECORDED_ON),
                        Timestamp.from(snapshot));
                r.set(getDoubleField(table, StringConstants.VALUE), value);
            } else {
                r.set(getTimestampField(table, StringConstants.SNAPSHOT_TIME),
                        Timestamp.from(snapshot));
                // compute a value that is randomly +-5% from base and set it in all value fields
                value = baseValue * (0.95 + rand.nextFloat() / 10);
                r.set(getDoubleField(table, StringConstants.AVG_VALUE), value);
                r.set(getDoubleField(table, StringConstants.MAX_VALUE), value);
                r.set(getDoubleField(table, StringConstants.MIN_VALUE), value);
                // set capacity and effective capacity as fixed muliples of the provided value
                // (we don't really care what they are, just that we're correctly aggregating them
                r.set(getDoubleField(table, StringConstants.CAPACITY), value * 2);
                r.set(getDoubleField(table, StringConstants.EFFECTIVE_CAPACITY), value * 4);
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
     * Class that keeps track of avg, min, and max values, number of samples and value (only for
     * cluster stats).
     *
     * <p>Aggregators independently compute these values so they can be compared to what ends up
     * in rollup records produced by the stored proc.</p>
     */
    protected static class Aggregator {
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

    /**
     * Get the rollup snapshot_time for the given timeFrame and a time within the rollup interval.
     *
     * @param timeFrame         rollup time frame - hourly, daily, or monthly
     * @param instantInInterval a time covered by the desired rollup period
     * @return the snapshot_time for the rollup record that would cover the given time
     */
    protected Timestamp getRollupSnapshot(TimeFrame timeFrame, Instant instantInInterval) {
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
     * Get the rollup table corresponding to the given reference table and the given time frame.
     *
     * @param table     reference table
     * @param timeFrame rollup timeframe
     * @return the rolulp table
     */
    protected Table<?> getRollupTable(Table<?> table, TimeFrame timeFrame) {
        EntityType entityType = EntityType.fromTable(table).orElse(null);
        if (entityType != null) {
            switch (timeFrame) {
                case HOUR:
                    return entityType.getHourTable().orElse(null);
                case DAY:
                    return entityType.getDayTable().orElse(null);
                case MONTH:
                    return entityType.getMonthTable().orElse(null);
                default:
                    throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Create a uuid value, based on a newly generated long.
     *
     * @return the uuid value
     */
    protected String getRandUuid() {
        return Long.toString(IdentityGenerator.next());
    }

    private static double round(double value, int scale) {
        return (double)Math.round(value * scale) / scale;
    }

    /**
     * Container for identity fields for a collection of rollup tables.
     *
     * @param <R> record type for the "latest" table in the collection
     */
    protected interface IdentityValues<R extends Record> {
        /**
         * Set identity vaalues into the given record.
         *
         * @param record record to receive identity values
         */
        void set(R record);

        /**
         * Produce jOOQ {@link Condition} instances to select reocrds that match these identity
         * values from the given table.
         *
         * @param table rollup table in this class's rollup table collection
         * @return list of conditions
         */
        List<Condition> matchConditions(Table<?> table);
    }

    /**
     * Retrieve a rollup record from the database.
     *
     * <p>If zero or multiple records are selected, this operation causes the overall test to
     * fail.</p>
     *
     * @param rollupTable        rollup table containing the desired record
     * @param timestampCondition condition on this record's timestamp field
     * @param identityValues     {@link IdentityValues} instance to produce identity conditions
     * @return the rollup record
     * @throws DataAccessException on db error
     */
    protected Record retrieveRecord(Table<?> rollupTable, Condition timestampCondition,
            IdentityValues<?> identityValues)
            throws DataAccessException {
        return retrieveRecord(rollupTable, timestampCondition, identityValues,
                Collections.emptySet());
    }

    protected Record retrieveRecord(Table<?> rollupTable, Condition timestampCondition,
            IdentityValues<?> identityValues, Set<String> excludedFieldNames)
            throws DataAccessException {
        List<Condition> conditions = new ArrayList<>();
        conditions.add(timestampCondition);
        conditions.addAll(identityValues.matchConditions(rollupTable));
        List<Field<?>> fields = Arrays.stream(rollupTable.fields())
                .filter(f -> !excludedFieldNames.contains(f.getName()))
                .collect(Collectors.toList());
        Result<?> records = dsl.select(fields).from(rollupTable).where(conditions).fetch();
        assertEquals(1, records.size());
        return records.get(0);
    }
}
