package com.vmturbo.history.listeners;

import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.PmStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Test entity stats rollups.
 */
@RunWith(Parameterized.class)
public class EntityStatsRollupTest extends RollupTestBase {

    private static final Field<String> uuidField = PM_STATS_LATEST.UUID;
    private static final Field<String> producerUuidField = PM_STATS_LATEST.PRODUCER_UUID;
    private static final Field<String> propertyTypeField = PM_STATS_LATEST.PROPERTY_TYPE;
    private static final Field<String> propertySubtypeField = PM_STATS_LATEST.PROPERTY_SUBTYPE;
    private static final Field<String> commodityKeyField = PM_STATS_LATEST.COMMODITY_KEY;
    private static final Field<RelationType> relationField = PM_STATS_LATEST.RELATION;
    private static final String HOUR_KEY_FIELD_NAME = "hour_key";
    private static final String DAY_KEY_FIELD_NAME = "day_key";
    private static final String MONTH_KEY_FIELD_NAME = "month_key";
    // following fields are unsafe in that they do not appear in the Postgres schema in some tables
    // where they do appear in the MariaDB schema. We cannot remove them from the latter without
    // significant cost, but we do not want include them in the former. Once we drop the
    // POSTGRES_PRIMARY_DB feature flag, we can exclude these fields from codegen, which means
    // jOOQ won't know about them, and we should then be fine.
    private static final ImmutableSet<String> UNSAFE_ENTITY_STATS_FIELDS = ImmutableSet.of(
            HOUR_KEY_FIELD_NAME, DAY_KEY_FIELD_NAME, MONTH_KEY_FIELD_NAME);

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public EntityStatsRollupTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(configurableDbDialect, dialect);
    }

    /**
     * Perform a test of rollups by inserting records into a time series over a span of 26 hours
     * crossing a month (and therefore day) boundary in the process, and checking that all rollup
     * tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     * @throws DataAccessException  on db error
     */
    @Test
    @CleanupOverrides(truncate = {PmStatsByHour.class, PmStatsByDay.class, PmStatsByMonth.class})
    public void testRollups() throws InterruptedException, DataAccessException {
        String producerUuid = rand.nextBoolean() ? null : getRandUuid();
        RelationType relation = producerUuid != null ? RelationType.COMMODITIESBOUGHT
                                                     : RelationType.COMMODITIES;
        EntityStatsIdentityValues identityValues = new EntityStatsIdentityValues(
                getRandUuid(), producerUuid, StringConstants.CPU, StringConstants.USED,
                null, relation);
        PmStatsLatestRecord template1 =
                createTemplate(PM_STATS_LATEST, identityValues);
        StatsTimeSeries<PmStatsLatestRecord> ts1 = new StatsTimeSeries<>(PM_STATS_LATEST, template1,
                100_000.0, Instant.parse("2019-01-31T22:01:35Z"), TimeUnit.MINUTES.toMillis(10));
        BulkLoader<PmStatsLatestRecord> loader = loaders.getLoader(PM_STATS_LATEST);
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
        checkEntityRollup(HOUR, template1, hourly10PM, PM_STATS_LATEST, identityValues);
        checkEntityRollup(HOUR, template1, hourly11PM, PM_STATS_LATEST, identityValues);
        checkEntityRollup(HOUR, template1, hourly12AM, PM_STATS_LATEST, identityValues);
        checkEntityRollup(HOUR, template1, hourly1AM, PM_STATS_LATEST, identityValues);
        checkEntityRollup(DAY, template1, dailyJan31, PM_STATS_LATEST, identityValues);
        checkEntityRollup(DAY, template1, dailyFeb1, PM_STATS_LATEST, identityValues);
        checkEntityRollup(MONTH, template1, monthlyJan, PM_STATS_LATEST, identityValues);
        checkEntityRollup(MONTH, template1, monthlyFeb, PM_STATS_LATEST, identityValues);
    }

    /**
     * Check that a rollup record's fields are all as expected.
     *
     * <p>Some fields are checked against the same fields in a "template" record for the time
     * series. Others area checked against values from an aggregator that covers all the records
     * that should contribute to this particular rollup record.</p>
     *
     * @param timeFrame  hourly, daily, or monthly
     * @param template   template record for this time series
     * @param aggregator aggregator for this time series for the period of this rollup record
     * @param table      underlying stats "latest" table
     * @param identity   identity fields for records
     * @param <R>        underlying record type
     * @throws DataAccessException on db error
     * @throws DataAccessException on db error
     */
    protected <R extends Record> void checkEntityRollup(TimeFrame timeFrame, R template,
            Aggregator aggregator, Table<R> table, IdentityValues<PmStatsLatestRecord> identity)
            throws DataAccessException {
        Timestamp snapshot = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(table, timeFrame);
        Field<Timestamp> snapshotTimeField = getTimestampField(rollupTable,
                StringConstants.SNAPSHOT_TIME);
        Record rollup = retrieveRecord(rollupTable, snapshotTimeField.eq(snapshot), identity,
                FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? UNSAFE_ENTITY_STATS_FIELDS
                                                             : Collections.emptySet());
        checkField(template, rollup, StringConstants.UUID, String.class);
        checkField(template, rollup, StringConstants.PRODUCER_UUID, String.class);
        checkField(template, rollup, StringConstants.PROPERTY_TYPE, String.class);
        checkField(template, rollup, StringConstants.PROPERTY_SUBTYPE, String.class);
        checkField(template, rollup, StringConstants.RELATION, RelationType.class);
        checkField(template, rollup, StringConstants.COMMODITY_KEY, String.class);
        checkField(aggregator.getAvg(), rollup, StringConstants.AVG_VALUE, Double.class);
        checkField(aggregator.getMin(), rollup, StringConstants.MIN_VALUE, Double.class);
        checkField(aggregator.getMax(), rollup, StringConstants.MAX_VALUE, Double.class);
        checkField(aggregator.getMaxObservedCapacity(), rollup, StringConstants.CAPACITY,
                Double.class);
        checkField(aggregator.getMaxObservedEffectiveCapacity(), rollup,
                StringConstants.EFFECTIVE_CAPACITY, Double.class);
        checkField(aggregator.getSamples(), rollup, StringConstants.SAMPLES, Integer.class);
    }

    /**
     * CLass to manage identity fields for entity stats records.
     */
    private static class EntityStatsIdentityValues implements IdentityValues<PmStatsLatestRecord> {

        private final String uuid;
        private final String producerUuid;
        private final String propertyType;
        private final String propertySubtype;
        private final String commodityKey;
        private final RelationType relation;

        EntityStatsIdentityValues(String uuid, String producerUuid, String propertyType,
                String propertySubtype, String commodityKey, RelationType relation) {
            this.uuid = uuid;
            this.producerUuid = producerUuid;
            this.propertyType = propertyType;
            this.propertySubtype = propertySubtype;
            this.commodityKey = commodityKey;
            this.relation = relation;
        }

        @Override
        public void set(PmStatsLatestRecord record) {
            record.setValue(uuidField, uuid);
            record.setValue(producerUuidField, producerUuid);
            record.setValue(propertyTypeField, propertyType);
            record.setValue(propertySubtypeField, propertySubtype);
            record.setValue(commodityKeyField, commodityKey);
            record.setValue(relationField, relation);
        }

        @Override
        public List<Condition> matchConditions(Table<?> table) {
            Field<String> uuidField = getStringField(table, StringConstants.UUID);
            Field<String> producerUuidField = getStringField(table, StringConstants.PRODUCER_UUID);
            Field<String> propertyTypeField = getStringField(table, StringConstants.PROPERTY_TYPE);
            Field<String> propertySubtypeField = getStringField(table,
                    StringConstants.PROPERTY_SUBTYPE);
            Field<String> commodityKeyField = getStringField(table, StringConstants.COMMODITY_KEY);
            Field<RelationType> relationField = getRelationTypeField(table,
                    StringConstants.RELATION);
            return Arrays.asList(
                    uuid != null ? uuidField.eq(uuid) : uuidField.isNull(),
                    producerUuid != null ? producerUuidField.eq(producerUuid)
                                         : producerUuidField.isNull(),
                    propertyType != null ? propertyTypeField.eq(propertyType)
                                         : propertyTypeField.isNull(),
                    propertySubtype != null ? propertySubtypeField.eq(propertySubtype)
                                            : propertySubtypeField.isNull(),
                    commodityKey != null ? commodityKeyField.eq(commodityKey)
                                         : commodityKeyField.isNull(),
                    relation != null ? relationField.eq(relation) : relationField.isNull());
        }
    }
}
