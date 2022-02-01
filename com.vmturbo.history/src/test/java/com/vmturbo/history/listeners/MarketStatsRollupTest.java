package com.vmturbo.history.listeners;

import static com.vmturbo.history.db.jooq.JooqUtils.getEnvField;
import static com.vmturbo.history.db.jooq.JooqUtils.getLongField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_LATEST;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests for market-stats rollups.
 */
public class MarketStatsRollupTest extends RollupTestBase {
    private static final String TOPOLOGY_CONTEXT_ID = "topology_context_id";
    private static final String ENVIRONMENT_TYPE_FIELD_NAME = "environment_type";

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public MarketStatsRollupTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(configurableDbDialect, dialect);
    }

    /**
     * Perform a test of market-stats rollups by inserting records into a time series over a span of
     * 26 hours crossing a month (and therefore day) boundary in the process, and checking that all
     * rollup tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    @CleanupOverrides(
            truncate = {MarketStatsByHour.class, MarketStatsByDay.class, MarketStatsByMonth.class})
    public void testMarketStatsRollups() throws InterruptedException {
        // We need to effectively ignore this test if we're operating in legacy scenario. The
        // reason is that in that case, rollups will be performed by a stored proc that appears
        // to work properly only if the JVM and the database are in UTC timezone. Nothing has
        // changed in the stored proc that would affect this, so it would appear to be a problem
        // that we have always had - except that there has never been a live-db test of market
        // stats rollups! Effect on production data of not having timezones properly set (e.g. at
        // BoFA) should be investigated. The new implementation does not have this problem, so
        // we need to assess the impact of the potential change where timezones are out of whack.
        if (!FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            return;
        }
        RelationType relation = rand.nextBoolean() ? RelationType.COMMODITIESBOUGHT
                                                   : RelationType.COMMODITIES;
        MarketStatsIdentity identity = new MarketStatsIdentity(777777L,
                StringConstants.VIRTUAL_MACHINE, EnvironmentType.ON_PREM, StringConstants.CPU,
                StringConstants.USED, relation);
        MarketStatsLatestRecord template1 = createTemplate(MARKET_STATS_LATEST, identity);
        StatsTimeSeries<MarketStatsLatestRecord> ts1 = new StatsTimeSeries<>(
                MARKET_STATS_LATEST, template1, 100_000.0,
                Instant.parse("2019-01-31T22:01:35Z"), TimeUnit.MINUTES.toMillis(10));
        BulkLoader<MarketStatsLatestRecord> loader = loaders.getLoader(MARKET_STATS_LATEST);
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
        checkMarketStatsRollup(HOUR, template1, hourly10PM, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(HOUR, template1, hourly11PM, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(HOUR, template1, hourly12AM, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(HOUR, template1, hourly1AM, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(DAY, template1, dailyJan31, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(DAY, template1, dailyFeb1, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(MONTH, template1, monthlyJan, MARKET_STATS_LATEST, identity);
        checkMarketStatsRollup(MONTH, template1, monthlyFeb, MARKET_STATS_LATEST, identity);
    }

    private <R extends Record> void checkMarketStatsRollup(TimeFrame timeFrame, R template,
            Aggregator aggregator, Table<R> table, IdentityValues<R> identity)
            throws DataAccessException {
        Timestamp snapshotTime = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(table, timeFrame);
        Field<Timestamp> snapshotTimeField = getTimestampField(rollupTable,
                StringConstants.SNAPSHOT_TIME);
        Record rollup = retrieveRecord(
                rollupTable, snapshotTimeField.eq(snapshotTime), identity);
        checkField(template, rollup, TOPOLOGY_CONTEXT_ID, Long.class);
        checkField(template, rollup, StringConstants.ENTITY_TYPE, String.class);
        checkField(template, rollup, ENVIRONMENT_TYPE_FIELD_NAME, EnvironmentType.class);
        checkField(template, rollup, StringConstants.PROPERTY_TYPE, String.class);
        checkField(template, rollup, StringConstants.PROPERTY_SUBTYPE, String.class);
        checkField(template, rollup, StringConstants.RELATION, RelationType.class);
        checkField(aggregator.getAvg(), rollup, StringConstants.AVG_VALUE, Double.class);
        checkField(aggregator.getMin(), rollup, StringConstants.MIN_VALUE, Double.class);
        checkField(aggregator.getMax(), rollup, StringConstants.MAX_VALUE, Double.class);
    }

    /**
     * Get the rollup table for market stats for the giventime frame.
     *
     * @param timeFrame rollup timeframe
     * @return the rolulp table
     */
    @Override
    protected Table<?> getRollupTable(Table<?> table, TimeFrame timeFrame) {
        switch (timeFrame) {
            case HOUR:
                return Tables.MARKET_STATS_BY_HOUR;
            case DAY:
                return Tables.MARKET_STATS_BY_DAY;
            case MONTH:
                return Tables.MARKET_STATS_BY_MONTH;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Class to manage identity values for market-stats records.
     */
    private static class MarketStatsIdentity implements IdentityValues<MarketStatsLatestRecord> {

        private static final Field<Long> topologyContextIdField =
                MARKET_STATS_LATEST.TOPOLOGY_CONTEXT_ID;
        private static final Field<String> entityTypeField = MARKET_STATS_LATEST.ENTITY_TYPE;
        private static final Field<EnvironmentType> environmentTypeField =
                MARKET_STATS_LATEST.ENVIRONMENT_TYPE;
        private static final TableField<MarketStatsLatestRecord, String>
                propertyTypeField = MARKET_STATS_LATEST.PROPERTY_TYPE;
        private static final TableField<MarketStatsLatestRecord, String>
                propertySubtypeField = MARKET_STATS_LATEST.PROPERTY_SUBTYPE;
        private static final TableField<MarketStatsLatestRecord, RelationType>
                relationField = MARKET_STATS_LATEST.RELATION;

        private final Long topologyContextId;
        private final String entityType;
        private final EnvironmentType environmentType;
        private final String propertyType;
        private final String propertySubtype;
        private final RelationType relation;

        MarketStatsIdentity(Long topologyContextId, String entityType,
                EnvironmentType environmentType, String propertyType, String propertySubtype,
                RelationType relation) {
            this.topologyContextId = topologyContextId;
            this.entityType = entityType;
            this.environmentType = environmentType;
            this.propertyType = propertyType;
            this.propertySubtype = propertySubtype;
            this.relation = relation;
        }

        @Override
        public void set(MarketStatsLatestRecord record) {
            record.set(topologyContextIdField, topologyContextId);
            record.set(entityTypeField, entityType);
            record.set(environmentTypeField, environmentType);
            record.set(propertyTypeField, propertyType);
            record.set(propertySubtypeField, propertySubtype);
            record.set(relationField, relation);
        }


        @Override
        public List<Condition> matchConditions(Table<?> table) {
            Field<Long> topologyContextIdField = getLongField(table, TOPOLOGY_CONTEXT_ID);
            Field<String> entityTypeField = getStringField(table, StringConstants.ENTITY_TYPE);
            Field<EnvironmentType> environmentTypeField = getEnvField(table,
                    ENVIRONMENT_TYPE_FIELD_NAME);
            Field<String> propertyTypeField = getStringField(table, StringConstants.PROPERTY_TYPE);
            Field<String> propertySubtypeField = getStringField(table,
                    StringConstants.PROPERTY_SUBTYPE);
            Field<RelationType> relationField = getRelationTypeField(table,
                    StringConstants.RELATION);

            return Arrays.asList(
                    topologyContextId != null ? topologyContextIdField.eq(topologyContextId)
                                              : topologyContextIdField.isNull(),
                    entityType != null ? entityTypeField.eq(entityType) : entityTypeField.isNull(),
                    environmentType != null ? environmentTypeField.eq(environmentTypeField)
                                            : environmentTypeField.isNull(),
                    propertyType != null ? propertyTypeField.eq(propertyType)
                                         : propertyTypeField.isNull(),
                    propertySubtype != null ? propertySubtypeField.eq(propertySubtype)
                                            : propertySubtypeField.isNull(),
                    relation != null ? relationField.eq(relation) : relationField.isNull()
            );
        }
    }
}
