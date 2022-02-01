package com.vmturbo.history.listeners;

import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;

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
import org.jooq.exception.DataAccessException;
import org.junit.Test;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class to test cluster stats rollups.
 */
public class ClusterStatsRollupTest extends RollupTestBase {

    private static final Field<String> internalNameField = CLUSTER_STATS_LATEST.INTERNAL_NAME;
    private static final Field<String> propertyTypeField = CLUSTER_STATS_LATEST.PROPERTY_TYPE;
    private static final Field<String> propertySubtypeField = CLUSTER_STATS_LATEST.PROPERTY_SUBTYPE;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public ClusterStatsRollupTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(configurableDbDialect, dialect);
    }

    /**
     * Perform a test of rollups by inserting records into a time series over a span of 26 hours
     * crossing a month (and therefore day) boundary in the process, and checking that all rollup
     * tables have correct values in all fields.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    @CleanupOverrides(truncate = {ClusterStatsByHour.class, ClusterStatsByDay.class,
            ClusterStatsByMonth.class})
    public void testClusterRollup() throws InterruptedException {
        ClusterStatsIdentity identity = new ClusterStatsIdentity(getRandUuid(), StringConstants.CPU,
                StringConstants.USED);
        ClusterStatsLatestRecord template1 = createTemplate(CLUSTER_STATS_LATEST, identity);
        StatsTimeSeries<ClusterStatsLatestRecord> ts1 = new StatsTimeSeries<>(CLUSTER_STATS_LATEST,
                template1, 100_000.0, Instant.parse("2019-01-31T22:01:35Z"),
                TimeUnit.MINUTES.toMillis(10));
        BulkLoader<ClusterStatsLatestRecord> loader = loaders.getLoader(CLUSTER_STATS_LATEST);
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
        checkClusterRollup(HOUR, template1, hourly10PM, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(HOUR, template1, hourly11PM, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(HOUR, template1, hourly12AM, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(HOUR, template1, hourly1AM, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(DAY, template1, dailyJan31, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(DAY, template1, dailyFeb1, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(MONTH, template1, monthlyJan, CLUSTER_STATS_LATEST, identity);
        checkClusterRollup(MONTH, template1, monthlyFeb, CLUSTER_STATS_LATEST, identity);
    }

    private <R extends Record> void checkClusterRollup(TimeFrame timeFrame, R template,
            Aggregator aggregator, Table<R> table, IdentityValues<R> identityValues)
            throws DataAccessException {
        Timestamp snapshot = getRollupSnapshot(timeFrame, aggregator.getLatestSnapshot());
        Table<?> rollupTable = getRollupTable(table, timeFrame);
        Field<Timestamp> recordedOnField = getTimestampField(rollupTable,
                StringConstants.RECORDED_ON);
        Record rollup = retrieveRecord(rollupTable, recordedOnField.eq(snapshot), identityValues);
        checkField(template, rollup, StringConstants.INTERNAL_NAME, String.class);
        checkField(template, rollup, StringConstants.PROPERTY_TYPE, String.class);
        checkField(template, rollup, StringConstants.PROPERTY_SUBTYPE, String.class);
        checkField(aggregator.getAvg(), rollup, StringConstants.VALUE, Double.class);
    }

    /**
     * Class to manage identity values for cluster stats records.
     */
    private static class ClusterStatsIdentity implements IdentityValues<ClusterStatsLatestRecord> {

        private final String internalName;
        private final String propertyType;
        private final String propertySubtype;

        ClusterStatsIdentity(
                String internalName, String propertyType, String propertySubtype) {
            this.internalName = internalName;
            this.propertyType = propertyType;
            this.propertySubtype = propertySubtype;
        }

        @Override
        public void set(ClusterStatsLatestRecord record) {
            record.set(internalNameField, internalName);
            record.set(propertyTypeField, propertyType);
            record.set(propertySubtypeField, propertySubtype);
        }

        @Override
        public List<Condition> matchConditions(Table<?> table) {
            Field<String> internalNameField = getStringField(table, StringConstants.INTERNAL_NAME);
            Field<String> propertyTypeField = getStringField(table, StringConstants.PROPERTY_TYPE);
            Field<String> propertySubtypeField = getStringField(table,
                    StringConstants.PROPERTY_SUBTYPE);

            return Arrays.asList(
                    internalName != null ? internalNameField.eq(internalName)
                                         : internalNameField.isNull(),
                    propertyType != null ? propertyTypeField.eq(propertyType)
                                         : propertyTypeField.isNull(),
                    propertySubtype != null ? propertySubtypeField.eq(propertySubtype)
                                            : propertySubtypeField.isNull()
            );
        }
    }
}
