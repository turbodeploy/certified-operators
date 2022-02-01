package com.vmturbo.history.db.procedure;

import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_ENTRIES;
import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.MOVING_STATISTICS_BLOBS;
import static com.vmturbo.history.schema.abstraction.Tables.PERCENTILE_BLOBS;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.schema.RetentionUtil;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.sql.utils.jooq.PurgeEvent;

/**
 * Test that all the purge procedures delete data correctly based on retention policy period.
 */
@RunWith(Parameterized.class)
public class StoredProcedureConfigTest extends MultiDbTestBase {

    private static final Clock clock = Clock.systemUTC();

    private static final long pastTime1 = TimeUtil.localDateTimeToMilli(
            LocalDateTime.of(2021, 2, 10, 9, 0), clock);
    private static final long pastTime2 = TimeUtil.localDateTimeToMilli(
            LocalDateTime.of(2021, 3, 10, 9, 0), clock);
    private static final long currentTime = TimeUtil.localDateTimeToMilli(
            LocalDateTime.of(2021, 3, 20, 9, 0), clock);
    private static final long futureTime1 = TimeUtil.localDateTimeToMilli(
            LocalDateTime.of(3000, 3, 14, 9, 0), clock);

    /**
     * Provide test parameters. It doesn't support the legacy mariadb.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return new Object[][]{ DBENDPOINT_MARIADB_PARAMS, DBENDPOINT_POSTGRES_PARAMS };
    }

    private final DSLContext dsl;
    private final StoredProcedureConfig storedProcedureConfig;

    /**
     * Construct a rule chaining for a given scenario.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if there's a provisioning problem
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public StoredProcedureConfigTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        this.dsl = super.getDslContext();
        this.storedProcedureConfig = new TestStoredProcedureConfig(dsl);
    }

    /**
     * Test that percentile data is purged correctly.
     */
    @Test
    public void testPurgeExpiredPercentileData() {
        // initialize records
        Stream.of(0L, pastTime1, pastTime2, futureTime1).forEach(timestamp -> {
            long aggregationWindowLength = timestamp == 0 ? currentTime : 86400000L;
            dsl.insertInto(PERCENTILE_BLOBS).columns(
                    PERCENTILE_BLOBS.START_TIMESTAMP,
                    PERCENTILE_BLOBS.AGGREGATION_WINDOW_LENGTH,
                    PERCENTILE_BLOBS.DATA,
                    PERCENTILE_BLOBS.CHUNK_INDEX
            ).values(timestamp, aggregationWindowLength, new byte[]{}, 0).execute();
        });

        // before
        assertThat(dsl.fetchCount(PERCENTILE_BLOBS), is(4));

        // remove retention policy and verify the purge doesn't delete anything
        deleteRetentionPolicy(RetentionUtil.PERCENTILE_RETENTION_POLICY_NAME);
        // purge
        storedProcedureConfig.purgeExpiredPercentileData().getProcedure().run();
        assertThat(dsl.fetchCount(PERCENTILE_BLOBS), is(4));

        // set retention period to 7 days, so pastTime1 and pastTime2 are removed
        setRetentionPeriod(RetentionUtil.PERCENTILE_RETENTION_POLICY_NAME, 7);
        // purge
        storedProcedureConfig.purgeExpiredPercentileData().getProcedure().run();
        List<Long> rest = dsl.select(PERCENTILE_BLOBS.START_TIMESTAMP).from(PERCENTILE_BLOBS)
                .stream().map(Record1::value1).collect(Collectors.toList());
        assertThat(rest, containsInAnyOrder(0L, futureTime1));
    }

    /**
     * Test that moving stats data is purged correctly.
     */
    @Test
    public void testPurgeExpiredMovingStatisticsData() {
        // initialize records
        Stream.of(pastTime1, pastTime2, currentTime).forEach(timestamp -> {
            dsl.insertInto(MOVING_STATISTICS_BLOBS).columns(
                    MOVING_STATISTICS_BLOBS.START_TIMESTAMP,
                    MOVING_STATISTICS_BLOBS.DATA,
                    MOVING_STATISTICS_BLOBS.CHUNK_INDEX
            ).values(timestamp, new byte[]{}, 0).execute();
        });

        // before
        assertThat(dsl.fetchCount(MOVING_STATISTICS_BLOBS), is(3));

        // remove retention policy and verify the purge doesn't delete anything
        deleteRetentionPolicy(RetentionUtil.MOVING_STATISTICS_RETENTION_POLICY_NAME);
        // purge
        storedProcedureConfig.purgeExpiredMovingStatisticsData().getProcedure().run();
        assertThat(dsl.fetchCount(MOVING_STATISTICS_BLOBS), is(3));

        // set retention period to 7 days, so pastTime1 and pastTime2 are removed
        setRetentionPeriod(RetentionUtil.MOVING_STATISTICS_RETENTION_POLICY_NAME, 7);
        // purge
        storedProcedureConfig.purgeExpiredMovingStatisticsData().getProcedure().run();
        List<Long> rest = dsl.select(MOVING_STATISTICS_BLOBS.START_TIMESTAMP)
                .from(MOVING_STATISTICS_BLOBS).stream()
                .map(Record1::value1).collect(Collectors.toList());
        assertThat(rest, containsInAnyOrder(currentTime));

        // clean
        dsl.deleteFrom(MOVING_STATISTICS_BLOBS).execute();
    }

    /**
     * Test that system load data is purged correctly.
     */
    @Test
    public void testPurgeExpiredSystemloadData() {
        // initialize records
        Stream.of(pastTime1, pastTime2, futureTime1).forEach(timestamp -> {
            dsl.insertInto(SYSTEM_LOAD).columns(SYSTEM_LOAD.SNAPSHOT_TIME)
                    .values(new Timestamp(timestamp)).execute();
        });

        // before
        assertThat(dsl.fetchCount(SYSTEM_LOAD), is(3));

        // remove retention policy and verify the purge doesn't delete anything
        deleteRetentionPolicy(RetentionUtil.SYSTEM_LOAD_RETENTION_POLICY_NAME);
        // purge
        storedProcedureConfig.purgeExpiredSystemloadData().getProcedure().run();
        assertThat(dsl.fetchCount(SYSTEM_LOAD), is(3));

        // set retention period to 7 days
        setRetentionPeriod(RetentionUtil.SYSTEM_LOAD_RETENTION_POLICY_NAME, 7);
        // purge
        storedProcedureConfig.purgeExpiredSystemloadData().getProcedure().run();
        List<Long> rest = dsl.select(SYSTEM_LOAD.SNAPSHOT_TIME)
                .from(SYSTEM_LOAD).stream()
                .map(Record1::value1)
                .map(Timestamp::getTime)
                .collect(Collectors.toList());
        assertThat(rest, containsInAnyOrder(futureTime1));
    }

    /**
     * Test that audit log data is purged correctly.
     */
    @Test
    public void testPurgeAuditLogExpiredEntries() {
        // initialize records
        Stream.of(pastTime1, pastTime2, futureTime1).forEach(timestamp -> {
            dsl.insertInto(AUDIT_LOG_ENTRIES).columns(AUDIT_LOG_ENTRIES.SNAPSHOT_TIME)
                    .values(new Timestamp(timestamp)).execute();
        });

        // before
        assertThat(dsl.fetchCount(AUDIT_LOG_ENTRIES), is(3));

        // remove retention policy and verify the purge doesn't delete anything
        dsl.deleteFrom(AUDIT_LOG_RETENTION_POLICIES).where(
                AUDIT_LOG_RETENTION_POLICIES.POLICY_NAME.eq(
                        RetentionUtil.AUDIT_LOG_RETENTION_POLICY_NAME)).execute();

        // purge
        storedProcedureConfig.purgeAuditLogExpiredEntries().getProcedure().run();
        assertThat(dsl.fetchCount(AUDIT_LOG_ENTRIES), is(3));

        // set retention period to 7 days
        dsl.insertInto(AUDIT_LOG_RETENTION_POLICIES).columns(
                AUDIT_LOG_RETENTION_POLICIES.POLICY_NAME,
                AUDIT_LOG_RETENTION_POLICIES.RETENTION_PERIOD)
                .values(RetentionUtil.AUDIT_LOG_RETENTION_POLICY_NAME, 7).execute();
        // purge
        storedProcedureConfig.purgeAuditLogExpiredEntries().getProcedure().run();
        List<Long> rest = dsl.select(AUDIT_LOG_ENTRIES.SNAPSHOT_TIME)
                .from(AUDIT_LOG_ENTRIES).stream()
                .map(Record1::value1)
                .map(Timestamp::getTime)
                .collect(Collectors.toList());
        assertThat(rest, containsInAnyOrder(futureTime1));
    }

    private void deleteRetentionPolicy(String policyName) {
        dsl.deleteFrom(RETENTION_POLICIES).where(RETENTION_POLICIES.POLICY_NAME.eq(policyName))
                .execute();
    }

    private void setRetentionPeriod(String policyName, Integer value) {
        if (dsl.fetchExists(RETENTION_POLICIES.where(
                RETENTION_POLICIES.POLICY_NAME.eq(policyName)))) {
            dsl.update(RETENTION_POLICIES).set(RETENTION_POLICIES.RETENTION_PERIOD, value).execute();
        } else {
            dsl.insertInto(RETENTION_POLICIES).columns(RETENTION_POLICIES.POLICY_NAME,
                    RETENTION_POLICIES.RETENTION_PERIOD, RETENTION_POLICIES.UNIT)
                    .values(policyName, value, "DAYS").execute();
        }
    }

    /**
     * Test config.
     */
    static class TestStoredProcedureConfig extends StoredProcedureConfig {

        private final DSLContext dsl;

        TestStoredProcedureConfig(DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext dsl()
                throws SQLException, UnsupportedDialectException, InterruptedException {
            return dsl;
        }

        @Override
        public PurgeEvent createPurgeEvent(String name, Runnable runnable) {
            PurgeEvent purgeEvent = spy(super.createPurgeEvent(name, runnable));
            // avoid actual invoke by scheduler
            doReturn(purgeEvent).when(purgeEvent).schedule(anyInt(), anyLong());
            return purgeEvent;
        }
    }
}