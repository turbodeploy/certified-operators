package com.vmturbo.history.db;

import static com.vmturbo.commons.TimeFrame.DAY;
import static com.vmturbo.commons.TimeFrame.HOUR;
import static com.vmturbo.commons.TimeFrame.LATEST;
import static com.vmturbo.commons.TimeFrame.MONTH;
import static com.vmturbo.history.schema.HistoryVariety.ENTITY_STATS;
import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.time.Instant;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests of the {@link RetentionPolicy} class.
 *
 * <p>These tests are executed in a test database constructed from history component migrations.</p>
 */
public class RetentionPolicyTest {

    /**
     * Provision and provide access to a test database.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Vmtdb.VMTDB);

    /**
     * Clean up tables in the test database before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    DSLContext dsl = dbConfig.getDslContext();

    /** Initialize retention policies before each test. */
    @Before
    public void before() {
        RetentionPolicy.init(dsl);
    }

    /**
     * Test that retention policies are loaded correctly from the database tables.
     *
     * <p>Of course, this test must be updated if the standard retention policies change.</p>
     */
    @Test
    public void testRetentionPeriodsCorrect() {
        assertEquals(2, (int)RetentionPolicy.LATEST_STATS.getPeriod());
        assertEquals(HOURS, RetentionPolicy.LATEST_STATS.getUnit());
        assertEquals(72, (int)RetentionPolicy.HOURLY_STATS.getPeriod());
        assertEquals(HOURS, RetentionPolicy.HOURLY_STATS.getUnit());
        assertEquals(60, (int)RetentionPolicy.DAILY_STATS.getPeriod());
        assertEquals(DAYS, RetentionPolicy.DAILY_STATS.getUnit());
        assertEquals(24, (int)RetentionPolicy.MONTHLY_STATS.getPeriod());
        assertEquals(MONTHS, RetentionPolicy.MONTHLY_STATS.getUnit());
        assertEquals(30, (int)RetentionPolicy.SYSTEM_LOAD.getPeriod());
        assertEquals(DAYS, RetentionPolicy.SYSTEM_LOAD.getUnit());
        assertEquals(91, (int)RetentionPolicy.PERCENTILE.getPeriod());
        assertEquals(DAYS, RetentionPolicy.PERCENTILE.getUnit());
    }

    /**
     * Test that expirations are correctly calculated.
     *
     * <p>Expected values must be adjusted if standard retention policies change.</p>
     */
    @Test
    public void testExpirations() {
        assertEquals(Instant.parse("2019-01-02T06:00:00.0Z"),
                RetentionPolicy.LATEST_STATS.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
        assertEquals(Instant.parse("2019-01-05T04:00:00.0Z"),
                RetentionPolicy.HOURLY_STATS.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
        assertEquals(Instant.parse("2019-03-04T00:00:00.0Z"),
                RetentionPolicy.DAILY_STATS.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
        assertEquals(Instant.parse("2021-02-01T00:00:00.0Z"),
                RetentionPolicy.MONTHLY_STATS.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
        assertEquals(Instant.parse("2019-02-02T00:00:00.0Z"),
                RetentionPolicy.SYSTEM_LOAD.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
        assertEquals(Instant.parse("2019-04-04T00:00:00.0Z"),
                RetentionPolicy.PERCENTILE.getExpiration(Instant.parse("2019-01-02T03:04:05.0Z")));
    }

    /**
     * Test that expiration values in available_timestamps records are updated correctly in response to
     * reported changes in retention policies during live operation.
     *
     */
    @Test
    public void testPolicyChange() {
        // create some available_snapshots reocrds to test
        Timestamp tsL = Timestamp.from(Instant.parse("2019-01-02T03:00:00Z"));
        insertAvailableTimestamp(tsL, LATEST, ENTITY_STATS, RetentionPolicy.LATEST_STATS, dsl);
        Timestamp tsL1 = Timestamp.from(Instant.parse("2019-01-02T03:00:01Z"));
        insertAvailableTimestamp(tsL1, LATEST, ENTITY_STATS, RetentionPolicy.LATEST_STATS, dsl);
        Timestamp tsH = Timestamp.from(Instant.parse("2019-01-02T03:00:00Z"));
        insertAvailableTimestamp(tsH, HOUR, ENTITY_STATS, RetentionPolicy.HOURLY_STATS, dsl);
        Timestamp tsH1 = Timestamp.from(Instant.parse("2019-01-02T03:00:01Z"));
        insertAvailableTimestamp(tsH1, HOUR, ENTITY_STATS, RetentionPolicy.HOURLY_STATS, dsl);
        Timestamp tsD = Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"));
        insertAvailableTimestamp(tsD, DAY, ENTITY_STATS, RetentionPolicy.DAILY_STATS, dsl);
        Timestamp tsD1 = Timestamp.from(Instant.parse("2019-01-02T00:00:01Z"));
        insertAvailableTimestamp(tsD1, DAY, ENTITY_STATS, RetentionPolicy.DAILY_STATS, dsl);
        Timestamp tsM = Timestamp.from(Instant.parse("2019-01-01T00:00:00Z"));
        insertAvailableTimestamp(tsM, MONTH, ENTITY_STATS, RetentionPolicy.MONTHLY_STATS, dsl);
        Timestamp tsM1 = Timestamp.from(Instant.parse("2019-01-01T00:00:01Z"));
        insertAvailableTimestamp(tsM1, MONTH, ENTITY_STATS, RetentionPolicy.MONTHLY_STATS, dsl);
        // make some policy changes
        changePolicy(RetentionPolicy.LATEST_STATS, 3, dsl);
        changePolicy(RetentionPolicy.HOURLY_STATS, 15, dsl);
        changePolicy(RetentionPolicy.DAILY_STATS, 10, dsl);
        changePolicy(RetentionPolicy.MONTHLY_STATS, 5, dsl);
        // notify regarding the change
        RetentionPolicy.onChange();
        // retreive available_timestamps records and check that they have correctly updated expirations
        AvailableTimestampsRecord rL = getAvailableTimestamp(tsL, LATEST, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-02T06:00:00Z"), rL.getExpiresAt().toInstant());
        AvailableTimestampsRecord rL1 = getAvailableTimestamp(tsL1, LATEST, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-02T07:00:00Z"), rL1.getExpiresAt().toInstant());
        AvailableTimestampsRecord rH = getAvailableTimestamp(tsH, HOUR, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-02T18:00:00Z"), rH.getExpiresAt().toInstant());
        AvailableTimestampsRecord rH1 = getAvailableTimestamp(tsH1, HOUR, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-02T19:00:00Z"), rH1.getExpiresAt().toInstant());
        AvailableTimestampsRecord rD = getAvailableTimestamp(tsD, DAY, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-12T00:00:00Z"), rD.getExpiresAt().toInstant());
        AvailableTimestampsRecord rD1 = getAvailableTimestamp(tsD1, DAY, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-01-13T00:00:00Z"), rD1.getExpiresAt().toInstant());
        AvailableTimestampsRecord rM = getAvailableTimestamp(tsM, MONTH, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-06-01T00:00:00Z"), rM.getExpiresAt().toInstant());
        AvailableTimestampsRecord rM1 = getAvailableTimestamp(tsM1, MONTH, ENTITY_STATS, dsl);
        assertEquals(Instant.parse("2019-07-01T00:00:00Z"), rM1.getExpiresAt().toInstant());
        // Undo our database changes (We couldn't just do everything in a transaction and roll it back,
        // unfortunately, because the policy changes need to be committed in order for RetentionPolicy class
        // to see those changes when processing the change.)
        dsl.deleteFrom(AVAILABLE_TIMESTAMPS).execute();
        changePolicy(RetentionPolicy.LATEST_STATS, 2, dsl);
        changePolicy(RetentionPolicy.HOURLY_STATS, 72, dsl);
        changePolicy(RetentionPolicy.DAILY_STATS, 60, dsl);
        changePolicy(RetentionPolicy.MONTHLY_STATS, 24, dsl);
        RetentionPolicy.onChange();
    }

    /**
     * Retrieve a record from the available_timestamps table.
     *
     * @param timestamp      time_stamp value of record
     * @param timeFrame      time_frame value of record
     * @param historyVariety history_variety value of record
     * @param ctx            jOOQ context
     * @return retrieved records
     */
    private AvailableTimestampsRecord getAvailableTimestamp(
            Timestamp timestamp, TimeFrame timeFrame, HistoryVariety historyVariety, DSLContext ctx) {
        return ctx.selectFrom(AVAILABLE_TIMESTAMPS)
                .where(AVAILABLE_TIMESTAMPS.TIME_STAMP.eq(timestamp),
                        AVAILABLE_TIMESTAMPS.TIME_FRAME.eq(timeFrame.name()),
                        AVAILABLE_TIMESTAMPS.HISTORY_VARIETY.eq(historyVariety.name()))
                .fetchOne();
    }

    /**
     * Update the retention period in a retention_policies record in the database.
     *
     * @param policy the {@link RetentionPolicy} enum member for the policy
     * @param value  new policy value
     * @param ctx    jOOQ context
     */
    private void changePolicy(RetentionPolicy policy, int value, DSLContext ctx) {
        ctx.update(RETENTION_POLICIES)
                .set(RETENTION_POLICIES.RETENTION_PERIOD, value)
                .where(RETENTION_POLICIES.POLICY_NAME.eq(policy.getKey()))
                .execute();
    }

    /**
     * Insert a record into the available_timestamps table.
     *
     * @param timestamp      time_stamp value
     * @param timeFrame      time_frame value
     * @param historyVariety history_variety value
     * @param policy         the {@link RetentionPolicy} enum member that will be used to compute expires_at value
     * @param ctx            jOOQ context
     */
    private void insertAvailableTimestamp(Timestamp timestamp,
            TimeFrame timeFrame,
            HistoryVariety historyVariety,
            RetentionPolicy policy,
            DSLContext ctx) {
        ctx.insertInto(AVAILABLE_TIMESTAMPS)
                .set(AVAILABLE_TIMESTAMPS.TIME_STAMP, timestamp)
                .set(AVAILABLE_TIMESTAMPS.TIME_FRAME, timeFrame.name())
                .set(AVAILABLE_TIMESTAMPS.HISTORY_VARIETY, historyVariety.name())
                .set(AVAILABLE_TIMESTAMPS.EXPIRES_AT,
                        Timestamp.from(policy.getExpiration(timestamp.toInstant())))
                .execute();
    }
}
