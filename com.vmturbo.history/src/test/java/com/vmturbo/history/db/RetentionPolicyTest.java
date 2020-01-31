package com.vmturbo.history.db;

import static com.vmturbo.history.schema.HistoryVariety.ENTITY_STATS;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.LATEST;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import org.jooq.DSLContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.stats.DbTestConfig;

/**
 * Tests of the {@link RetentionPolicy} class.
 *
 * <p>These tests are executed in a test database constructed from history component migrations.</p>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class RetentionPolicyTest {
    @Autowired
    private DbTestConfig dbTestConfig;

    private static HistorydbIO historydbIO;
    private static String testDbName;

    /**
     * Create a history database to be used by all tests.
     *
     * @throws VmtDbException if an error occurs during migrations
     */
    @Before
    public void setup() throws VmtDbException {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        historydbIO.init(false, null, testDbName, Optional.empty());
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
     * @throws VmtDbException if certain DB errors occur
     * @throws SQLException if other DB errors occur
     */
    @Test
    public void testPolicyChange() throws VmtDbException, SQLException {
        try (Connection conn = historydbIO.connection()) {
            DSLContext ctx = historydbIO.using(conn);
            // create some available_snapshots reocrds to test
            Timestamp tsL = Timestamp.from(Instant.parse("2019-01-02T03:00:00Z"));
            insertAvailableTimestamp(tsL, LATEST, ENTITY_STATS, RetentionPolicy.LATEST_STATS, ctx);
            Timestamp tsL1 = Timestamp.from(Instant.parse("2019-01-02T03:00:01Z"));
            insertAvailableTimestamp(tsL1, LATEST, ENTITY_STATS, RetentionPolicy.LATEST_STATS, ctx);
            Timestamp tsH = Timestamp.from(Instant.parse("2019-01-02T03:00:00Z"));
            insertAvailableTimestamp(tsH, HOUR, ENTITY_STATS, RetentionPolicy.HOURLY_STATS, ctx);
            Timestamp tsH1 = Timestamp.from(Instant.parse("2019-01-02T03:00:01Z"));
            insertAvailableTimestamp(tsH1, HOUR, ENTITY_STATS, RetentionPolicy.HOURLY_STATS, ctx);
            Timestamp tsD = Timestamp.from(Instant.parse("2019-01-02T00:00:00Z"));
            insertAvailableTimestamp(tsD, DAY, ENTITY_STATS, RetentionPolicy.DAILY_STATS, ctx);
            Timestamp tsD1 = Timestamp.from(Instant.parse("2019-01-02T00:00:01Z"));
            insertAvailableTimestamp(tsD1, DAY, ENTITY_STATS, RetentionPolicy.DAILY_STATS, ctx);
            Timestamp tsM = Timestamp.from(Instant.parse("2019-01-01T00:00:00Z"));
            insertAvailableTimestamp(tsM, MONTH, ENTITY_STATS, RetentionPolicy.MONTHLY_STATS, ctx);
            Timestamp tsM1 = Timestamp.from(Instant.parse("2019-01-01T00:00:01Z"));
            insertAvailableTimestamp(tsM1, MONTH, ENTITY_STATS, RetentionPolicy.MONTHLY_STATS, ctx);
            // make some policy changes
            changePolicy(RetentionPolicy.LATEST_STATS, 3, ctx);
            changePolicy(RetentionPolicy.HOURLY_STATS, 15, ctx);
            changePolicy(RetentionPolicy.DAILY_STATS, 10, ctx);
            changePolicy(RetentionPolicy.MONTHLY_STATS, 5, ctx);
            // notify regarding the change
            RetentionPolicy.onChange();
            // retreive available_timestamps records and check that they have correctly updated expirations
            AvailableTimestampsRecord rL = getAvailableTimestamp(tsL, LATEST, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-02T06:00:00Z"), rL.getExpiresAt().toInstant());
            AvailableTimestampsRecord rL1 = getAvailableTimestamp(tsL1, LATEST, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-02T07:00:00Z"), rL1.getExpiresAt().toInstant());
            AvailableTimestampsRecord rH = getAvailableTimestamp(tsH, HOUR, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-02T18:00:00Z"), rH.getExpiresAt().toInstant());
            AvailableTimestampsRecord rH1 = getAvailableTimestamp(tsH1, HOUR, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-02T19:00:00Z"), rH1.getExpiresAt().toInstant());
            AvailableTimestampsRecord rD = getAvailableTimestamp(tsD, DAY, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-12T00:00:00Z"), rD.getExpiresAt().toInstant());
            AvailableTimestampsRecord rD1 = getAvailableTimestamp(tsD1, DAY, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-01-13T00:00:00Z"), rD1.getExpiresAt().toInstant());
            AvailableTimestampsRecord rM = getAvailableTimestamp(tsM, MONTH, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-06-01T00:00:00Z"), rM.getExpiresAt().toInstant());
            AvailableTimestampsRecord rM1 = getAvailableTimestamp(tsM1, MONTH, ENTITY_STATS, ctx);
            assertEquals(Instant.parse("2019-07-01T00:00:00Z"), rM1.getExpiresAt().toInstant());
            // Undo our database changes (We couldn't just do everything in a transaction and roll it back,
            // unfortunately, because the policy changes need to be committed in order for RetentionPolicy class
            // to see those changes when processing the change.)
            ctx.deleteFrom(AVAILABLE_TIMESTAMPS).execute();
            changePolicy(RetentionPolicy.LATEST_STATS, 2, ctx);
            changePolicy(RetentionPolicy.HOURLY_STATS, 72, ctx);
            changePolicy(RetentionPolicy.DAILY_STATS, 60, ctx);
            changePolicy(RetentionPolicy.MONTHLY_STATS, 24, ctx);
            RetentionPolicy.onChange();
        }
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
