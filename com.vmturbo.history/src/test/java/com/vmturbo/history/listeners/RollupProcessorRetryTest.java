package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.RollupProcessor.RollupOutcome.COMPLETE;
import static com.vmturbo.history.listeners.RollupProcessor.RollupOutcome.FAILED;
import static com.vmturbo.history.listeners.RollupProcessor.RollupOutcome.INTERRUPTED;
import static com.vmturbo.history.listeners.RollupProcessor.RollupOutcome.TIMED_OUT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.sql.utils.partition.PartitioningManager;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests of the logic in {@link RollupProcessor} that manages parallel upsert execution, including
 * retries and other error handling logic.
 */
public class RollupProcessorRetryTest {
    private final ArrayDeque<Exception> exceptions = new ArrayDeque<>();
    private final DSLContext dsl = spy(DSL.using(SQLDialect.MARIADB));

    private RollupProcessor rollupProcessor;

    private final Map<Table<?>, Long> tableCounts = ImmutableMap.of(Tables.VM_STATS_LATEST, 10L);

    // flags indicating what sort of rollup operations were attempted during the course of a test
    boolean hourlyAttempted = false;
    boolean dailyAttempted = false;
    boolean monthlyAttempted = false;

    /** Manage feature flag settings. */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    /**
     * Set up mocks etc.
     */
    @Before
    public void before() {
        doAnswer(args -> {
            analyzeQuery(args.getArgumentAt(0, Query.class));
            // this will be null if the queue is empty or if it contains an actual null value
            Exception e = exceptions.poll();
            // used when an operation is executed in a task
            if (e == null) {
                // in normal cases when the upsert succeeds, we need to return a record count
                return 1;
            } else if (e instanceof InterruptedException) {
                // if we're interrupted we'll interrupt our thread group; if the parent
                // processing thread is waiting, it will get interrupted, but it also
                // checks its own interrupted status in its processing loop to catch
                // interruptions while it's not waiting for anything.
                Thread.currentThread().getThreadGroup().interrupt();
                return 1;
            } else if (e instanceof TimeoutException) {
                // here we delay long enough to trigger a timeout in the overall process
                Thread.sleep(1000);
                return 1;
            } else {
                // for anything else we just throw the indicated exception, which will cause
                // the tasks future to complete exceptionally with this exception
                throw e;
            }
        }).when(dsl).execute(any(Query.class));
        // avoid the complications of the partitioning manager (well, at least until we retire
        // this feature flag)
        featureFlagTestRule.disable(FeatureFlags.OPTIMIZE_PARTITIONING);
        this.rollupProcessor = spy(new RollupProcessor(dsl, dsl, mock(PartitioningManager.class),
                Executors::newSingleThreadExecutor, 25000, 500, 500, 2));
        // suppress attempts ot update available_timestamps table
        doNothing().when(rollupProcessor).addAvailableTimestamps(any(), any(), anyVararg());
    }

    private void analyzeQuery(Query query) {
        if (query instanceof Insert) {
            String sql = query.getSQL();
            if (sql.contains("_latest")) {
                hourlyAttempted = true;
                return;
            } else if (sql.contains("_by_day")) {
                dailyAttempted = true;
                return;
            } else if (sql.contains("_by_month")) {
                monthlyAttempted = true;
                return;
            }
        }
        throw new IllegalArgumentException(
                String.format("Invalid rollup upsert: %s", query.getSQL()));
    }

    /**
     * Check the normal case when all upserts succeed.
     */
    @Test
    public void testRollupIsCompleteWhenNoUpsertFails() {
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(COMPLETE));
    }

    /**
     * Check cases where first execution and one concurrent retry fail, but the second concurrent
     * retry succeeds.
     */
    @Test
    public void testRollupIsCompleteIfRetryLimitIsNotExceeded() {
        this.exceptions.add(new SQLException());
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(COMPLETE));
        this.exceptions.addAll(Arrays.asList(new SQLException(), new SQLException()));
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(COMPLETE));
    }

    /**
     * Check the case where the first execution and both concurrent retries fail, but the
     * last-chance serial retry succeeds.
     */
    @Test
    public void testRollupIsCompleteIfLastChanceRetrySucceeds() {
        this.exceptions.add(new SQLException());
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(COMPLETE));
        this.exceptions.addAll(
                Arrays.asList(new SQLException(), new SQLException(), new SQLException()));
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(COMPLETE));
    }

    /**
     * Check the case where the first attempt, both concurrent retry, and the last-chance serial
     * retry all fail.
     */
    @Test
    public void testRollupIsIncompleteIfLastChanceRetryFails() {
        this.exceptions.addAll(Arrays.asList(new SQLException(), new SQLException(),
                new SQLException(), new SQLException()));
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(FAILED));
    }

    /**
     * Check the case where the parent thread is interrupted.
     */
    @Test
    public void testThatInterruptionIsDetected() {
        this.exceptions.add(new InterruptedException());
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(INTERRUPTED));
    }

    /**
     * Check the case where overall execution exceeds the time limit.
     */
    @Test
    public void testThatLongRunningUpsetCausesTimeout() {
        this.exceptions.add(new TimeoutException());
        assertThat(rollupProcessor.performHourRollups(tableCounts, Instant.now()), is(TIMED_OUT));
    }

    /**
     * Check that both daily and monthly rollups are performed when the method to run them is
     * invoked, assuming both work normally.
     */
    @Test
    public void testThatDailiesAndMonthliesBothRunIfNoProblem() {
        assertThat(rollupProcessor.performDayMonthRollups(tableCounts, Instant.now(), false),
                is(COMPLETE));
        assertTrue(dailyAttempted);
        assertTrue(monthlyAttempted);
    }

    /**
     * Check that monthly rollups are attempted even if dailies fail for a reason other than
     * interruption or timeout.
     */
    @Test
    public void testThatMonthliesRunIfDailiesFail() {
        this.exceptions.addAll(Arrays.asList(new SQLDataException(), new SQLDataException(),
                new SQLDataException(), new SQLDataException()));
        assertThat(rollupProcessor.performDayMonthRollups(tableCounts, Instant.now(), false),
                is(FAILED));
        assertTrue(dailyAttempted);
        assertTrue(monthlyAttempted);
    }

    /**
     * Check that monhtlies are skipped if dailies are interrupted.
     */
    @Test
    public void testThatMonthliesDontRunIfDailiesAreInterrupted() {
        this.exceptions.add(new InterruptedException());
        assertThat(rollupProcessor.performDayMonthRollups(tableCounts, Instant.now(), false),
                is(INTERRUPTED));
        assertTrue(dailyAttempted);
        assertFalse(monthlyAttempted);
    }

    /**
     * Check that monthlies are skipped if dailies time out.
     */
    @Test
    public void testThatMonthliesRunIfDailiesTimeOut() {
        this.exceptions.add(new TimeoutException());
        assertThat(rollupProcessor.performDayMonthRollups(tableCounts, Instant.now(), false),
                is(TIMED_OUT));
        assertTrue(dailyAttempted);
        assertTrue(monthlyAttempted);
    }
}
