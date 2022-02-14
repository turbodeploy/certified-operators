package com.vmturbo.cost.component.billedcosts;

import static com.vmturbo.cost.component.db.Tables.BILLED_COST_DAILY;
import static com.vmturbo.cost.component.db.Tables.BILLED_COST_MONTHLY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.tables.BilledCostMonthly;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RolledUpTable;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link RollupBilledCostProcessor}.
 */
public class RollupBilledCostProcessorTest {

    private static final long ENTITY_1 = 1L;
    private static final long ENTITY_2 = 2L;

    /**
     * Rule to create DB schema and migrate it.
     */
    @ClassRule
    public static final DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);
    private final DSLContext dslContext = dbConfig.getDslContext();

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public final DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final Clock clock = Clock.systemUTC();
    private RollupTimesStore rollupTimesStore;
    private RollupBilledCostProcessor rollupBilledCostProcessor;

    @Before
    public void setup() {
        rollupTimesStore = new RollupTimesStore(dslContext, RolledUpTable.BILLED_COST);
        final SqlBilledCostStore billedCostStore = new SqlBilledCostStore(
                dslContext, mock(BatchInserter.class), mock(TimeFrameCalculator.class));
        rollupBilledCostProcessor = new RollupBilledCostProcessor(billedCostStore,
                rollupTimesStore, clock);
    }

    @Test
    @CleanupOverrides(truncate = {BilledCostMonthly.class})
    public void testPerformRollup() {
        final YearMonth currentMonth = YearMonth.now();
        final YearMonth currentMonthMinusOne = currentMonth.minusMonths(1);
        final YearMonth currentMonthMinusTwo = currentMonth.minusMonths(2);
        final LocalDateTime currentMonthTime = currentMonth.atEndOfMonth().atStartOfDay();
        final LocalDateTime currentMonthMinusOneTime = currentMonthMinusOne.atEndOfMonth().atStartOfDay();
        final LocalDateTime currentMonthMinusTwoTime = currentMonthMinusTwo.atEndOfMonth().atStartOfDay();

        final LocalDateTime day2 = currentMonthMinusOne.atEndOfMonth().atStartOfDay();
        final LocalDateTime day1 = day2.minusDays(1);
        final LocalDateTime day4 = currentMonth.atEndOfMonth().atStartOfDay();
        final LocalDateTime day3 = day4.minusDays(1);

        // Insert points for 2 months
        upsertDailyBilledCost(day1, ENTITY_1, 1, 10);
        upsertDailyBilledCost(day2, ENTITY_1, 2, 20);
        upsertDailyBilledCost(day3, ENTITY_1, 3, 30);
        upsertDailyBilledCost(day4, ENTITY_1, 4, 40);
        upsertDailyBilledCost(day1, ENTITY_2, 100, 1000);
        upsertDailyBilledCost(day2, ENTITY_2, 200, 2000);
        upsertDailyBilledCost(day3, ENTITY_2, 300, 3000);
        upsertDailyBilledCost(day4, ENTITY_2, 400, 4000);

        // Set last rollup time to 2 months ago
        final LastRollupTimes lastRollupTimes = new LastRollupTimes();
        lastRollupTimes.setLastTimeByDay(
                TimeUtil.localDateTimeToMilli(currentMonthMinusTwoTime, clock));
        lastRollupTimes.setLastTimeUpdated(System.currentTimeMillis());
        rollupTimesStore.setLastRollupTimes(lastRollupTimes);

        // Rollup
        rollupBilledCostProcessor.process();

        // Verify
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_1, 3, 30);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_1, 7, 70);
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_2, 300, 3000);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_2, 700, 7000);

        // Update some of the earlier inserted points
        upsertDailyBilledCost(day2, ENTITY_1, 5, 50);
        upsertDailyBilledCost(day4, ENTITY_1, 6, 60);
        upsertDailyBilledCost(day2, ENTITY_2, 500, 5000);
        upsertDailyBilledCost(day4, ENTITY_2, 600, 6000);

        // Rollup again without modifying last rollup times (should have no effect)
        rollupBilledCostProcessor.process();

        // Verify that rollup didn't happen (because last rollup times were not reset)
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_1, 3, 30);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_1, 7, 70);
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_2, 300, 3000);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_2, 700, 7000);

        // Reset last rollup time to 2 months ago
        rollupTimesStore.setLastRollupTimes(lastRollupTimes);

        // Rollup again
        rollupBilledCostProcessor.process();

        // Verify updated monthly values
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_1, 6, 60);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_1, 9, 90);
        verifyMonthlyBilledCost(currentMonthMinusOneTime, ENTITY_2, 600, 6000);
        verifyMonthlyBilledCost(currentMonthTime, ENTITY_2, 900, 9000);
    }

    private void upsertDailyBilledCost(
            @Nonnull final LocalDateTime sampleTime,
            final long entityId,
            final double usageAmount,
            final double cost) {
        dslContext.insertInto(BILLED_COST_DAILY)
                .columns(BILLED_COST_DAILY.SAMPLE_TIME,
                        BILLED_COST_DAILY.ENTITY_ID,
                        BILLED_COST_DAILY.REGION_ID,
                        BILLED_COST_DAILY.ACCOUNT_ID,
                        BILLED_COST_DAILY.COST,
                        BILLED_COST_DAILY.SERVICE_PROVIDER_ID,
                        BILLED_COST_DAILY.PRICE_MODEL,
                        BILLED_COST_DAILY.COST_CATEGORY,
                        BILLED_COST_DAILY.USAGE_AMOUNT,
                        BILLED_COST_DAILY.UNIT,
                        BILLED_COST_DAILY.CURRENCY)
                .values(sampleTime,
                        entityId,
                        1L,
                        1L,
                        cost,
                        1L,
                        (short)1,
                        (short)1,
                        usageAmount,
                        (short)1,
                        (short)1)
                .onDuplicateKeyUpdate()
                .set(BILLED_COST_DAILY.USAGE_AMOUNT, usageAmount)
                .set(BILLED_COST_DAILY.COST, cost)
                .execute();
    }

    private void verifyMonthlyBilledCost(
            @Nonnull final LocalDateTime sampleTime,
            final long entityId,
            final double usageAmount,
            final double cost) {
        final List<Condition> conditions = ImmutableList.of(
                BILLED_COST_MONTHLY.ENTITY_ID.eq(entityId),
                BILLED_COST_MONTHLY.SAMPLE_TIME.eq(sampleTime),
                BILLED_COST_MONTHLY.USAGE_AMOUNT.eq(usageAmount),
                BILLED_COST_MONTHLY.COST.eq(cost));
        final int count = dslContext.fetchCount(BILLED_COST_MONTHLY, conditions);
        assertEquals(1, count);
    }
}
