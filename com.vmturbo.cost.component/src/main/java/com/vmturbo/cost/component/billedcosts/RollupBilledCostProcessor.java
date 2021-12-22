package com.vmturbo.cost.component.billedcosts;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupTimesStore;

/**
 * Class to do roll-ups for billed cost tables.
 */
public class RollupBilledCostProcessor {

    /**
     * Maximum number of months in the past that will be rolled up.
     */
    private static final int MAX_MONTHS_TO_ROLLUP = 3;

    private final Logger logger = LogManager.getLogger();

    private final BilledCostStore billedCostStore;
    private final RollupTimesStore rollupTimesStore;

    private final Clock clock;

    /**
     * Create new instance of {@code RollupBilledCostProcessor}.
     *
     * @param billedCostStore Billed costs store.
     * @param rollupTimesStore Rollup times store for billed costs.
     * @param clock Clock.
     */
    public RollupBilledCostProcessor(
            @Nonnull final BilledCostStore billedCostStore,
            @Nonnull final RollupTimesStore rollupTimesStore,
            @Nonnull final Clock clock) {
        this.billedCostStore = billedCostStore;
        this.rollupTimesStore = rollupTimesStore;
        this.clock = clock;
    }

    /**
     * Start rollup.
     */
    public void process() {
        try {
            final LastRollupTimes lastRollupTimes = rollupTimesStore.getLastRollupTimes();
            logger.trace("Last billed cost rollup times: {}", lastRollupTimes);

            final List<YearMonth> monthsToRollup = new ArrayList<>();
            final YearMonth currentMonth = YearMonth.now(clock);
            if (lastRollupTimes.hasLastTimeByDay()) {
                final long lastTimeByDay = lastRollupTimes.getLastTimeByDay();
                final YearMonth lastMonth = YearMonth.from(toLocalDateTime(lastTimeByDay));
                if (!lastMonth.isBefore(currentMonth)) {
                    return;
                }
                // Get all months between currentMonth and lastMonth
                YearMonth monthToRollup = currentMonth;
                do {
                    monthsToRollup.add(monthToRollup);
                    monthToRollup = monthToRollup.minusMonths(1);
                } while (lastMonth.isBefore(monthToRollup)
                        && monthsToRollup.size() < MAX_MONTHS_TO_ROLLUP);
            } else {
                // If it is the first rollup calculation, calculate for the current month
                monthsToRollup.add(currentMonth);
            }

            // We should roll up starting from the earliest month, so reversing the order of monthsToRollup
            Collections.reverse(monthsToRollup);
            for (final YearMonth monthToRollup : monthsToRollup) {
                final LocalDateTime toTime = monthToRollup.atEndOfMonth().atStartOfDay();
                final LocalDateTime fromTimeStart = monthToRollup.atDay(1).atStartOfDay();
                final LocalDateTime fromTimeEnd = monthToRollup.atEndOfMonth().plusDays(1)
                        .atStartOfDay();
                billedCostStore.performRollup(RollupDurationType.DAILY, toTime, fromTimeStart,
                        fromTimeEnd);

                // Set last time for monthly rollup to the last day of the month, e.g. '2021-02-28 00:00:00'
                lastRollupTimes.setLastTimeByDay(TimeUtil.localDateTimeToMilli(toTime, clock));
                lastRollupTimes.setLastTimeUpdated(System.currentTimeMillis());
                rollupTimesStore.setLastRollupTimes(lastRollupTimes);
            }
        } catch (Exception ex) {
            logger.error("Error in billed cost rollup", ex);
        }
    }

    private LocalDateTime toLocalDateTime(final long millis) {
        return Instant.ofEpochMilli(millis).atZone(clock.getZone()).toLocalDateTime();
    }
}
