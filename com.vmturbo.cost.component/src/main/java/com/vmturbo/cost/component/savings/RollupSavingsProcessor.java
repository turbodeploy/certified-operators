package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;
import com.vmturbo.cost.component.savings.EntitySavingsStore.RollupDurationType;
import com.vmturbo.cost.component.savings.EntitySavingsStore.RollupTimeInfo;

/**
 * Responsible for rolling hourly savings data into daily and monthly tables. This gets called
 * every hour, after the hourly savings data has been processed.
 */
public class RollupSavingsProcessor {
    private final Logger logger = LogManager.getLogger();

    /**
     * DB interface to call rollup procedures.
     */
    private final EntitySavingsStore savingsStore;

    /**
     * UTC clock from config.
     */
    private final Clock clock;

    /**
     * Info about last rollup times read from DB.
     */
    private LastRollupTimes lastRollupTimes;

    /**
     * Creates a new instance. Initialized from config.
     *
     * @param savingsStore DB store.
     * @param clock UTC clock.
     */
    public RollupSavingsProcessor(@Nonnull final EntitySavingsStore savingsStore,
            @Nonnull final Clock clock) {
        this.savingsStore = savingsStore;
        this.clock = clock;
    }

    /**
     * Checks if any rollup needs to be done for the hourly stats data that has just been written
     * to DB. If needed, performs daily/monthly rollup of that data set.
     *
     * @param hourlyStatsTimes Hourly stats data written to DB in this cycle.
     */
    void process(@Nonnull final List<Long> hourlyStatsTimes) {
        if (hourlyStatsTimes.isEmpty()) {
            logger.info("Entity Savings rollup: Nothing to rollup.");
            return;
        }
        // Check last time updated metadata.
        if (lastRollupTimes == null) {
            logger.trace("Reading first time rollup last times from DB...");
            lastRollupTimes = savingsStore.getLastRollupTimes();
            logger.trace("Last times: {}", lastRollupTimes);
        }
        List<RollupTimeInfo> nextRollupTimes = checkRollupRequired(hourlyStatsTimes,
                lastRollupTimes, clock);
        logger.debug("Entity Savings rollup: {}", nextRollupTimes.stream()
                .map(RollupTimeInfo::toString)
                .collect(Collectors.joining(", ")));

        if (nextRollupTimes.isEmpty()) {
            return;
        }
        logger.info("Performing {} Entity Savings rollup this time.",
                nextRollupTimes.size());
        for (RollupTimeInfo newRollupTime : nextRollupTimes) {
            if (newRollupTime.isDaily()) {
                if (newRollupTime.fromTime() > lastRollupTimes.getLastTimeByHour()) {
                    lastRollupTimes.setLastTimeByHour(newRollupTime.fromTime());
                }
            } else if (newRollupTime.isMonthly()) {
                if (newRollupTime.fromTime() > lastRollupTimes.getLastTimeByDay()) {
                    lastRollupTimes.setLastTimeByDay(newRollupTime.fromTime());
                }
                if (newRollupTime.toTime() > lastRollupTimes.getLastTimeByMonth()) {
                    lastRollupTimes.setLastTimeByMonth(newRollupTime.toTime());
                }
            }
            // Call the stored proc to do the rollup.
            savingsStore.performRollup(newRollupTime);
        }
        lastRollupTimes.setLastTimeUpdated(Instant.now(clock).toEpochMilli());
        savingsStore.setLastRollupTimes(lastRollupTimes);
    }

    /**
     * Util function to check if any rollup is required to be done this time, based on the latest
     * stats data that has just been written to DB.
     *
     * @param hourlyStatsTimes Times (all at hour mark) for stats data written to DB this cycle.
     * @param rollupTimes Last rollup time metadata read from DB.
     * @param clock UTC clock.
     * @return List of times for which daily or monthly rollup needs to be done.
     */
    @Nonnull
    @VisibleForTesting
    static List<RollupTimeInfo> checkRollupRequired(@Nonnull final List<Long> hourlyStatsTimes,
            @Nonnull final LastRollupTimes rollupTimes, @Nonnull final Clock clock) {
        long lastHourlyRolledUp = rollupTimes.getLastTimeByHour();

        final List<RollupTimeInfo> nextRollupTimes = new ArrayList<>();
        for (Long eachHourlyTime : hourlyStatsTimes) {
            // Skip any times that we have already done hourly rollup for.
            // This should not happen, but just in case.
            if (eachHourlyTime <= lastHourlyRolledUp) {
                continue;
            }
            Instant eachInstant = Instant.ofEpochMilli(eachHourlyTime);
            LocalDateTime eachDateUtc = eachInstant.atZone(clock.getZone()).toLocalDateTime();

            // This is a newer time for which we haven't yet done rollup.
            // From an hourly time like 2/18 11:00:00 AM, get day start 2/18 00:00:00 (12 AM).
            LocalDateTime startOfDay = eachDateUtc.toLocalDate().atStartOfDay();
            long dayStartTime = TimeUtil.localDateTimeToMilli(startOfDay, clock);

            nextRollupTimes.add(new RollupTimeInfo(RollupDurationType.DAILY, eachHourlyTime,
                    dayStartTime));

            // By now, we have rolled up current hourly data into daily tables.
            // Check if it is the first hour of the day.
            if (eachHourlyTime == dayStartTime) {
                // The previous day is now 'done', so we can roll it up to monthly.
                LocalDateTime dayBefore = startOfDay.minusDays(1L);
                long previousDayStartTime = TimeUtil.localDateTimeToMilli(dayBefore, clock);

                long monthEndTime = SavingsUtil.getMonthEndTime(dayBefore, clock);
                nextRollupTimes.add(new RollupTimeInfo(RollupDurationType.MONTHLY,
                        previousDayStartTime, monthEndTime));
            }
        }
        return nextRollupTimes;
    }
}
