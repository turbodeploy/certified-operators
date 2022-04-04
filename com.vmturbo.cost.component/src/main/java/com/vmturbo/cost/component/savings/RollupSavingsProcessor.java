package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupTimeInfo;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;

/**
 * Responsible for rolling hourly savings data into daily and monthly tables. This gets called
 * every hour, after the hourly savings data has been processed.
 */
public class RollupSavingsProcessor {
    private final Logger logger = LogManager.getLogger();

    /**
     * DB interface to call rollup procedures.
     */
    private final EntitySavingsStore<?> savingsStore;

    private final RollupTimesStore rollupTimesStore;

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
     * @param rollupTimesStore Last rollup times store.
     * @param clock UTC clock.
     */
    public RollupSavingsProcessor(
            @Nonnull final EntitySavingsStore<?> savingsStore,
            @Nonnull final RollupTimesStore rollupTimesStore,
            @Nonnull final Clock clock) {
        this.savingsStore = savingsStore;
        this.rollupTimesStore = rollupTimesStore;
        this.clock = clock;
    }

    /**
     * Checks if any rollup needs to be done for the hourly stats data that has just been written
     * to DB. If needed, performs daily/monthly rollup of that data set.
     *
     * @param durationType If Hourly, then data is rolled up to daily and monthly tables.
     *          If Daily, then data is rolled up to monthly table.
     * @param statsTimes Hourly stats data written to DB in this cycle.
     */
    public void process(RollupDurationType durationType, @Nonnull final List<Long> statsTimes) {
        if (statsTimes.isEmpty()) {
            logger.info("Entity Savings rollup: Nothing to rollup.");
            return;
        }
        // Check last time updated metadata.
        if (lastRollupTimes == null) {
            logger.trace("Reading first time rollup last times from DB...");
            lastRollupTimes = rollupTimesStore.getLastRollupTimes();
            logger.trace("Last times: {}", lastRollupTimes);
        }
        List<RollupTimeInfo> nextRollupTimes = checkRollupRequired(durationType, statsTimes,
                lastRollupTimes, clock);
        logger.debug("Entity Savings rollup: {}", () -> nextRollupTimes.stream()
                .map(RollupTimeInfo::toString)
                .collect(Collectors.joining(", ")));

        if (nextRollupTimes.isEmpty()) {
            return;
        }
        logger.info("Performing {} Entity Savings rollup this time.",
                nextRollupTimes.size());
        Map<Long, List<Long>> dailiesToDo = new LinkedHashMap<>();
        Map<Long, List<Long>> monthliesToDo = new LinkedHashMap<>();
        for (RollupTimeInfo newRollupTime : nextRollupTimes) {
            if (newRollupTime.isDaily()) {
                if (newRollupTime.fromTime() > lastRollupTimes.getLastTimeByHour()) {
                    lastRollupTimes.setLastTimeByHour(newRollupTime.fromTime());
                }
                dailiesToDo.computeIfAbsent(newRollupTime.toTime(), _t -> new ArrayList<>())
                        .add(newRollupTime.fromTime());
            } else if (newRollupTime.isMonthly()) {
                if (newRollupTime.fromTime() > lastRollupTimes.getLastTimeByDay()) {
                    lastRollupTimes.setLastTimeByDay(newRollupTime.fromTime());
                }
                if (newRollupTime.toTime() > lastRollupTimes.getLastTimeByMonth()) {
                    lastRollupTimes.setLastTimeByMonth(newRollupTime.toTime());
                }
                monthliesToDo.computeIfAbsent(newRollupTime.toTime(), _t -> new ArrayList<>())
                        .add(newRollupTime.fromTime());
            }
        }
        dailiesToDo.forEach((toTime, fromTimes) ->
                savingsStore.performRollup(RollupDurationType.DAILY, toTime, fromTimes));
        monthliesToDo.forEach((toTime, fromTimes) ->
                savingsStore.performRollup(RollupDurationType.MONTHLY, toTime, fromTimes));
        lastRollupTimes.setLastTimeUpdated(Instant.now(clock).toEpochMilli());
        rollupTimesStore.setLastRollupTimes(lastRollupTimes);
    }

    /**
     * Util function to check if any rollup is required to be done this time, based on the latest
     * stats data that has just been written to DB.
     *
     * @param durationType Whether to roll up to monthly only (if daily), or both daily and
     *      monthly (if hourly).
     * @param statsTimes Times (all at mark or day mark) for stats data written to DB this cycle.
     * @param rollupTimes Last rollup time metadata read from DB.
     * @param clock UTC clock.
     * @return List of times for which daily or monthly rollup needs to be done.
     */
    @Nonnull
    @VisibleForTesting
    public static List<RollupTimeInfo> checkRollupRequired(RollupDurationType durationType,
            @Nonnull final List<Long> statsTimes, @Nonnull final LastRollupTimes rollupTimes,
            @Nonnull final Clock clock) {
        boolean isHourly = durationType == RollupDurationType.HOURLY;
        long lastRolledUp = isHourly ? rollupTimes.getLastTimeByHour() : rollupTimes.getLastTimeByDay();

        final List<RollupTimeInfo> nextRollupTimes = new ArrayList<>();
        for (Long eachTime : statsTimes) {
            // Skip any times that we have already done hourly rollup for.
            // This should not happen, but just in case.
            if (eachTime <= lastRolledUp) {
                continue;
            }
            // This is a newer time for which we haven't yet done rollup.
            // From an hourly time like 2/18 11:00:00 AM, get day start 2/18 00:00:00 (12 AM).
            LocalDateTime startOfDay = SavingsUtil.getDayStartTime(eachTime, clock);
            long dayStartTime = TimeUtil.localDateTimeToMilli(startOfDay, clock);

            if (isHourly) {
                nextRollupTimes.add(
                        new RollupTimeInfo(RollupDurationType.DAILY, eachTime, dayStartTime));
            }

            // By now, we have rolled up current hourly data into daily tables.
            // Check if it is the first hour of the day.
            // If rolling up daily -> monthly, then do it for all input day times.
            if (eachTime == dayStartTime) {
                // The previous day is now 'done', so we can roll it up to monthly.
                LocalDateTime dayBefore = startOfDay.minusDays(1L);
                long previousDayStartTime = TimeUtil.localDateTimeToMilli(dayBefore, clock);

                long monthEndTime = SavingsUtil.getMonthEndTime(dayBefore, clock);
                nextRollupTimes.add(new RollupTimeInfo(RollupDurationType.MONTHLY,
                        previousDayStartTime, monthEndTime));
            } else if (!isHourly) {
                nextRollupTimes.add(new RollupTimeInfo(RollupDurationType.MONTHLY,
                        dayStartTime, SavingsUtil.getMonthEndTime(startOfDay, clock)));
            }
        }
        return nextRollupTimes;
    }
}
