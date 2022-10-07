package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;

/**
 * Responsible for cleanup of old savings data from DB. Any data older than configured retention
 * period gets deleted periodically. This is to ensure that savings related DB tables don't grow
 * too big.
 */
public class BillBasedDataRetentionProcessor extends DataRetentionProcessor {
    /**
     * For logging.
     */
    protected final Logger logger = LogManager.getLogger();

    /**
     * Number of Hours(default to 365*24) to retain before the data gets deleted from the daily table.
     */
    private final long billBasedDailyStatsRetentionInHours;

    /**
     * Constructor for bill based data retention processor.
     *
     * @param savingsStore savings store
     * @param clock clock
     * @param runFrequencyHours run frequency in hours
     * @param billBasedDailyStatsRetentionInHours Retention hours setting from yaml.
     */
    public BillBasedDataRetentionProcessor(@Nonnull final EntitySavingsStore savingsStore,
                           @Nonnull final Clock clock,
                           long runFrequencyHours,
                           long billBasedDailyStatsRetentionInHours) {
        super(savingsStore, clock, runFrequencyHours);
        this.billBasedDailyStatsRetentionInHours = billBasedDailyStatsRetentionInHours;
    }

    @Override
    public void process() {
        long currentTimeMillis = clock.millis();

        if (shouldSkipRun(currentTimeMillis)) {
            // If we ran before, but ran too recently, less than 24 hours go, then skip for now.
            logger.trace("Last ran: {}, Current time: {}, Run frequency hours: {}",
                    this.lastTimeRan, currentTimeMillis, this.runFrequencyHours);
            return;
        }
        long timestamp;
        int rowsDeleted;

        final Stopwatch deletionWatch = Stopwatch.createStarted();
        final String lastRunDisplayTime = this.lastTimeRan == null ? "Never"
                : SavingsUtil.getLocalDateTime(this.lastTimeRan).toString();

        logger.info("Starting cleanup for bill-based savings tables. Retention hrs: {}, "
                        + "Frequency: {} hrs, Last ran: {}", billBasedDailyStatsRetentionInHours,
                this.runFrequencyHours, lastRunDisplayTime);
        timestamp = currentTimeMillis - billBasedDailyStatsRetentionInHours * millisInHour;
        // Make sure to delete from new bill based daily stats table.
        rowsDeleted = savingsStore.deleteOlderThanDaily(timestamp, true);
        logger.info("Deleted {} daily stats (bill-based) records older than {}.",
                rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));
        this.lastTimeRan = currentTimeMillis;

        logger.info("Completed cleanup of bill-based savings tables in {} ms.",
                deletionWatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
