package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;

/**
 * Parent of both bill-based and bottom-up retention processors, has some common methods.
 */
public abstract class DataRetentionProcessor {
    /**
     * To delete savings stats table rows.
     */
    protected final EntitySavingsStore savingsStore;

    /**
     * UTC Clock for timing.
     */
    protected final Clock clock;

    /**
     * Epoch millis for last time when processor was run, will null on first time after start.
     */
    protected Long lastTimeRan;

    /**
     * How often to run the processor, default 24 hours. Can be changed by config setting:
     * entitySavingsRetentionProcessorFrequencyHours
     */
    protected final long runFrequencyHours;

    /**
     * Constant for number of millis in hour.
     */
    protected static final long millisInHour = TimeUnit.HOURS.toMillis(1);

    /**
     * Constructor.
     *
     * @param savingsStore savings store
     * @param clock clock
     * @param runFrequencyHours run frequency in hours
     */
    protected DataRetentionProcessor(@Nonnull final EntitySavingsStore savingsStore,
                           @Nonnull final Clock clock,
                           long runFrequencyHours) {
        this.savingsStore = savingsStore;
        this.clock = clock;
        this.runFrequencyHours = runFrequencyHours;
        this.lastTimeRan = null;
    }

    /**
     * Deletes old stats/audit records (older than configured duration) from DB tables.
     */
    public abstract void process();

    /**
     * Returns true if we should skip the retention processing this time, based on the configured
     * run frequency (default 24 hrs).
     *
     * @param currentTimeMillis Current time in epoch millis.
     * @return True if retention processing should be skipped this hour, false otherwise.
     */
    protected boolean shouldSkipRun(long currentTimeMillis) {
        if (lastTimeRan == null) {
            // If we never ran before (first time after cost pod startup), then always run.
            return false;
        }
        // Strip of min/secs/millis from times, truncate and check hour difference.
        // From a time like 'June 7, 2021 2:15:49 PM', get truncated time: 'June 7, 2021 2:00:00 PM'
        long truncatedLastTime = TimeUtil.localDateTimeToMilli(SavingsUtil.getLocalDateTime(
                this.lastTimeRan, clock).truncatedTo(ChronoUnit.HOURS), clock);
        long truncatedCurrentTime = TimeUtil.localDateTimeToMilli(SavingsUtil.getLocalDateTime(
                currentTimeMillis, clock).truncatedTo(ChronoUnit.HOURS), clock);
        return (truncatedCurrentTime - truncatedLastTime) < (this.runFrequencyHours * millisInHour);
    }
}
