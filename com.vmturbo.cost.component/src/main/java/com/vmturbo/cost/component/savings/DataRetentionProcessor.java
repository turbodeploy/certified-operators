package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.EntitySavingsRetentionConfig.DataRetentionSettings;

/**
 * Responsible for cleanup of old savings data from DB. Any data older than configured retention
 * period gets deleted periodically. This is to ensure that savings related DB tables don't grow
 * too big.
 */
public class DataRetentionProcessor {
    private static final Logger logger = LogManager.getLogger();

    /**
     * To delete savings stats table rows.
     */
    private final EntitySavingsStore savingsStore;

    /**
     * To delete audit data rows.
     */
    private final AuditLogWriter auditLogWriter;

    /**
     * Used to fetch retention config settings.
     */
    private final EntitySavingsRetentionConfig retentionConfig;

    /**
     * UTC Clock for timing.
     */
    private final Clock clock;

    /**
     * Epoch millis for last time when processor was run, will null on first time after start.
     */
    private Long lastTimeRan;

    /**
     * How often to run the processor, default 24 hours. Can be changed by config setting:
     * entitySavingsRetentionProcessorFrequencyHours
     */
    private final long runFrequencyHours;

    /**
     * Constant for number of millis in hour.
     */
    private static final long millisInHour = TimeUnit.HOURS.toMillis(1);

    DataRetentionProcessor(@Nonnull final EntitySavingsStore savingsStore,
            @Nonnull final AuditLogWriter auditLogWriter,
            @Nonnull final EntitySavingsRetentionConfig retentionConfig,
            @Nonnull final Clock clock,
            long runFrequencyHours) {
        this.savingsStore = savingsStore;
        this.auditLogWriter = auditLogWriter;
        this.retentionConfig = retentionConfig;
        this.clock = clock;
        this.runFrequencyHours = runFrequencyHours;
        this.lastTimeRan = null;
    }

    /**
     * Deletes old stats/audit records (older than configured duration) from DB tables.
     *
     * @param force Whether to force run it, irrespective of last run time. Mainly for testing.
     */
    public void process(boolean force) {
        long currentTimeMillis = clock.millis();

        if (shouldSkipRun(force, currentTimeMillis)) {
            // If we ran before, but ran too recently, less than 24 hours go, then skip for now.
            logger.trace("Last ran: {}, Current time: {}, Run frequency hours: {}",
                    this.lastTimeRan, currentTimeMillis, this.runFrequencyHours);
            return;
        }
        // Fetch latest settings values from SettingsManager.
        final DataRetentionSettings hourSettings = retentionConfig.fetchDataRetentionSettings();
        if (hourSettings != null) {
            logger.info("Starting {}. Frequency: {} hrs. Last ran: {}",
                    hourSettings,
                    this.runFrequencyHours,
                    this.lastTimeRan == null ? "Never" : new Date(this.lastTimeRan));

            // Delete audit records.
            long timestamp = currentTimeMillis - hourSettings.getAuditLogRetentionInHours() * millisInHour;
            int rowsDeleted = auditLogWriter.deleteOlderThan(timestamp);
            logger.info("Deleted {} audit records older than {}.", rowsDeleted,
                    new Date(timestamp));

            // Delete stats records.
            timestamp = currentTimeMillis - hourSettings.getHourlyStatsRetentionInHours() * millisInHour;
            rowsDeleted = savingsStore.deleteOlderThanHourly(timestamp);
            logger.info("Deleted {} hourly stats records older than {}.", rowsDeleted,
                    new Date(timestamp));

            timestamp = currentTimeMillis - hourSettings.getDailyStatsRetentionInHours() * millisInHour;
            rowsDeleted = savingsStore.deleteOlderThanDaily(timestamp);
            logger.info("Deleted {} daily stats records older than {}.", rowsDeleted,
                    new Date(timestamp));

            timestamp = currentTimeMillis - hourSettings.getMonthlyStatsRetentionInHours() * millisInHour;
            rowsDeleted = savingsStore.deleteOlderThanMonthly(timestamp);
            logger.info("Deleted {} monthly stats records older than {}.", rowsDeleted,
                    new Date(timestamp));
            this.lastTimeRan = currentTimeMillis;
        }
    }

    /**
     * Returns true if we should skip the retention processing this time, based on the configured
     * run frequency (default 24 hrs).
     *
     * @param force Whether to always run, for testing only.
     * @param currentTimeMillis Current time in epoch millis.
     * @return True if retention processing should be skipped this hour, false otherwise.
     */
    private boolean shouldSkipRun(boolean force, long currentTimeMillis) {
        if (force) {
            // If force flag is on, we always run, so no skipping.
            return false;
        }
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
