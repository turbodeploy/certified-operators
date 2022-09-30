package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.bottomup.AuditLogWriter;
import com.vmturbo.cost.component.savings.bottomup.EntityEventsJournal;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig.DataRetentionSettings;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;

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
    @Nullable
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
     * Number of Hours(default to 365*24) to retain before the data gets deleted from the daily table.
     */
    private final long billBasedDailyStatsRetentionInHours;

    /**
     * Events journal, if available (when events are persisted to DB).
     */
    @Nullable
    private final EntityEventsJournal persistentEventsJournal;

    /**
     * Constant for number of millis in hour.
     */
    private static final long millisInHour = TimeUnit.HOURS.toMillis(1);

    /**
     * Whether bill based savings is enabled.
     */
    private final boolean isBillBasedSavings;

    /**
     * Constructor.
     *
     * @param savingsStore savings store
     * @param auditLogWriter audit log writer
     * @param retentionConfig retention config
     * @param clock clock
     * @param runFrequencyHours run frequency in hours
     * @param savingsEventJournal savings event journal
     * @param isBillBasedSavings Whether bill based savings is enabled.
     */
    public DataRetentionProcessor(@Nonnull final EntitySavingsStore savingsStore,
                           @Nullable final AuditLogWriter auditLogWriter,
                           @Nonnull final EntitySavingsRetentionConfig retentionConfig,
                           @Nonnull final Clock clock,
                           long runFrequencyHours,
                           @Nullable final EntityEventsJournal savingsEventJournal,
                           long billBasedDailyStatsRetentionInHours,
                           boolean isBillBasedSavings) {
        this.savingsStore = savingsStore;
        this.auditLogWriter = auditLogWriter;
        this.retentionConfig = retentionConfig;
        this.clock = clock;
        this.runFrequencyHours = runFrequencyHours;
        this.lastTimeRan = null;
        this.persistentEventsJournal = savingsEventJournal;
        this.billBasedDailyStatsRetentionInHours = billBasedDailyStatsRetentionInHours;
        this.isBillBasedSavings = isBillBasedSavings;
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
        long timestamp;
        int rowsDeleted;

        final Stopwatch deletionWatch = Stopwatch.createStarted();
        final String lastRunDisplayTime = this.lastTimeRan == null ? "Never"
                : SavingsUtil.getLocalDateTime(this.lastTimeRan).toString();
        // Bottom-up stats table entry cleanup.
        // Fetch latest settings values from SettingsManager.
        final DataRetentionSettings  hourSettings = retentionConfig.fetchDataRetentionSettings();
        if (hourSettings == null) {
            logger.warn("Could not fetch latest retention settings. Skipping processing.");
            return;
        }
        logger.info("Starting cleanup for bottom-up savings tables. Settings: {}, "
                + "Frequency: {} hrs, Last ran: {}",
                hourSettings, this.runFrequencyHours,
                this.lastTimeRan == null ? "Never" : SavingsUtil.getLocalDateTime(this.lastTimeRan));

        // Delete audit records, but only if enabled.
        if (auditLogWriter != null && auditLogWriter.isEnabled()) {
            timestamp = currentTimeMillis - hourSettings.getAuditLogRetentionInHours() * millisInHour;
            rowsDeleted = auditLogWriter.deleteOlderThan(timestamp);
            logger.info("Deleted {} audit records older than {}.", rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));
        }

        // Delete event journal records if events are being persisted to DB.
        if (persistentEventsJournal != null) {
            timestamp = currentTimeMillis - hourSettings.getEventsRetentionInHours() * millisInHour;
            rowsDeleted = persistentEventsJournal.purgeEventsOlderThan(timestamp);
            logger.info("Deleted {} event records older than {}.", rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));
        }

        // Delete stats records.
        timestamp = currentTimeMillis - hourSettings.getHourlyStatsRetentionInHours() * millisInHour;
        rowsDeleted = savingsStore.deleteOlderThanHourly(timestamp);
        logger.info("Deleted {} hourly stats records older than {}.", rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));

        long dailyStatsRetentionInHours = hourSettings.getDailyStatsRetentionInHours();
        timestamp = currentTimeMillis - dailyStatsRetentionInHours * millisInHour;
        rowsDeleted = savingsStore.deleteOlderThanDaily(timestamp);
        logger.info("Deleted {} daily stats records older than {}.", rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));

        timestamp = currentTimeMillis - hourSettings.getMonthlyStatsRetentionInHours() * millisInHour;
        rowsDeleted = savingsStore.deleteOlderThanMonthly(timestamp);
        logger.info("Deleted {} monthly stats records older than {}.", rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));
        this.lastTimeRan = currentTimeMillis;

        if (isBillBasedSavings) {
            logger.info("Starting cleanup for bill-based savings tables. Retention hrs: {}, "
                            + "Frequency: {} hrs, Last ran: {}", billBasedDailyStatsRetentionInHours,
                    this.runFrequencyHours, lastRunDisplayTime);
            timestamp = currentTimeMillis - billBasedDailyStatsRetentionInHours * millisInHour;
            rowsDeleted = savingsStore.deleteOlderThanDaily(timestamp);
            logger.info("Deleted {} daily stats (bill-based) records older than {}.",
                    rowsDeleted, SavingsUtil.getLocalDateTime(timestamp));
        }

        logger.info("Completed cleanup of savings tables in {} ms.",
                deletionWatch.elapsed(TimeUnit.MILLISECONDS));
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
