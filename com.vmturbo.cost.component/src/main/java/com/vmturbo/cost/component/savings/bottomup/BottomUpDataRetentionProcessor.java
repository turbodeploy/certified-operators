package com.vmturbo.cost.component.savings.bottomup;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.savings.DataRetentionProcessor;
import com.vmturbo.cost.component.savings.SavingsUtil;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig.DataRetentionSettings;

/**
 * Responsible for cleanup of old savings data from bottom-up savings DB tables.
 * Any data older than configured retention period gets deleted periodically. This is to ensure
 * that savings related DB tables don't grow too big.
 */
public class BottomUpDataRetentionProcessor extends DataRetentionProcessor {
    /**
     * For logging.
     */
    protected final Logger logger = LogManager.getLogger();

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
     * Events journal, if available (when events are persisted to DB).
     */
    @Nullable
    private final EntityEventsJournal persistentEventsJournal;

    /**
     * Constructor for bottom-up data retention processor.
     *
     * @param savingsStore savings store
     * @param auditLogWriter audit log writer
     * @param retentionConfig retention config
     * @param clock clock
     * @param runFrequencyHours run frequency in hours
     * @param savingsEventJournal savings event journal
     */
    public BottomUpDataRetentionProcessor(@Nonnull final EntitySavingsStore savingsStore,
                           @Nullable final AuditLogWriter auditLogWriter,
                           @Nonnull final EntitySavingsRetentionConfig retentionConfig,
                           @Nonnull final Clock clock,
                           long runFrequencyHours,
                           @Nullable final EntityEventsJournal savingsEventJournal) {
        super(savingsStore, clock, runFrequencyHours);
        this.auditLogWriter = auditLogWriter;
        this.retentionConfig = retentionConfig;
        this.persistentEventsJournal = savingsEventJournal;
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
        // Bottom-up stats table entry cleanup.
        // Fetch latest settings values from SettingsManager.
        final DataRetentionSettings  hourSettings = retentionConfig.fetchDataRetentionSettings();
        if (hourSettings == null) {
            logger.warn("Could not fetch latest retention settings. Skipping processing.");
            return;
        }
        final String lastRunDisplayTime = this.lastTimeRan == null ? "Never"
                : SavingsUtil.getLocalDateTime(this.lastTimeRan).toString();
        logger.info("Starting cleanup for bottom-up savings tables. Settings: {}, "
                + "Frequency: {} hrs, Last ran: {}",
                hourSettings, this.runFrequencyHours, lastRunDisplayTime);

        // Delete audit records, but only if enabled.
        if (auditLogWriter != null && auditLogWriter.isEnabled()) {
            timestamp = currentTimeMillis - hourSettings.getAuditLogRetentionInHours() * millisInHour;
            rowsDeleted = auditLogWriter.deleteOlderThan(timestamp);
            logger.info("Deleted {} audit records older than {}.", rowsDeleted,
                    SavingsUtil.getLocalDateTime(timestamp));
        }

        // Delete event journal records if events are being persisted to DB.
        if (persistentEventsJournal != null) {
            timestamp = currentTimeMillis - hourSettings.getEventsRetentionInHours() * millisInHour;
            rowsDeleted = persistentEventsJournal.purgeEventsOlderThan(timestamp);
            logger.info("Deleted {} event records older than {}.", rowsDeleted,
                    SavingsUtil.getLocalDateTime(timestamp));
        }

        // Delete stats records.
        timestamp = currentTimeMillis - hourSettings.getHourlyStatsRetentionInHours() * millisInHour;
        rowsDeleted = savingsStore.deleteOlderThanHourly(timestamp);
        logger.info("Deleted {} hourly stats records older than {}.", rowsDeleted,
                SavingsUtil.getLocalDateTime(timestamp));

        long dailyStatsRetentionInHours = hourSettings.getDailyStatsRetentionInHours();
        timestamp = currentTimeMillis - dailyStatsRetentionInHours * millisInHour;
        // Call the bottom-up daily stats cleanup.
        rowsDeleted = savingsStore.deleteOlderThanDaily(timestamp);
        logger.info("Deleted {} daily stats records older than {}.", rowsDeleted,
                SavingsUtil.getLocalDateTime(timestamp));

        timestamp = currentTimeMillis - hourSettings.getMonthlyStatsRetentionInHours() * millisInHour;
        rowsDeleted = savingsStore.deleteOlderThanMonthly(timestamp);
        logger.info("Deleted {} monthly stats records older than {}.", rowsDeleted,
                SavingsUtil.getLocalDateTime(timestamp));
        this.lastTimeRan = currentTimeMillis;

        logger.info("Completed cleanup of bottom-up savings tables in {} ms.",
                deletionWatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
