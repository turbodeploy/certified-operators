package com.vmturbo.components.common.utils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Utility class used to obtain the latest retention periods from the setting service.
 */
public class RetentionPeriodFetcher {
    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;

    /**
     * We don't have a setting to configure the minutes to retain for "LATEST" stats, so we
     * configure that outside of the setting framework.
     */
    private final int numRetainedMinutes;

    private final Object retentionPeriodUpdateLock = new Object();

    /**
     * We cache the retention periods for some amount of time before going back to the setting
     * component. This is because stat queries come in bursts (e.g. when a new UI page loads),
     * and retention settings change infrequently AND get applied infrequently (at best every
     * broadcast). Also, since it's just a few integers, the cost of the cache is minimal.
     */
    @GuardedBy("retentionPeriodUpdateLock")
    private RetentionPeriods cachedRetentionPeriods = null;

    @GuardedBy("retentionPeriodUpdateLock")
    private Instant lastUpdateRetentionPeriods = null;

    /**
     * How long to keep the cached retention periods for.
     */
    private final long updateRetentionIntervalSeconds;

    private final SettingServiceBlockingStub settingServiceClient;

    public RetentionPeriodFetcher(@Nonnull final Clock clock,
                               final int updateRetentionInterval,
                               final TimeUnit updateRetentionIntervalTimeUnit,
                               final int numRetainedMinutes,
                               @Nonnull final SettingServiceBlockingStub settingServiceBlockingStub) {
        this.clock = clock;
        this.updateRetentionIntervalSeconds = updateRetentionIntervalTimeUnit.toSeconds(updateRetentionInterval);
        this.numRetainedMinutes = numRetainedMinutes;
        this.settingServiceClient = settingServiceBlockingStub;

        initializeRetention();
    }


    private void initializeRetention() {
        synchronized (retentionPeriodUpdateLock) {
            this.cachedRetentionPeriods = new RetentionPeriodsImpl(numRetainedMinutes,
                    (int)GlobalSettingSpecs.StatsRetentionHours.createSettingSpec()
                            .getNumericSettingValueType()
                            .getDefault(),
                    (int)GlobalSettingSpecs.StatsRetentionDays.createSettingSpec()
                            .getNumericSettingValueType()
                            .getDefault(),
                    (int)GlobalSettingSpecs.StatsRetentionMonths.createSettingSpec()
                            .getNumericSettingValueType()
                            .getDefault());
            this.lastUpdateRetentionPeriods = Instant.ofEpochMilli(0);
        }
    }

    /**
     * Update the cached retention periods if they haven't been updated for longer than the
     * update interval.
     */
    private void updateRetentionIfExpired() {
        final Instant curTime = clock.instant();
        final Duration timeSinceUpdate = Duration.between(lastUpdateRetentionPeriods, curTime);
        if (timeSinceUpdate.getSeconds() < updateRetentionIntervalSeconds) {
            logger.debug("Skipping retention periods update because it's only been {} seconds, " +
                "and we update every {}.", timeSinceUpdate.getSeconds(), updateRetentionIntervalSeconds);
            return;
        }

        synchronized (retentionPeriodUpdateLock) {
            try {
                final GetMultipleGlobalSettingsRequest.Builder reqBuilder =
                    GetMultipleGlobalSettingsRequest.newBuilder();
                reqBuilder.addSettingSpecName(GlobalSettingSpecs.StatsRetentionHours.getSettingName());
                reqBuilder.addSettingSpecName(GlobalSettingSpecs.StatsRetentionDays.getSettingName());
                reqBuilder.addSettingSpecName(GlobalSettingSpecs.StatsRetentionMonths.getSettingName());

                final Map<String, Integer> retentionSettings = new HashMap<>();
                settingServiceClient.getMultipleGlobalSettings(reqBuilder.build())
                    .forEachRemaining(setting -> {
                        retentionSettings.put(setting.getSettingSpecName(),
                            (int) setting.getNumericSettingValue().getValue());
                    });

                // We replace the current cached periods with the values retrieved from the
                // setting service. If, for whatever reason, the setting service doesn't return
                // some values, we will keep the existing ones.
                final Integer newHours = retentionSettings.get(GlobalSettingSpecs.StatsRetentionHours.getSettingName());
                final Integer newDays = retentionSettings.get(GlobalSettingSpecs.StatsRetentionDays.getSettingName());
                final Integer newMonths = retentionSettings.get(GlobalSettingSpecs.StatsRetentionMonths.getSettingName());
                cachedRetentionPeriods = new RetentionPeriodsImpl(cachedRetentionPeriods.latestRetentionMinutes(),
                        newHours != null ? newHours : cachedRetentionPeriods.hourlyRetentionHours(),
                        newDays != null ? newDays : cachedRetentionPeriods.dailyRetentionDays(),
                        newMonths != null ? newMonths : cachedRetentionPeriods.monthlyRetentionMonths()
                        );
                // Update the time last, so that exceptions obtaining the settings don't prevent
                // cache updates the next time this method is called.
                lastUpdateRetentionPeriods = curTime;
            } catch (StatusRuntimeException e) {
                // Not updating anything - retain the most recent values.
                logger.error("Failed to update retention periods because the settings" +
                    " query failed with error: {}. Keeping existing periods: {}",
                    e.getLocalizedMessage(), cachedRetentionPeriods);
            }
        }
    }

    /**
     * Get the retention periods. This may make a remote call to the setting service to check for
     * any updates to the retention settings.
     *
     * @return A {@link RetentionPeriods} object describing the retention periods.
     */
    @Nonnull
    public RetentionPeriods getRetentionPeriods() {
        synchronized (retentionPeriodUpdateLock) {
            updateRetentionIfExpired();
            return cachedRetentionPeriods;
        }
    }

    /**
     * The retention periods for stats in XL.
     */
    public interface RetentionPeriods {
        /**
         * Instance of {@link RetentionPeriods} contains highest numbers for every unit, but do not
         * cross the border of units, i.e. border value for minutes is 60, because 60 minutes means
         * 1 hour and 0 minutes.
         */
        RetentionPeriods BOUNDARY_RETENTION_PERIODS =
                        new RetentionPeriodsImpl(getBoundary(ChronoUnit.HOURS, Duration::toMinutes),
                                        getBoundary(ChronoUnit.DAYS, Duration::toHours),
                                        getBoundary(ChronoUnit.MONTHS, Duration::toDays),
                                        (int)(Period.ofYears(1).toTotalMonths() - 1));

        /**
         * Calculates boundary for specified temporal unit into units specified by a converter and
         * subtracting one from result value.
         *
         * @param unit source unit which needs to be converted.
         * @param converter function applying which we will be able to convert
         *                 source unit.
         * @return amount of target units equal to 1 source unit, but without one target
         *                 unit.
         */
        static int getBoundary(@Nonnull TemporalUnit unit,
                        @Nonnull Function<Duration, Long> converter) {
            return (int)(converter.apply(unit.getDuration()) - 1);
        }

        /**
         * How many minutes to retain unaggregated stats for.
         */
        int latestRetentionMinutes();

        /**
         * How many hours to retain hourly-aggregated stats for.
         */
        int hourlyRetentionHours();

        /**
         * How many days to retain daily-aggregated stats for.
         */
        int dailyRetentionDays();

        /**
         * How many months to retain monthly-aggregated stats for.
         */
        int monthlyRetentionMonths();
    }

    /**
     * Immutable implementation of {@link RetentionPeriods}.
     */
    @Immutable
    private static class RetentionPeriodsImpl implements RetentionPeriods {
        /**
         * How many minutes to retain unaggregated stats for.
         */
        private final int latestRetentionMinutes;

        /**
         * How many hours to retain hourly-aggregated stats for.
         */
        private final int hourlyRetentionHours;

        /**
         * How many days to retain daily-aggregated stats for.
         */
        private final int dailyRetentionDays;

        /**
         * How many months to retain monthly-aggregated stats for.
         */
        private final int monthlyRetentionMonths;

        RetentionPeriodsImpl(int latestRetentionMinutes, int hourlyRetentionHours,
                int dailyRetentionDays, int monthlyRetentionMonths) {
            this.latestRetentionMinutes = latestRetentionMinutes;
            this.hourlyRetentionHours = hourlyRetentionHours;
            this.dailyRetentionDays = dailyRetentionDays;
            this.monthlyRetentionMonths = monthlyRetentionMonths;
        }

        @Override
        public int latestRetentionMinutes() {
            return latestRetentionMinutes;
        }

        @Override
        public int hourlyRetentionHours() {
            return hourlyRetentionHours;
        }

        @Override
        public int dailyRetentionDays() {
            return dailyRetentionDays;
        }

        @Override
        public int monthlyRetentionMonths() {
            return monthlyRetentionMonths;
        }
    }
}
