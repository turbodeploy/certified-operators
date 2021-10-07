package com.vmturbo.cost.component.savings;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Manages action retention configuration values.
 */
public class EntitySavingsRetentionConfig {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    private final SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient;
    private Long actionRetentionMs;
    private Long volumeDeleteRetentionMs;
    private Long previousActionRetentionMs;
    private Long previousVolumeDeleteRetentionMs;
    private final long savingsAuditLogRetentionHours;

    /**
     * Constructor.
     *
     * @param settingServiceClient settings service client.
     * @param auditLogRetentionHours How long to retain audit events.
     */
    public EntitySavingsRetentionConfig(SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient,
            long auditLogRetentionHours) {
        this.settingServiceClient = settingServiceClient;
        this.actionRetentionMs = getDefaultSetting(GlobalSettingSpecs.CloudSavingsActionRetention);
        this.volumeDeleteRetentionMs =
                getDefaultSetting(GlobalSettingSpecs.CloudSavingsDeleteVolumeRetention);
        previousActionRetentionMs = null;
        previousVolumeDeleteRetentionMs = null;
        this.savingsAuditLogRetentionHours = auditLogRetentionHours;
    }

    /**
     * Query the settings manager for a global setting.  The global setting in the UI is expressed
     * in months, but internally, milliseconds are used, so this returns the configured value in
     * milliseconds.  A month is assumed to have a duration of 730 hours.
     *
     * @param spec the {@link GlobalSettingSpecs} to query.
     * @return the global setting value expressed in milliseconds.  If the configuration value
     * isn't explicitly set, the previously configured value will be returned instead.
     */
    private long getSetting(GlobalSettingSpecs spec) {
        String settingName = spec.getSettingName();

        final SettingProto.GetSingleGlobalSettingRequest request =
                SettingProto.GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(settingName)
                        .build();
        SettingProto.Setting setting;
        try {
            setting = settingServiceClient.getGlobalSetting(request).getSetting();
        } catch (io.grpc.StatusRuntimeException e) {
            // The service isn't available, so use the default value below
            setting = null;
        }
        if (setting != null
                && setting.hasNumericSettingValue()
                && setting.getNumericSettingValue().hasValue()) {
            float durationInMonths = setting.getNumericSettingValue().getValue();
            return TimeUnit.HOURS.toMillis((long)(durationInMonths * 730f));
        } else {
            long durationInMs = settingName
                    .equals(GlobalSettingSpecs.CloudSavingsActionRetention.getSettingName())
                ? actionRetentionMs
                : volumeDeleteRetentionMs;
            logger.warn("Error retrieving {} setting - using previously set value of {} months",
                    spec.getDisplayName(), TimeUnit.MILLISECONDS.toHours(durationInMs) / 730f);
            return durationInMs;
        }
    }

    /**
     * Get the default value of an action expiration setting, in milliseconds.
     *
     * @param spec the GlobalSettingSpecs to get the default for
     * @return the default value for the setting, in milliseconds.
     */
    private long getDefaultSetting(GlobalSettingSpecs spec) {
        long durationInMonths;
        SettingProto.SettingSpec settingSpec = spec.createSettingSpec();
        durationInMonths = (long)settingSpec.getNumericSettingValueType().getDefault();
        return TimeUnit.HOURS.toMillis((long)(durationInMonths * 730f));
    }

    /**
     * Query the global settings manager to update the configured values.  This should be done once
     * at the beginning of periodic processing to avoid using stale configuration values.
     */
    public void updateValues() {
        this.actionRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsActionRetention);
        this.volumeDeleteRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsDeleteVolumeRetention);
        if (!actionRetentionMs.equals(previousActionRetentionMs)
                || !volumeDeleteRetentionMs.equals(previousVolumeDeleteRetentionMs)) {
            logger.info("Configured action expirations: volume delete = {}ms, all other actions = {}ms",
                    actionRetentionMs, volumeDeleteRetentionMs);
        }
        previousActionRetentionMs = actionRetentionMs;
        previousVolumeDeleteRetentionMs = volumeDeleteRetentionMs;
    }

    public long getActionRetentionMs() {
        return actionRetentionMs;
    }

    public long getVolumeDeleteRetentionMs() {
        return volumeDeleteRetentionMs;
    }

    /**
     * Queries the SettingsManager and fetches stats data retention settings values.
     *
     * @return DataRetentionHourlySettings with hourly settings values. Null if fetch error.
     */
    @Nullable
    DataRetentionSettings fetchDataRetentionSettings() {
        final SettingProto.GetMultipleGlobalSettingsRequest.Builder builder =
                SettingProto.GetMultipleGlobalSettingsRequest.newBuilder();
        DataRetentionSettings.getSettingNames().forEach(builder::addSettingSpecName);
        try {
            Iterator<Setting> settings = settingServiceClient.getMultipleGlobalSettings(
                    builder.build());
            return DataRetentionSettings.valueOf(settings, savingsAuditLogRetentionHours);
        } catch (io.grpc.StatusRuntimeException | IllegalArgumentException e) {
            logger.warn("Unable to fetch stats retention settings.", e);
        }
        return null;
    }

    /**
     * Used to store hourly settings values for stats and audit retention. Values are read from
     * SettingsManager and stored here in this POJO for convenience.
     */
    static class DataRetentionSettings {
        private final long auditLogRetentionInHours;
        private final long hourlyStatsRetentionInHours;
        private final long dailyStatsRetentionInHours;
        private final long monthlyStatsRetentionInHours;

        private static final Set<GlobalSettingSpecs> requiredSettings = ImmutableSet.of(
                GlobalSettingSpecs.StatsRetentionHours,
                GlobalSettingSpecs.StatsRetentionDays,
                GlobalSettingSpecs.StatsRetentionMonths);

        /**
         * Creates a new instance with given values.
         *
         * @param auditLogRetentionHours Audit retention duration in hours.
         * @param hourlyStatsRetentionInHours Hourly stats table retention in hours.
         * @param dailyStatsRetentionInDays Daily stats table retention in days.
         * @param monthlyStatsRetentionInMonths Monthly stats table retention in months.
         */
        DataRetentionSettings(long auditLogRetentionHours, long hourlyStatsRetentionInHours,
                long dailyStatsRetentionInDays, long monthlyStatsRetentionInMonths) {
            this.auditLogRetentionInHours = auditLogRetentionHours;
            this.hourlyStatsRetentionInHours = hourlyStatsRetentionInHours;
            this.dailyStatsRetentionInHours = dailyStatsRetentionInDays * 24;
            this.monthlyStatsRetentionInHours = monthlyStatsRetentionInMonths * 730;
        }

        /**
         * Reads values from settings read from SettingsManager.
         *
         * @param settings Settings containing values.
         * @param auditLogRetentionHours How long to retain audit events.
         * @return Instance with filled in values, defaults used where value not present.
         * @throws IllegalArgumentException When required settings are not found.
         */
        public static DataRetentionSettings valueOf(@Nullable final Iterator<Setting> settings,
                long auditLogRetentionHours)
                throws IllegalArgumentException {
            if (settings == null) {
                throw new IllegalArgumentException("No retention settings available.");
            }
            final Map<GlobalSettingSpecs, Long> values = new HashMap<>();
            settings.forEachRemaining(instance -> {
                if (instance.hasSettingSpecName() && instance.hasNumericSettingValue()) {
                    float value = instance.getNumericSettingValue().getValue();
                    Optional<GlobalSettingSpecs> spec = GlobalSettingSpecs.getSettingByName(
                            instance.getSettingSpecName());
                    spec.ifPresent(specs -> values.put(specs, Float.valueOf(value).longValue()));
                }
            });
            for (GlobalSettingSpecs spec : requiredSettings) {
                if (!values.containsKey(spec)) {
                    throw new IllegalArgumentException("Missing data retention settings for "
                            + spec.getSettingName());
                }
            }
            return new DataRetentionSettings(auditLogRetentionHours,
                    values.get(GlobalSettingSpecs.StatsRetentionHours),
                    values.get(GlobalSettingSpecs.StatsRetentionDays),
                    values.get(GlobalSettingSpecs.StatsRetentionMonths));
        }

        /**
         * Gets audit log retention settings in hours.
         *
         * @return Audit log retention.
         */
        public long getAuditLogRetentionInHours() {
            return auditLogRetentionInHours;
        }

        /**
         * Get hourly stats table retention in hours.
         *
         * @return Hourly stats retention.
         */
        public long getHourlyStatsRetentionInHours() {
            return hourlyStatsRetentionInHours;
        }

        /**
         * Get daily stats table retention in hours.
         *
         * @return Daily stats retention.
         */
        public long getDailyStatsRetentionInHours() {
            return dailyStatsRetentionInHours;
        }

        /**
         * Get monthly stats table retention in hours.
         *
         * @return Monthly stats retention.
         */
        public long getMonthlyStatsRetentionInHours() {
            return monthlyStatsRetentionInHours;
        }

        /**
         * Util method to get names of all settings we care about.
         *
         * @return Set of settings names to query.
         */
        @Nonnull
        public static Set<String> getSettingNames() {
            return requiredSettings.stream()
                    .map(GlobalSettingSpecs::getSettingName)
                    .collect(Collectors.toSet());
        }

        /**
         * Display string for debugging.
         *
         * @return Settings as string.
         */
        @Nonnull
        @Override
        public String toString() {
            return "Data retention (hours): " + "audit: " + auditLogRetentionInHours + ", hourly: "
                    + hourlyStatsRetentionInHours + ", daily: " + dailyStatsRetentionInHours
                    + ", monthly: " + monthlyStatsRetentionInHours;
        }
    }
}
