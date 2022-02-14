package com.vmturbo.history.listeners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.history.listeners.PartmanHelper.PartitionProcessingException;

/**
 * Class to manage retention settings for configurable settings (everything but `latest`).
 */
class RetentionSettings {
    private static final Logger logger = LogManager.getLogger();

    // this lets us pretend there's a global setting for latest retention, which there isn't
    private static final String LATEST_RETENTION_MINUTES_SETTING_NAME = "latestRetentionMinutes";

    private static final Map<String, TimeFrame> settingToTimeFrame = ImmutableMap.of(
            LATEST_RETENTION_MINUTES_SETTING_NAME, TimeFrame.LATEST,
            GlobalSettingSpecs.StatsRetentionHours.getSettingName(), TimeFrame.HOUR,
            GlobalSettingSpecs.StatsRetentionDays.getSettingName(), TimeFrame.DAY,
            GlobalSettingSpecs.StatsRetentionMonths.getSettingName(), TimeFrame.MONTH
    );

    private static final Map<TimeFrame, String> timeFrameToUnitName = ImmutableMap.of(
            TimeFrame.LATEST, "minutes",
            TimeFrame.HOUR, "hours",
            TimeFrame.DAY, "days",
            TimeFrame.MONTH, "months"
    );

    private final SettingServiceBlockingStub settingService;
    private final int latestRetentionMinutes;
    private final Map<TimeFrame, Number> currentSettings = new HashMap<>();
    private Map<TimeFrame, Number> priorSettings = new HashMap<>();

    RetentionSettings(SettingServiceBlockingStub settingService, int latestRetentionMinutes) {
        this.settingService = settingService;
        this.latestRetentionMinutes = latestRetentionMinutes;
    }

    public void refresh() {
        priorSettings = new HashMap<>(currentSettings);
        currentSettings.clear();
        GetMultipleGlobalSettingsRequest request = GetMultipleGlobalSettingsRequest.newBuilder()
                .addSettingSpecName(GlobalSettingSpecs.StatsRetentionHours.getSettingName())
                .addSettingSpecName(GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                .addSettingSpecName(GlobalSettingSpecs.StatsRetentionMonths.getSettingName())
                .build();
        settingService.getMultipleGlobalSettings(request)
                .forEachRemaining(this::addSetting);
        currentSettings.put(TimeFrame.LATEST, latestRetentionMinutes);
    }

    /**
     * Set the value for one of the retention settings.
     *
     * @param setting {@link Setting} structure returned by `group` component, including setting
     *                name and value
     */
    public void addSetting(Setting setting) {
        TimeFrame timeFrame = settingToTimeFrame.getOrDefault(setting.getSettingSpecName(), null);
        if (timeFrame != null) {
            currentSettings.put(timeFrame, setting.getNumericSettingValue().getValue());
        } else {
            logger.warn("Unexpected retention setting name: {}", setting.getSettingSpecName());
        }
    }

    /**
     * Formatting a retention setting into an interval string suitable for the `retention` column if
     * the `partman.part_config` table.
     *
     * @param timeFrame timeframe whose setting is needed
     * @return interval string for timeframe
     * @throws PartitionProcessingException if the operation fails
     */
    public String getRetentionString(TimeFrame timeFrame) throws PartitionProcessingException {
        if (currentSettings.containsKey(timeFrame)) {
            return currentSettings.get(timeFrame) + " " + timeFrameToUnitName.get(timeFrame);
        } else {
            return null;
        }
    }

    /**
     * Return a list of strings describing the differences between these settings and another
     * (presumably prior) {@link RetentionSettings} instance.
     *
     * @return list of differences, if any
     */
    public Optional<List<String>> describeChanges() {
        List<String> diffs = new ArrayList<>();
        for (TimeFrame timeFrame : TimeFrame.values()) {
            Number current = currentSettings.get(timeFrame);
            Number prior = priorSettings.get(timeFrame);
            if (!Objects.equals(current, prior)) {
                diffs.add(String.format("%s retention: %s => %s %s",
                        timeFrame, prior, current, timeFrameToUnitName.get(timeFrame)));
            }
        }
        return diffs.isEmpty() ? Optional.empty() : Optional.of(diffs);
    }
}
