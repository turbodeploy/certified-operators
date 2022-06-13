package com.vmturbo.sql.utils.partition;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.RollupTimeFrame;

/**
 * Class to manage retention settings for configurable settings (everything but `latest`).
 */
public class RetentionSettings {
    private static final Logger logger = LogManager.getLogger();

    // this lets us pretend there's a global setting for latest retention, which there isn't
    private static final String LATEST_RETENTION_MINUTES_SETTING_NAME = "latestRetentionMinutes";

    private static final Map<String, RollupTimeFrame> settingToTimeFrame = ImmutableMap.of(
            LATEST_RETENTION_MINUTES_SETTING_NAME, RollupTimeFrame.LATEST,
            GlobalSettingSpecs.StatsRetentionHours.getSettingName(), RollupTimeFrame.HOUR,
            GlobalSettingSpecs.StatsRetentionDays.getSettingName(), RollupTimeFrame.DAY,
            GlobalSettingSpecs.StatsRetentionMonths.getSettingName(), RollupTimeFrame.MONTH
    );

    private static final Map<RollupTimeFrame, String> timeFrameToUnitName = ImmutableMap.of(
            RollupTimeFrame.LATEST, "minutes",
            RollupTimeFrame.HOUR, "hours",
            RollupTimeFrame.DAY, "days",
            RollupTimeFrame.MONTH, "months"
    );

    private static final Map<RollupTimeFrame, ChronoUnit> timeFrameToUnit = ImmutableMap.of(
            RollupTimeFrame.LATEST, ChronoUnit.MINUTES,
            RollupTimeFrame.HOUR, ChronoUnit.HOURS,
            RollupTimeFrame.DAY, ChronoUnit.DAYS,
            RollupTimeFrame.MONTH, ChronoUnit.MONTHS
    );

    private final SettingServiceBlockingStub settingService;
    private final int latestRetentionMinutes;
    private final Map<RollupTimeFrame, Number> currentSettings = new HashMap<>();
    private Map<RollupTimeFrame, Number> priorSettings = new HashMap<>();

    /**
     * Create a new instance.
     *
     * @param settingService         access ot the settings service to refresh retention settings
     * @param latestRetentionMinutes retention settiungs for `latest` table (not covered by settings
     *                               service)
     */
    public RetentionSettings(SettingServiceBlockingStub settingService,
            int latestRetentionMinutes) {
        this.settingService = settingService;
        this.latestRetentionMinutes = latestRetentionMinutes;
    }

    /**
     * Reload all retention settings from teh settings service.
     */
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
        currentSettings.put(RollupTimeFrame.LATEST, latestRetentionMinutes);
    }

    /**
     * Set the value for one of the retention settings.
     *
     * @param setting {@link Setting} structure returned by `group` component, including setting
     *                name and value
     */
    public void addSetting(Setting setting) {
        RollupTimeFrame timeFrame = settingToTimeFrame.getOrDefault(setting.getSettingSpecName(),
                null);
        if (timeFrame != null) {
            currentSettings.put(timeFrame, setting.getNumericSettingValue().getValue());
        } else {
            logger.warn("Unexpected retention setting name: {}", setting.getSettingSpecName());
        }
    }

    /**
     * Determine how long to retain data for tables associated with the given timeframe.
     *
     * @param timeFrame {@link TimeFrame} associated with table
     * @return retention {@link PeriodDuration} for tables with that timeframe
     */
    @Nullable
    public PeriodDuration getRetentionPeriod(RollupTimeFrame timeFrame) {
        if (currentSettings.containsKey(timeFrame)) {
            return timeFrame.getPeriod(currentSettings.get(timeFrame).intValue());
        } else {
            return null;
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
        for (RollupTimeFrame timeFrame : RollupTimeFrame.values()) {
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
