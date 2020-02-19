package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.setting.DailyObservationWindowsCount;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.topology.processor.history.BackgroundLoadingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;

/**
 * Configuration settings for timeslot historical editor.
 */
public class TimeslotHistoricalEditorConfig extends BackgroundLoadingHistoricalEditorConfig {
    /**
     * Default value how often to checkpoint observation window.
     */
    public static int defaultMaintenanceWindowHours = 23;

    private final int maintenanceWindowHours;

    /**
     * Construct the timeslot editor settings.
     *
     * @param loadingChunkSize chunk size for loading from the persistence store
     * @param calculationChunkSize chunk size for calculating values
     * @param backgroundLoadThreshold if that many commodities are not initialized, run loading in the background
     * @param backgroundLoadRetries how many load failures to tolerate before giving up
     * @param backgroundLoadTimeoutMin how much time to give to a single loading attempt
     * @param maintenanceWindowHours how often to perform maintenance
     * @param clock provides information about current time
     */
    public TimeslotHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                    int backgroundLoadThreshold, int backgroundLoadRetries,
                    int backgroundLoadTimeoutMin, int maintenanceWindowHours, @Nonnull Clock clock) {
        super(loadingChunkSize, calculationChunkSize, backgroundLoadThreshold, backgroundLoadRetries,
              backgroundLoadTimeoutMin, clock);
        this.maintenanceWindowHours = maintenanceWindowHours;
    }

    public int getMaintenanceWindowHours() {
        return maintenanceWindowHours;
    }

    /**
     * Get the timeslot observation window configured for a given entity.
     *
     * @param context pipeline context
     * @param oid entity oid
     * @return window in months
     */
    public int getObservationPeriod(@Nonnull HistoryAggregationContext context, long oid) {
        Float window =
                   context.getEntitySetting(oid,
                                    EntitySettingSpecs.MaxObservationPeriodDesktopPool,
                                    Float.class);
        if (window != null) {
            return window.intValue();
        }
        return (int)EntitySettingSpecs.MaxObservationPeriodDesktopPool.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    /**
     * Get the number of slot windows per day for a given entity.
     *
     * @param context pipeline context
     * @param oid entity oid
     * @return slots, default to 1
     */
    public int getSlots(@Nonnull HistoryAggregationContext context, long oid) {
        DailyObservationWindowsCount slots =
                   context.getEntitySetting(oid,
                                    EntitySettingSpecs.DailyObservationWindowDesktopPool,
                                    DailyObservationWindowsCount.class);
        if (slots != null) {
            return slots.getCountOfWindowsPerDay();
        }
        return DailyObservationWindowsCount.THREE.getCountOfWindowsPerDay();
    }
}
