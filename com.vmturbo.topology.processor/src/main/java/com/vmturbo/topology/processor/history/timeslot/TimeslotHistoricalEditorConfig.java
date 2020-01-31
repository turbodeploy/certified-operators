package com.vmturbo.topology.processor.history.timeslot;

import com.vmturbo.topology.processor.history.BackgroundLoadingHistoricalEditorConfig;

/**
 * Configuration settings for timeslot historical editor.
 */
public class TimeslotHistoricalEditorConfig extends BackgroundLoadingHistoricalEditorConfig {
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
     */
    public TimeslotHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                    int backgroundLoadThreshold, int backgroundLoadRetries,
                    int backgroundLoadTimeoutMin, int maintenanceWindowHours) {
        super(loadingChunkSize, calculationChunkSize, backgroundLoadThreshold, backgroundLoadRetries,
              backgroundLoadTimeoutMin);
        this.maintenanceWindowHours = maintenanceWindowHours;
    }

    public int getMaintenanceWindowHours() {
        return maintenanceWindowHours;
    }

}
