package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.common.setting.DailyObservationWindowsCount;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.BackgroundLoadingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;

/**
 * Configuration settings for timeslot historical editor.
 */
public class TimeslotHistoricalEditorConfig extends BackgroundLoadingHistoricalEditorConfig {
    /**
     * Default value how often to checkpoint observation window.
     */
    public static final int DEFAULT_MAINTENANCE_WINDOW_HOURS = 23;

    private final int maintenanceWindowHours;

    /**
     * Construct the timeslot editor settings.
     *
     * @param loadingChunkSize chunk size for loading from the persistence store
     * @param calculationChunkSize chunk size for calculating values
     * @param realtimeTopologyContextId identifier of the realtime topology.
     * @param backgroundLoadThreshold if that many commodities are not initialized, run loading in the background
     * @param backgroundLoadRetries how many load failures to tolerate before giving up
     * @param backgroundLoadTimeoutMin how much time to give to a single loading attempt
     * @param maintenanceWindowHours how often to perform maintenance
     * @param clock provides information about current time
     * @param kvConfig the config to access the topology processor key value store.
     */
    public TimeslotHistoricalEditorConfig(int loadingChunkSize, int calculationChunkSize,
                    long realtimeTopologyContextId, int backgroundLoadThreshold,
                    int backgroundLoadRetries, int backgroundLoadTimeoutMin,
                    int maintenanceWindowHours, @Nonnull Clock clock, @Nonnull KVConfig kvConfig) {
        super(loadingChunkSize, calculationChunkSize, realtimeTopologyContextId,
                        backgroundLoadThreshold, backgroundLoadRetries, backgroundLoadTimeoutMin,
                        clock, kvConfig);
        this.maintenanceWindowHours = maintenanceWindowHours;
    }

    public int getMaintenanceWindowHours() {
        return maintenanceWindowHours;
    }

    /**
     * Get the timeslot observation window configured for a given entity.
     *
     * @param context pipeline context
     * @param reference to the reference to the entity commodity
     * @return window in months
     */
    public int getObservationPeriod(@Nonnull HistoryAggregationContext context,
                    @Nonnull EntityCommodityReference reference) {
        return getReferenceSetting(context, reference,
                        EntitySettingSpecs.MaxObservationPeriodDesktopPool,
                        ss -> ss.getNumericSettingValueType().getDefault(), Float.class,
                        Float::intValue).intValue();
    }

    private static <V> Number getReferenceSetting(@Nonnull HistoryAggregationContext context,
                    @Nonnull EntityCommodityReference reference,
                    @Nonnull EntitySettingSpecs settingSpecs,
                    @Nonnull Function<SettingSpec, V> defaultValueProvider,
                    Class<V> settingValueType, @Nonnull Function<V, ? extends Number> converter) {
        final long relatedId = reference.getProviderOid() == null ?
                        reference.getEntityOid() :
                        reference.getProviderOid();
        return context.getSettingValue(relatedId, settingSpecs, settingValueType, converter,
                        defaultValueProvider);
    }

    /**
     * Get the number of slot windows per day for a given entity.
     *
     * @param context pipeline context
     * @param reference which setting we want to know.
     * @return slots, default to 1
     */
    public int getSlots(@Nonnull HistoryAggregationContext context,
                    @Nonnull EntityCommodityReference reference) {
        return getReferenceSetting(context, reference,
                        EntitySettingSpecs.DailyObservationWindowDesktopPool,
                        ss -> DailyObservationWindowsCount.THREE,
                        DailyObservationWindowsCount.class,
                        DailyObservationWindowsCount::getCountOfWindowsPerDay).intValue();
    }
}
