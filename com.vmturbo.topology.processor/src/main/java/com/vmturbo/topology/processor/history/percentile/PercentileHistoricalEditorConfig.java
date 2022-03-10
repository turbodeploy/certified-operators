package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.PercentileSettingSpecs;
import com.vmturbo.components.common.setting.PercentileSettingSpecs.EntityTypePercentileSettings;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.AbstractBlobsHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;

/**
 * Configuration parameters for percentile commodity editor.
 */
public class PercentileHistoricalEditorConfig extends AbstractBlobsHistoricalEditorConfig {
    /**
     * Default value how often to checkpoint observation window.
     */
    public static final int DEFAULT_MAINTENANCE_WINDOW_HOURS = 24;
    private static final int DEFAULT_FULL_PAGE_REASSEMBLY_PERIOD_DAYS = 7;
    private static final String CONSUL_FOLDER_NAME = "history-aggregation/";
    private static final String FULL_PAGE_REASSEMBLY_LAST_CHECKPOINT = "fullPageReassemblyLastCheckpoint";
    private static final String FULL_PAGE_REASSEMBLY_PERIOD = "fullPageReassemblyPeriod";

    /**
     * The property name in the topology processor key value store whose value represents boolean
     * flag. In case flag is set to true then percentile cache will be added to diagnostics.
     */
    @VisibleForTesting
    public static final String STORE_CACHE_TO_DIAGNOSTICS_PROPERTY = "storeCacheToDiagnostics";

    private static final Function<EntityTypePercentileSettings, EntitySettingSpecs> TYPE_AGGRESSIVENESS = EntityTypePercentileSettings::getAggressiveness;
    private static final Function<EntityTypePercentileSettings, EntitySettingSpecs> TYPE_MAX_OBSERVATION_PERIOD = EntityTypePercentileSettings::getObservationPeriod;
    private static final Function<EntityTypePercentileSettings, EntitySettingSpecs> TYPE_MIN_OBSERVATION_PERIOD = EntityTypePercentileSettings::getMinObservationPeriod;

    private final Map<CommodityType, PercentileBuckets> buckets = new HashMap<>();
    private final int maintenanceWindowHours;


    /**
     * Initialize the percentile configuration values.
     * @param calculationChunkSize chunk size for percentile calculation
     * @param realtimeTopologyContextId identifier of the realtime topology.
     * @param maintenanceWindowHours how often to checkpoint cache to persistent store
     * @param grpcStreamTimeoutSec the timeout for history access streaming operations
     * @param blobReadWriteChunkSizeKb the size of chunks for reading and writing from persistent store
     * @param commType2Buckets map of commodity type to percentile buckets specification
     * @param kvConfig the config to access the topology processor key value store.
     * @param clock provides information about current time.
     */
    public PercentileHistoricalEditorConfig(int calculationChunkSize, int maintenanceWindowHours,
                    long realtimeTopologyContextId, int grpcStreamTimeoutSec,
                    int blobReadWriteChunkSizeKb,
                    @Nonnull Map<CommodityType, String> commType2Buckets,
                    @Nullable KVConfig kvConfig, @Nonnull Clock clock) {
        super(0, calculationChunkSize, realtimeTopologyContextId, clock, kvConfig,
                grpcStreamTimeoutSec, blobReadWriteChunkSizeKb);
        // maintenance window cannot exceed minimum observation window
        final int minMaxObservationPeriod = (int)EntitySettingSpecs.MaxObservationPeriodVirtualMachine
                        .getSettingSpec().getNumericSettingValueType().getMin();
        this.maintenanceWindowHours = Math
                        .min(maintenanceWindowHours <= 0 ? DEFAULT_MAINTENANCE_WINDOW_HOURS
                                        : maintenanceWindowHours,
                             minMaxObservationPeriod * 24);
        commType2Buckets.forEach((commType, bucketsSpec) -> buckets
                        .put(commType, new PercentileBuckets(bucketsSpec)));
    }

    /**
     * Get the buckets distribution specification for the given commodity type.
     * Return default 101-value bucket for an unconfigured type.
     *
     * @param commodityType commodity type number
     * @return percentile buckets
     */
    public PercentileBuckets getPercentileBuckets(int commodityType) {
        return buckets.computeIfAbsent(CommodityType.forNumber(commodityType),
                                       type -> new PercentileBuckets(StringUtils.EMPTY));
    }

    /**
     * Get the maintenance window.
     *
     * @return how often to checkpoint the full window into the persistent store.
     */
    public int getMaintenanceWindowHours() {
        return maintenanceWindowHours;
    }

    /**
     * Get the percentile scaling aggressiveness for a given entity.
     *
     * @param context the history aggregation context.
     * @param oid entity oid
     * @return aggressiveness in percents, default if not found
     */
    public float getAggressiveness(@Nonnull HistoryAggregationContext context, long oid) {
        return getNumberSetting(context, oid, TYPE_AGGRESSIVENESS, "aggressiveness",
                             getDefaultAggressiveness()).floatValue();
    }

    /**
     * Get the percentile observation period for a given entity.
     *
     * @param oid entity oid
     * @return observation period
     */
    public int getObservationPeriod(@Nonnull HistoryAggregationContext context, long oid) {
        return getNumberSetting(context, oid, TYPE_MAX_OBSERVATION_PERIOD, "observation period",
                             getDefaultObservationPeriod()).intValue();
    }

    /**
     * Get min the percentile observation period for a given entity.
     *
     * @param oid entity oid
     * @return observation period
     */
    public int getMinObservationPeriod(@Nonnull HistoryAggregationContext context, long oid) {
        return getNumberSetting(context, oid, TYPE_MIN_OBSERVATION_PERIOD, "min observation period",
                getDefaultMinObservationPeriod()).intValue();
    }

    private static int getDefaultAggressiveness() {
        return (int)EntitySettingSpecs.PercentileAggressivenessVirtualMachine.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    public static int getDefaultObservationPeriod() {
        return (int)EntitySettingSpecs.MaxObservationPeriodVirtualMachine.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    private static int getDefaultMinObservationPeriod() {
        return (int)EntitySettingSpecs.MinObservationPeriodVirtualMachine.getSettingSpec()
                .getNumericSettingValueType().getDefault();
    }

    @Nonnull
    private Number getNumberSetting(@Nonnull HistoryAggregationContext context, long oid,
                                    @Nonnull Function<EntityTypePercentileSettings, EntitySettingSpecs> type2spec,
                                    @Nonnull String description, @Nonnull Number defaultValue) {
        return PercentileSettingSpecs.getPercentileSettings(context.getEntityType(oid))
            .map(type2spec)
            .map(spec -> context.getSettingValue(oid, spec, Number.class, Function.identity(),
                    ss -> ss.getNumericSettingValueType().getDefault()))
            .orElse(defaultValue);
    }

    /**
     * Get the full page reassembly period in days.
     *
     * @return the full page reassembly period in days.
     */
    public int getFullPageReassemblyPeriodInDays() {
        return (int)getFullPageConsulValue(FULL_PAGE_REASSEMBLY_PERIOD,
                DEFAULT_FULL_PAGE_REASSEMBLY_PERIOD_DAYS);
    }

    private long getFullPageConsulValue(String consulPropertyName, long defaultValue){
        long result;
        try {
            result = Long.parseLong(
                    getConsulValue(CONSUL_FOLDER_NAME, consulPropertyName).orElse(""));
            if (result < 0) {
                result = defaultValue;
            }
        } catch (NumberFormatException ex) {
            result = defaultValue;
        }
        return result;
    }

    /**
     * Get the full page reassembly checkpoint in milliseconds.
     *
     * @return the full page reassembly checkpoint in millisecond
     */
    public long getFullPageReassemblyLastCheckpointInMs() {
        return getFullPageConsulValue(FULL_PAGE_REASSEMBLY_LAST_CHECKPOINT, 0);
    }

    /**
     * Set the full page reassembly checkpoint in milliseconds.
     *
     * @param checkpointMs checkpoint value in milliseconds
     */
    public void setFullPageReassemblyLastCheckpoint(long checkpointMs) {
        setConsulValue(CONSUL_FOLDER_NAME, FULL_PAGE_REASSEMBLY_LAST_CHECKPOINT, String.valueOf(checkpointMs));
    }


    @Override
    protected String getDiagnosticsEnabledPropertyName() {
        return STORE_CACHE_TO_DIAGNOSTICS_PROPERTY;
    }
}
