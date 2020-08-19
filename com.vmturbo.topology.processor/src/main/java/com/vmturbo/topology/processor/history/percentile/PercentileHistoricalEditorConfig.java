package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;

/**
 * Configuration parameters for percentile commodity editor.
 */
public class PercentileHistoricalEditorConfig extends CachingHistoricalEditorConfig {
    /**
     * Default value how often to checkpoint observation window.
     */
    public static final int DEFAULT_MAINTENANCE_WINDOW_HOURS = 24;

    private static final Logger logger = LogManager.getLogger();
    private static final Map<EntityType, EntitySettingSpecs> TYPE_AGGRESSIVENESS = ImmutableMap
                    .of(EntityType.BUSINESS_USER,
                        EntitySettingSpecs.PercentileAggressivenessBusinessUser,
                        EntityType.CONTAINER_SPEC,
                        EntitySettingSpecs.PercentileAggressivenessContainerSpec,
                        EntityType.VIRTUAL_MACHINE,
                        EntitySettingSpecs.PercentileAggressivenessVirtualMachine,
                        EntityType.DATABASE,
                        EntitySettingSpecs.PercentileAggressivenessDatabase);
    private static final Map<EntityType, EntitySettingSpecs> TYPE_MAX_OBSERVATION_PERIOD = ImmutableMap
                    .of(EntityType.BUSINESS_USER,
                        EntitySettingSpecs.MaxObservationPeriodBusinessUser,
                        EntityType.CONTAINER_SPEC,
                        EntitySettingSpecs.MaxObservationPeriodContainerSpec,
                        EntityType.VIRTUAL_MACHINE,
                        EntitySettingSpecs.MaxObservationPeriodVirtualMachine,
                        EntityType.DATABASE,
                        EntitySettingSpecs.MaxObservationPeriodDatabase);
    private static final Map<EntityType, EntitySettingSpecs> TYPE_MIN_OBSERVATION_PERIOD =
            ImmutableMap.of(EntityType.CONTAINER_SPEC,
                            EntitySettingSpecs.MinObservationPeriodContainerSpec,
                            EntityType.VIRTUAL_MACHINE,
                            EntitySettingSpecs.MinObservationPeriodVirtualMachine);
    private final Map<CommodityType, PercentileBuckets> buckets = new HashMap<>();
    private final int maintenanceWindowHours;
    private final int grpcStreamTimeoutSec;
    private final int blobReadWriteChunkSizeKb;


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
        super(0, calculationChunkSize, realtimeTopologyContextId, clock, kvConfig);
        // maintenance window cannot exceed minimum observation window
        final int minMaxObservationPeriod = (int)EntitySettingSpecs.MaxObservationPeriodVirtualMachine
                        .getSettingSpec().getNumericSettingValueType().getMin();
        this.maintenanceWindowHours = Math
                        .min(maintenanceWindowHours <= 0 ? DEFAULT_MAINTENANCE_WINDOW_HOURS
                                        : maintenanceWindowHours,
                             minMaxObservationPeriod * 24);
        this.grpcStreamTimeoutSec = grpcStreamTimeoutSec;
        this.blobReadWriteChunkSizeKb = blobReadWriteChunkSizeKb;
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

    /**
     * Get the stream read/write operations timeout.
     *
     * @return timeout in seconds
     */
    public int getGrpcStreamTimeoutSec() {
        return grpcStreamTimeoutSec;
    }

    /**
     * Get the size of chunks for reading and writing from persistent store.
     *
     * @return chunk size
     */
    public int getBlobReadWriteChunkSizeKb() {
        return blobReadWriteChunkSizeKb;
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
                                    @Nonnull Map<EntityType, EntitySettingSpecs> type2spec,
                                    @Nonnull String description, @Nonnull Number defaultValue) {
        EntitySettingSpecs spec = type2spec.get(context.getEntityType(oid));
        if (spec != null) {
            return context.getSettingValue(oid, spec, Number.class, Function.identity(),
                ss -> ss.getNumericSettingValueType().getDefault());
        }
        logger.trace("{} Returning default value {} for spec with description {} for entity "
                + "with oid {}",
            getClass().getSimpleName(), defaultValue, description, oid);
        return defaultValue;
    }
}
