package com.vmturbo.topology.processor.history.percentile;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
import com.vmturbo.topology.processor.history.HistoryCalculationException;

/**
 * Configuration parameters for percentile commodity editor.
 */
public class PercentileHistoricalEditorConfig extends CachingHistoricalEditorConfig {
    /**
     * Default value how often to checkpoint observation window.
     */
    public static int defaultMaintenanceWindowHours = 24;

    /**
     * The property name in the topology processor key value store whose value
     * represents the OID of the entity for which percentile counts are logged.
     */
    private static final String OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY = "oidForPercentileCountsLog";
    private static final Logger logger = LogManager.getLogger();
    private static final Map<EntityType, EntitySettingSpecs> TYPE_AGGRESSIVENESS = ImmutableMap
                    .of(EntityType.BUSINESS_USER,
                        EntitySettingSpecs.PercentileAggressivenessBusinessUser,
                        EntityType.VIRTUAL_MACHINE,
                        EntitySettingSpecs.PercentileAggressivenessVirtualMachine);
    private static final Map<EntityType, EntitySettingSpecs> TYPE_OBSERVATION_PERIOD = ImmutableMap
                    .of(EntityType.BUSINESS_USER,
                        EntitySettingSpecs.MaxObservationPeriodBusinessUser,
                        EntityType.VIRTUAL_MACHINE,
                        EntitySettingSpecs.MaxObservationPeriodVirtualMachine);

    private final Map<CommodityType, PercentileBuckets> buckets = new HashMap<>();
    private final int maintenanceWindowHours;
    private final int grpcStreamTimeoutSec;
    private final int blobReadWriteChunkSizeKb;
    private final KVConfig kvConfig;


    /**
     * Initialize the percentile configuration values.
     *
     * @param calculationChunkSize chunk size for percentile calculation
     * @param maintenanceWindowHours how often to checkpoint cache to persistent store
     * @param grpcStreamTimeoutSec the timeout for history access streaming operations
     * @param blobReadWriteChunkSizeKb the size of chunks for reading and writing from persistent store
     * @param commType2Buckets map of commodity type to percentile buckets specification
     * @param kvConfig the config to access the topology processor key value store.
     */
    public PercentileHistoricalEditorConfig(int calculationChunkSize, int maintenanceWindowHours,
                                            int grpcStreamTimeoutSec, int blobReadWriteChunkSizeKb,
                                            @Nonnull Map<CommodityType, String> commType2Buckets,
                                            @Nullable KVConfig kvConfig) {
        super(0, calculationChunkSize);
        // maintenance window cannot exceed minimum observation window
        final int minMaxObservationPeriod = (int)EntitySettingSpecs.MaxObservationPeriodVirtualMachine
                        .getSettingSpec().getNumericSettingValueType().getMin();
        this.maintenanceWindowHours = Math
                        .min(maintenanceWindowHours <= 0 ? defaultMaintenanceWindowHours
                                        : maintenanceWindowHours,
                             minMaxObservationPeriod * 24);
        this.grpcStreamTimeoutSec = grpcStreamTimeoutSec;
        this.blobReadWriteChunkSizeKb = blobReadWriteChunkSizeKb;
        commType2Buckets.forEach((commType, bucketsSpec) -> buckets
                        .put(commType, new PercentileBuckets(bucketsSpec)));
        this.kvConfig = kvConfig;
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
     * @param oid entity oid
     * @return aggressiveness in percents, default if not found
     */
    public int getAggressiveness(long oid) {
        return getIntSetting(oid, TYPE_AGGRESSIVENESS, "aggressiveness",
                             getDefaultAggressiveness());
    }

    /**
     * Get the percentile observation period for a given entity.
     *
     * @param oid entity oid
     * @return observation period
     */
    public int getObservationPeriod(long oid) {
        return getIntSetting(oid, TYPE_OBSERVATION_PERIOD, "observation period",
                             getDefaultObservationPeriod());
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

    /**
     * The configured OID whose percentile counts needs to be logged when debug enabled.
     *
     * @return the configured OID.
     */
    public Optional<String> getOidToBeTracedInLog() {
        return Optional.ofNullable(kvConfig)
                .map(KVConfig::keyValueStore)
                .map(store -> store.get(OID_FOR_PERCENTILE_COUNTS_LOG_PROPERTY))
                .orElse(Optional.empty());
    }

    private static int getDefaultAggressiveness() {
        return (int)EntitySettingSpecs.PercentileAggressivenessVirtualMachine.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    private static int getDefaultObservationPeriod() {
        return (int)EntitySettingSpecs.MaxObservationPeriodVirtualMachine.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    private int getIntSetting(long oid,
                              @Nonnull Map<EntityType, EntitySettingSpecs> type2spec,
                              @Nonnull String description, int defaultValue) {
        try {
            EntitySettingSpecs spec = type2spec.get(getEntityType(oid));
            if (spec != null) {
                Float value = getEntitySetting(oid, spec, Float.class);
                if (value != null) {
                    return value.intValue();
                }
            }
        } catch (HistoryCalculationException e) {
            logger.warn("Cannot get percentile " + description
                        + " setting, assuming default for "
                        + oid, e);
        }
        logger.debug("{} Returning default value {} for percentile {} with oid {}",
                getClass().getSimpleName(), defaultValue, description, oid);
        return defaultValue;
    }

}
