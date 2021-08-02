package com.vmturbo.topology.processor.history.movingstats;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.AbstractBlobsHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.MovingStatisticsSamplerDataCase;

/**
 * Configuration parameters for moving statistics commodity editor.
 */
public class MovingStatisticsHistoricalEditorConfig extends AbstractBlobsHistoricalEditorConfig {

    private final Map<Integer, MovingStatisticsSamplingConfiguration<?>> samplingConfigurations;

    private final Map<MovingStatisticsSamplerDataCase, Integer> samplerTypeToCommodityType;

    /**
     * The property name in the topology processor key value store whose value represents boolean
     * flag. In case flag is set to true then moving statistics cache will be added to diagnostics.
     */
    @VisibleForTesting
    public static final String ADD_TO_DIAGNOSTICS_PROPERTY = "movingStatisticsInDiagnostics";

    /**
     * Construct the configuration for history editors.
     *
     * @param samplingConfigurations    Configurations for how to sample moving statistics for various
     *                                  commodity types for which we keep moving statistics.
     * @param loadingChunkSize          how to partition commodities for loading from db
     * @param calculationChunkSize      how to partition commodities for aggregating
     *                                  values
     * @param realtimeTopologyContextId identifier of the realtime topology.
     * @param clock                     provides information about current time
     * @param kvConfig                  the config to access the topology processor key value store.
     * @param grpcStreamTimeoutSec      the timeout for history access streaming operations
     * @param blobReadWriteChunkSizeKb  the size of chunks for reading and writing from persistent store
     */
    public MovingStatisticsHistoricalEditorConfig(
        @Nonnull Collection<MovingStatisticsSamplingConfiguration<?>> samplingConfigurations,
        int loadingChunkSize, int calculationChunkSize, long realtimeTopologyContextId,
        @Nonnull Clock clock, KVConfig kvConfig, int grpcStreamTimeoutSec,
        int blobReadWriteChunkSizeKb) {
        super(loadingChunkSize, calculationChunkSize, realtimeTopologyContextId, clock, kvConfig, grpcStreamTimeoutSec, blobReadWriteChunkSizeKb);

        this.samplingConfigurations = samplingConfigurations.stream()
            .collect(Collectors.toMap(samplingConfig -> samplingConfig.getPrincipalCommodityType().getType(),
                Function.identity()));
        this.samplerTypeToCommodityType = samplingConfigurations.stream()
            .collect(Collectors.toMap(MovingStatisticsSamplingConfiguration::getSamplerDataCase,
                samplingConfig -> samplingConfig.getPrincipalCommodityType().getType()));
    }

    /**
     * Get the sampling configuration associated with a particular commodity type. Null if
     * we have no sampling configuration for the commodity type.
     *
     * @param commodityType The commodity type whose {@link MovingStatisticsSamplingConfiguration}
     *                      should be fetched.
     * @return Get the sampling configuration associated with a particular commodity type.
     */
    @Nullable
    protected MovingStatisticsSamplingConfiguration<?> getSamplingConfigurations(final int commodityType) {
        return samplingConfigurations.get(commodityType);
    }

    /**
     * Get the commodity type number associated with a particular sampler data case. Null if
     * we have no commodity type associated with the given data case.
     *
     * @param dataCase The sampler data case.
     * @return the commodity type number associated with a particular sampler data case.
     */
    @Nullable
    public Integer getCommodityTypeForSamplerCase(final MovingStatisticsSamplerDataCase dataCase) {
        return samplerTypeToCommodityType.get(dataCase);
    }

    @Override
    protected String getDiagnosticsEnabledPropertyName() {
        return ADD_TO_DIAGNOSTICS_PROPERTY;
    }
}
