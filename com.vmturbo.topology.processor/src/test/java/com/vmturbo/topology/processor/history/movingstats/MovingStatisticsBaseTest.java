package com.vmturbo.topology.processor.history.movingstats;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.BlobPersistingCachingHistoricalEditorTest;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.MovingStatisticsSamplerDataCase;
import com.vmturbo.topology.processor.history.movingstats.MovingStatisticsSamplingConfiguration.ThrottlingSamplerConfiguration;

/**
 * Base for moving statistics-related tests.
 */
public class MovingStatisticsBaseTest {
    /**
     * The fast halflife.
     */
    public static final long FAST_HALFLIFE_HOURS = 12;

    /**
     * The slow halflife.
     */
    public static final long SLOW_HALFLIFE_HOURS = 72;

    /**
     * The standard deviations above the mean when generating a moving statistics analysis value.
     */
    public static final double STANDARD_DEVIATIONS_ABOVE_MEAN = 2.0;

    /**
     * The desired state target.
     */
    public static final double DESIRED_STATE_TARGET = 3.5;

    /**
     * Sampling config for throttling.
     */
    public static final ThrottlingSamplerConfiguration THROTTLING_SAMPLER_CONFIGURATION =
        new ThrottlingSamplerConfiguration(Duration.ofHours(FAST_HALFLIFE_HOURS),
            Duration.ofHours(SLOW_HALFLIFE_HOURS),
            Duration.ofDays(30),
            2.0,
            3.5);

    /**
     * Common key-value config.
     */
    public static final KVConfig KV_CONFIG = BlobPersistingCachingHistoricalEditorTest.createKvConfig(
        MovingStatisticsHistoricalEditorConfig.ADD_TO_DIAGNOSTICS_PROPERTY);

    /**
     * TopologyInfo.
     */
    public static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder().setTopologyId(77777L).build();

    protected MovingStatisticsBaseTest() {
        // Hide utility class constructor.
    }

    /**
     * Mock a sampling config.
     *
     * @param principalType The principal commodity type for the config.
     * @param partnerType The partner commodity type for the config.
     * @param retentionPeriod The retention period for the associated data.
     * @param samplerSupplier The supplier for the specific sampler type.
     * @param dataCase The protobuf datacase associated with this sampler type.
     * @param <T> The associated sampler type.
     * @return a sampling config.
     */
    public static <T extends MovingStatisticsSampler> MovingStatisticsSamplingConfiguration<T>
    mockSamplingConfig(final int principalType, final int partnerType, final Duration retentionPeriod,
                       Function<EntityCommodityFieldReference, T> samplerSupplier,
                       MovingStatisticsSamplerDataCase dataCase) {
        final MovingStatisticsSamplingConfiguration<T> config = mock(MovingStatisticsSamplingConfiguration.class);
        when(config.getPrincipalCommodityType()).thenReturn(CommodityType.newBuilder().setType(principalType).build());
        when(config.getPartnerCommodityTypes()).thenReturn(Collections.singletonList(
            CommodityType.newBuilder().setType(partnerType).build()));
        when(config.getFastHalflife()).thenReturn(Duration.ofHours(FAST_HALFLIFE_HOURS));
        when(config.getSlowHalflife()).thenReturn(Duration.ofHours(SLOW_HALFLIFE_HOURS));
        when(config.getDesiredStateTargetValue()).thenReturn(DESIRED_STATE_TARGET);
        when(config.getStandardDeviationsAbove()).thenReturn(STANDARD_DEVIATIONS_ABOVE_MEAN);
        when(config.getThrottlingRetentionPeriod()).thenReturn(retentionPeriod);
        when(config.supplySamplerInstance(any(EntityCommodityFieldReference.class)))
            .thenAnswer(invocation -> samplerSupplier.apply(invocation.getArgumentAt(0, EntityCommodityFieldReference.class)));
        when(config.getSamplerDataCase()).thenReturn(dataCase);

        return config;
    }

    /**
     * Create a config for testing.
     *
     * @param clock The clock to use.
     * @param samplingConfigurations The sampling configurations for the config.
     * @return a config.
     */
    public static MovingStatisticsHistoricalEditorConfig
    movingStatisticsHistoricalEditorConfig(@Nonnull final Clock clock,
                                           @Nonnull List<MovingStatisticsSamplingConfiguration<?>> samplingConfigurations) {
        return new MovingStatisticsHistoricalEditorConfig(samplingConfigurations,
                100, 100, 777777L,
                clock, KV_CONFIG, 10, 102);
    }
}
