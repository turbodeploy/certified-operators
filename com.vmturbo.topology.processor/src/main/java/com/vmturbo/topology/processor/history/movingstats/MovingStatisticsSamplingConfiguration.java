package com.vmturbo.topology.processor.history.movingstats;

import java.time.Duration;
import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.MovingStatisticsSamplerDataCase;

/**
 * {@link MovingStatisticsSamplingConfiguration} describes various settings to use when
 * computing moving statistics for VCPU throttling-related commodities. In the current
 * implementation, none of these settings are controllable by the end-user and can
 * only be set via platform configuration options.
 * <p/>
 * Generally there is a separate {@link MovingStatisticsSamplingConfiguration} type for each type
 * of {@link MovingStatisticsSampler}. For example, the {@link ThrottlingSamplerConfiguration} is
 * used together with {@link VcpuThrottlingSampler}.
 *
 * @param <T> The {@link MovingStatisticsSampler} type associated with this configuration.
 */
@Immutable
public abstract class MovingStatisticsSamplingConfiguration<T extends MovingStatisticsSampler> {
    private final Duration fastHalflife;
    private final Duration slowHalflife;
    private final Duration throttlingRetentionPeriod;

    private final double standardDeviationsAboveMean;
    private final double desiredStateTarget;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new {@link MovingStatisticsSamplingConfiguration}.
     *
     * @param fastHalflife The halflife to use for the faster moving statistics.
     * @param slowHalflife The halflife to use for the slower moving statistics. Must be at least as long as
     *                     the fastHalflife.
     * @param statisticsRetentionPeriod The number of days to retain statistics when no new sample
     *                                  has been received for a capacity.
     * @param standardDeviationsAboveMean The number of standard deviations above the mean to use when
     *                                        calculating a value to populate in the TopologyEntityDTO
     *                                        moving_mean_plus_sigma field for a commodity.
     * @param desiredStateTarget The desired state target to use when computing the minThreshold for
     *                           the principal commodity associated with the related {@link MovingStatisticsSampler}.
     */
    protected MovingStatisticsSamplingConfiguration(@Nonnull Duration fastHalflife,
                                                    @Nonnull Duration slowHalflife,
                                                    @Nonnull Duration statisticsRetentionPeriod,
                                                    final double standardDeviationsAboveMean,
                                                    final double desiredStateTarget) {
        if (fastHalflife.toMinutes() <= 0) {
            logger.warn("Illegal fast halflife duration {}. Setting to 1 minute", fastHalflife);
            fastHalflife = Duration.ofMinutes(1);
        }
        if (slowHalflife.toMinutes() <= 0) {
            logger.warn("Illegal slow halflife duration {}. Setting to 1 minute", fastHalflife);
            slowHalflife = Duration.ofMinutes(1);
        }
        if (fastHalflife.toMillis() > slowHalflife.toMillis()) {
            logger.warn("Fast halflife {} must be less than or equal to slow halflife {}. "
                + "Setting fast halflife to slow halflife.", fastHalflife, slowHalflife);
            fastHalflife = slowHalflife;
        }
        if (statisticsRetentionPeriod.toMinutes() <= 0) {
            logger.warn("Statistics retention period {} must be at least one minute. Setting to 1 minute.",
                statisticsRetentionPeriod);
            statisticsRetentionPeriod = Duration.ofMinutes(1);
        }

        this.fastHalflife = fastHalflife;
        this.slowHalflife = slowHalflife;
        this.throttlingRetentionPeriod = statisticsRetentionPeriod;
        this.standardDeviationsAboveMean = standardDeviationsAboveMean;
        this.desiredStateTarget = desiredStateTarget;
    }

    /**
     * Supply a new instance of the associated sampler to be used with this configuration.
     *
     * @param fieldReference The field reference associated with the sampler.
     * @return a new instance of the associated sampler to be used with this configuration.
     */
    @Nonnull
    public abstract T supplySamplerInstance(@Nonnull EntityCommodityFieldReference fieldReference);

    /**
     * Get the principal commodity type for the associated {@link MovingStatisticsSampler}.
     * Moving statistics are used to set both a TopologyEntityDTO#HistoricalUsed#moving_mean_plus_sigma
     * value as well as a min threshold on the associated commodity sold.
     *
     * @return the principal commodity type for the associated {@link MovingStatisticsSampler}.
     */
    @Nonnull
    public abstract CommodityType getPrincipalCommodityType();

    /**
     * Get the partner commodity types for the associated {@link MovingStatisticsSampler}.
     * Moving statistics are used to set a TopologyEntityDTO#HistoricalUsed#moving_mean_plus_sigma
     * but NOT the min thresholds for the associated commodity sold.
     *
     * @return the collection of secondary commodity types for the associated {@link MovingStatisticsSampler}.
     *         It is fine for this collection to be empty.
     */
    @Nonnull
    public abstract Collection<CommodityType> getPartnerCommodityTypes();

    /**
     * Get the protobuf sampler data case associated with this sampler type.
     * Note that this must be unique per principal commodity type.
     *
     * @return the protobuf sampler data case associated with this sampler type.
     */
    public abstract MovingStatisticsSamplerDataCase getSamplerDataCase();

    /**
     * Get the fast halflife for the moving statistics.
     *
     * @return the fast halflife for the moving statistics.
     */
    @Nonnull
    public Duration getFastHalflife() {
        return fastHalflife;
    }

    /**
     * Get the slow halflife for the moving statistics.
     *
     * @return the slow halflife for the moving statistics.
     */
    @Nonnull
    public Duration getSlowHalflife() {
        return slowHalflife;
    }

    /**
     * The number of days to retain statistics when no new sample has been received for a capacity.
     *
     * @return The retention period.
     */
    @Nonnull
    public Duration getThrottlingRetentionPeriod() {
        return throttlingRetentionPeriod;
    }

    /**
     * The number of deviations above the mean for use in computing the moving statistic to set in
     * the moving_mean_plus_sigma historical value.
     *
     * @return the number of deviations above the mean.
     */
    public double getStandardDeviationsAbove() {
        return standardDeviationsAboveMean;
    }

    /**
     * The desired state target value for the particular commodity type. Used when computing the
     * min threshold for the principal commodity.
     *
     * @return the desired state target value.
     */
    public double getDesiredStateTargetValue() {
        return desiredStateTarget;
    }

    /**
     * {@link MovingStatisticsSamplingConfiguration} for VCPU (principal) and throttling (partner) commodities.
     */
    @Immutable
    public static class ThrottlingSamplerConfiguration extends
        MovingStatisticsSamplingConfiguration<VcpuThrottlingSampler> {

        /**
         * Create a new {@link ThrottlingSamplerConfiguration}.
         *
         * @param fastHalflife The halflife to use for the faster moving statistics.
         * @param slowHalflife The halflife to use for the slower moving statistics. Must be at least as long as
         *                     the fastHalflife.
         * @param statisticsRetentionPeriod The number of days to retain statistics when no new sample
         *                                  has been received for a capacity.
         * @param standardDeviationsAboveMean The number of standard deviations above the mean to use when
         *                                        calculating a value to populate in the TopologyEntityDTO
         *                                        moving_mean_plus_sigma field for a commodity.
         * @param desiredStateTarget The desired state target to use when computing the minThreshold for
         *                           the principal commodity associated with the related {@link MovingStatisticsSampler}.
         */
        public ThrottlingSamplerConfiguration(@Nonnull final Duration fastHalflife,
                                              @Nonnull final Duration slowHalflife,
                                              @Nonnull final Duration statisticsRetentionPeriod,
                                              final double standardDeviationsAboveMean,
                                              final double desiredStateTarget) {
            super(fastHalflife, slowHalflife, statisticsRetentionPeriod,
                standardDeviationsAboveMean, desiredStateTarget);
        }

        @Override
        @Nonnull
        public VcpuThrottlingSampler supplySamplerInstance(@Nonnull EntityCommodityFieldReference fieldReference) {
            return new VcpuThrottlingSampler(fieldReference);
        }

        @Override
        @Nonnull
        public CommodityType getPrincipalCommodityType() {
            return VcpuThrottlingSampler.getPrincipalCommodityType();
        }

        @Override
        @Nonnull
        public Collection<CommodityType> getPartnerCommodityTypes() {
            return VcpuThrottlingSampler.PARTNER_COMMODITY_TYPES;
        }

        @Override
        public MovingStatisticsSamplerDataCase getSamplerDataCase() {
            return MovingStatisticsSamplerDataCase.THROTTLING_RECORD;
        }
    }
}
