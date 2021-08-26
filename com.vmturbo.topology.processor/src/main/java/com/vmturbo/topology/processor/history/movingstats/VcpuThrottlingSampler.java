package com.vmturbo.topology.processor.history.movingstats;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.math.DoubleMath;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCapacityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;

/**
 * Moving statistics for VCPU and VCPU_THROTTLING commodities. The throttling commodity
 * is tightly coupled to the behavior of the VCPU commodity and we maintain statistics
 * for both of them together.
 * <p/>
 * Throttling statistics cannot be normalized to new capacities, so we instead we maintain history
 * for each capacity independently. The active statistics are the statistics for the current
 * capacity of the VCPU commodity. Statistics collected at other capacities are maintained
 * in the inactive set.
 */
public class VcpuThrottlingSampler implements MovingStatisticsSampler {

    private static final Logger logger = LogManager.getLogger();

    private static final double CAPACITY_EQUIVALENCE_DELTA = 1e-5;

    private static final int THROTTLING_COMMODITY_PARTNER_POSITION = 0;

    /**
     * Partner commodity types for this {@link MovingStatisticsSampler}. In this case,
     * the partner commodity types is {@code CommodityType.VCPU_THROTTLING}.
     */
    public static final Collection<CommodityType> PARTNER_COMMODITY_TYPES = Collections.singletonList(
        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE).build());

    private final EntityCommodityFieldReference fieldReference;
    private CapacityMovingStatistics active;

    private ObjectArrayList<CapacityMovingStatistics> allCapacityStatistics;

    /**
     * Create a new {@link VcpuThrottlingSampler}.
     *
     * @param fieldReference The field containing the information about the entity and commodity
     *                       associated with the data sampled by this sampler.
     */
    public VcpuThrottlingSampler(@Nonnull final EntityCommodityFieldReference fieldReference) {
        this.fieldReference = fieldReference;
    }

    @Override
    public void addSample(@Nonnull MovingStatisticsSamplingConfiguration<?> configuration,
                                       @Nonnull EntityCommodityFieldReference principalCommodityField,
                                       @Nonnull List<EntityCommodityFieldReference> partnerFields,
                                       @Nonnull ICommodityFieldAccessor commodityFieldAccessor) {
        if (partnerFields.size() != 1) {
            logger.error("Unexpected throttling partner commodities {}", partnerFields);
            return;
        }

        final Double vcpuCapacity = commodityFieldAccessor.getCapacity(principalCommodityField);
        final Double throttlingUsed = commodityFieldAccessor.getRealTimeValue(
            partnerFields.get(THROTTLING_COMMODITY_PARTNER_POSITION));
        final Long lastUpdatedTime = commodityFieldAccessor.getLastUpdatedTime(principalCommodityField);

        if (vcpuCapacity == null || lastUpdatedTime == null || throttlingUsed == null) {
            logger.debug("Unable to add MovingStatistics sample for {}. "
                    + "vcpuCapacity {}, throttlingUsed {}, lastUpdatedTime {}",
                principalCommodityField, vcpuCapacity, lastUpdatedTime, throttlingUsed);
            return;
        }

        activateCapacity(vcpuCapacity);
        active.sample(throttlingUsed, lastUpdatedTime, configuration);
    }

    @Nullable
    @Override
    public Double meanPlusSigma(@Nonnull EntityCommodityFieldReference field,
                                             double standardDeviationsAboveMean) {
        // We only set the mean plus standard deviations on the THROTTLING commodity, not the VCPU commodity.
        if (active == null) {
            return null;
        }

        if (field.getCommodityType().getType() == CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE) {
            return active.meanPlusSigma(standardDeviationsAboveMean);
        }

        return null;
    }

    @Nullable
    @Override
    public Double getMinThreshold(@Nonnull EntityCommodityFieldReference field,
                                               double standardDeviationsAboveMean, double desiredStateTarget) {
        // We only set the threshold on the VCPU commodity, not the throttling commodity.
        if (active == null || field.getCommodityType().getType() != CommodityDTO.CommodityType.VCPU_VALUE) {
            return null;
        }

        return computeVcpuMinThreshold(standardDeviationsAboveMean, desiredStateTarget);
    }

    @Override
    public boolean cleanExpiredData(long currentTimeMs,
                                    @Nonnull MovingStatisticsSamplingConfiguration<?> configuration) {
        if (allCapacityStatistics == null) {
            return false;
        }

        final long retentionPeriodMs = configuration.getThrottlingRetentionPeriod().toMillis();

        // Never drop the active stats
        return allCapacityStatistics.removeIf(stats -> stats != active
            && !stats.isWithinRetentionPeriod(currentTimeMs, retentionPeriodMs));
    }

    @Nonnull
    @Override
    public MovingStatisticsRecord.Builder serialize() {
        final MovingStatisticsRecord.Builder statsRecord = MovingStatisticsRecord.newBuilder();
        final ThrottlingMovingStatisticsRecord.Builder statsBuilder =
            ThrottlingMovingStatisticsRecord.newBuilder();
        statsRecord.setEntityOid(fieldReference.getEntityOid());

        if (allCapacityStatistics != null) {
            for (CapacityMovingStatistics stats : allCapacityStatistics) {
                statsBuilder.addCapacityRecords(stats.serialize());
            }
        }

        if (active != null) {
            statsBuilder.setActiveVcpuCapacity(active.getCapacity());
        }

        statsRecord.setThrottlingRecord(statsBuilder);
        return statsRecord;
    }

    @Override
    public void deserialize(@Nonnull final MovingStatisticsRecord record)
        throws InvalidHistoryDataException {
        if (!record.hasThrottlingRecord()) {
            throw new InvalidHistoryDataException("Unable to handle data of type "
                + record.getMovingStatisticsSamplerDataCase());
        }
        if (record.getEntityOid() != fieldReference.getEntityOid()) {
            throw new InvalidHistoryDataException("Entity OID " + record.getEntityOid()
                + " does not match expected OID " + fieldReference.getEntityOid());
        }

        active = null;
        allCapacityStatistics = null;

        final ThrottlingMovingStatisticsRecord throttlingRecord = record.getThrottlingRecord();
        if (throttlingRecord.getCapacityRecordsCount() > 0) {
            allCapacityStatistics = new ObjectArrayList<>(throttlingRecord.getCapacityRecordsCount());
        }

        for (ThrottlingCapacityMovingStatistics capacityRecord : throttlingRecord.getCapacityRecordsList()) {
            final CapacityMovingStatistics stats = new CapacityMovingStatistics(capacityRecord);
            allCapacityStatistics.add(stats);
        }

        if (throttlingRecord.hasActiveVcpuCapacity()) {
            activateCapacity(throttlingRecord.getActiveVcpuCapacity());
        }
    }

    /**
     * Get the number of capacity statistics for this sampler.
     *
     * @return the number of capacity statistics for this sampler.
     */
    public int getCapacityStatCount() {
        return allCapacityStatistics.size();
    }

    /**
     * Get the principal commodity type for this {@link MovingStatisticsSampler}. In this case,
     * the principal commodity type is {@code CommodityType.VCPU}.
     *
     * @return the principal commodity type for this {@link MovingStatisticsSampler}.
     */
    public static CommodityType getPrincipalCommodityType() {
        return CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE).build();
    }

    /**
     * Determine if the capacityA and capacityB are close enough to be considered equivalent.
     *
     * @param capacityA The first capacity
     * @param capacityB The second capacity.
     * @return if the capacityA and capacityB are close enough to be considered equivalent.
     */
    private static boolean capacityEquivalent(final double capacityA, final double capacityB) {
        return DoubleMath.fuzzyEquals(capacityA, capacityB, CAPACITY_EQUIVALENCE_DELTA);
    }

    private void activateCapacity(final double newCapacity) {
        if (allCapacityStatistics == null) {
            allCapacityStatistics = new ObjectArrayList<>(1);
            allCapacityStatistics.add(new CapacityMovingStatistics(newCapacity));
        }

        if (active == null || !capacityEquivalent(newCapacity, active.vcpuCapacity)) {
            CapacityMovingStatistics existing = null;
            for (CapacityMovingStatistics stats : allCapacityStatistics) {
                if (capacityEquivalent(newCapacity, stats.vcpuCapacity)) {
                    existing = stats;
                    break;
                }
            }

            if (existing != null) {
                active = existing;
            } else {
                active = new CapacityMovingStatistics(newCapacity);
                allCapacityStatistics.add(active);
            }
        }
    }

    /**
     * Interpolate lower bound for VCPU capacity based on the moving average statistics at
     * various capacities. Right now, given a target throttling value, we find the lowest
     * VCPU capacity with throttling below the target value and the highest VCPU capacity
     * with throttling above the target throttling value and then perform a linear interpolation
     * to find the VCPU capacity at which we could expect to hit the target throttling value.
     * We can improve this algorithm if/when we find issues but so far this has delivered
     * pretty good empirical results in a testbed where VCPU resize actions are automated.
     *
     * @param sigmaCoefficient The number of standard deviations above the mean when computing throttling values.
     * @param targetMaxThrottling The max throttling value that our analysis engine attempts to drive
     *                            throttling commodity utilization toward.
     * @return null if no throttling, or the capacity lower bound to set otherwise.
     */
    private Double computeVcpuMinThreshold(final double sigmaCoefficient,
                                          final double targetMaxThrottling) {
        if (allCapacityStatistics == null) {
            // No lower bound if we only have history at a single size.
            return null;
        }

        double vcpuCapacityBelow = Double.MAX_VALUE;
        double vcpuCapacityAbove = -1.0;

        double throttlingValueBelow = -1.0;
        double throttlingValueAbove = Double.MAX_VALUE;

        for (CapacityMovingStatistics stats : allCapacityStatistics) {
            final double throttlingValue = stats.meanPlusSigma(sigmaCoefficient);

            if (throttlingValue > targetMaxThrottling) {
                // Pick the highest VCPU cpapacity with throttling above the target
                if (stats.vcpuCapacity > vcpuCapacityAbove) {
                    throttlingValueAbove = throttlingValue;
                    vcpuCapacityAbove = stats.vcpuCapacity;
                }
            } else if (throttlingValue < targetMaxThrottling) {
                // Pick the lowest VCPU capacity with throttling below the target
                if (stats.vcpuCapacity < vcpuCapacityBelow) {
                    throttlingValueBelow = throttlingValue;
                    vcpuCapacityBelow = stats.vcpuCapacity;
                }
            } else {
                vcpuCapacityAbove = stats.vcpuCapacity;
                vcpuCapacityBelow = stats.vcpuCapacity;

                throttlingValueAbove = throttlingValue;
                throttlingValueBelow = throttlingValue;
            }
        }

        return interpolateLowerBound(vcpuCapacityBelow, vcpuCapacityAbove,
            throttlingValueBelow, throttlingValueAbove, targetMaxThrottling);
    }

    /**
     * Compute the VCPU capacity lower bound given the history of throttling values at different
     * capacities. We expect throttling to have an inverse relationship with the VCPU limit
     * (as the limit decreases, throttling increases). In situations where this is not true,
     * we return the highest VCPU capacity over the target throttling utilization.
     *
     * @param vcpuCapacityBelow The VCPU capacity at which we collected the samples for the throttlingValueBelow.
     * @param vcpuCapacityAbove The VCPU capacity at which we collected the samples for the throttlingValueAbove.
     * @param throttlingValueBelow The closest throttling value below the target for which we have sampeld data.
     * @param throttlingValueAbove The closest throttling value above the target for which we have sampled data.
     * @param targetThrottlingValue The max throttling value that our analysis engine attempts to drive
     *                              throttling commodity utilization toward.
     * @return null if no meaningful lower bound can be computed. The lower bound if we can
     *         compute a meaningful lower bound.
     */
    @Nullable
    private static Double interpolateLowerBound(final double vcpuCapacityBelow, final double vcpuCapacityAbove,
                                                final double throttlingValueBelow, final double throttlingValueAbove,
                                                final double targetThrottlingValue) {
        // We don't have points on both sides of the target value. In this case we have
        // insufficient data for a lower bound.
        if (throttlingValueAbove == Double.MAX_VALUE) {
            return null;
        }

        if (throttlingValueAbove == throttlingValueBelow) {
            // In the case where we have hit exactly the target value, it's fine to return
            // either the capacity above or below because they will be the same.
            return vcpuCapacityAbove;
        }

        // If we have samples above the target throttling but none below, or somehow we have a higher
        // capacity above than below, return a value just above the capacity above. We want to prevent
        // sizing exactly back down to the vcpuCapacityAbove because if we do, we'll just immediately
        // go back up.
        if (throttlingValueBelow < 0 || vcpuCapacityAbove > vcpuCapacityBelow) {
            return vcpuCapacityAbove + 1.0;
        }

        // Compute value t such that lower + t * (upper - lower) == target
        // t * (upper - lower) == target - lower
        // t == (target - lower) / (upper - lower)
        double t = (targetThrottlingValue - throttlingValueBelow) / (throttlingValueAbove - throttlingValueBelow);
        return vcpuCapacityBelow + t * (vcpuCapacityAbove - vcpuCapacityBelow);
    }

    /**
     * Moving statistics for an individual VCPU capacity value. Every time the VCPU capacity
     * changes, we create a new {@link CapacityMovingStatistics}. Note that because Throttling
     * behavior is so tightly coupled to VCPU Capacity, we refresh both VCPU and throttling
     * statistics every time VCPU capacity changes.
     */
    private static class CapacityMovingStatistics {

        private static final double LOG_ONE_HALF = Math.log(0.5);

        private final double vcpuCapacity;
        private long lastSampleTimestamp;
        private int sampleCount;

        private double throttlingMaxSample;

        private double throttlingFastMovingAverage;
        private double throttlingFastMovingVariance;

        private double throttlingSlowMovingAverage;
        private double throttlingSlowMovingVariance;

        /**
         * Speed (fast or slow) at which statistics are accumulated.
         */
        private enum StatisticsSpeed {
            FAST,
            SLOW
        }

        private CapacityMovingStatistics(double vcpuCapacity) {
            this.vcpuCapacity = vcpuCapacity;
            lastSampleTimestamp = -1;
            sampleCount = 0;
        }

        private CapacityMovingStatistics(@Nonnull final ThrottlingCapacityMovingStatistics stats) {
            vcpuCapacity = stats.getVcpuCapacity();
            lastSampleTimestamp = stats.getLastSampleTimestamp();
            sampleCount = stats.getSampleCount();
            throttlingFastMovingAverage = stats.getThrottlingFastMovingAverage();
            throttlingFastMovingVariance = stats.getThrottlingFastMovingVariance();
            throttlingSlowMovingAverage = stats.getThrottlingSlowMovingAverage();
            throttlingSlowMovingVariance = stats.getThrottlingSlowMovingVariance();
            throttlingMaxSample = stats.getThrottlingMaxSample();
        }

        public double getCapacity() {
            return vcpuCapacity;
        }

        /**
         * Compute the smoothing coefficient given the halflife and the timestamp of the most
         * recent sample. The derivation is given in the comments below.
         *
         * @param halflife The halflife duration.
         * @param sampleTimestamp The timestamp of the most recent sample.
         * @return The exponential smoothing coefficient for use in accumulating the moving
         *         statistics.
         */
        private double computeExponentialSmoothingCoefficient(@Nonnull final Duration halflife,
                                                              long sampleTimestamp) {
            if (lastSampleTimestamp <= 0) {
                // If this is the first sample, use an alpha of 1.0.
                return 1.0;
            }

            long elapsedMillis = sampleTimestamp - lastSampleTimestamp;
            if (elapsedMillis <= 0) {
                // Prevent division by zero in case of fast consecutive calls.
                return 0;
            }

            // Derivation. Given that we want the value to move halfway between
            // the past values and the new value in the given halflife time period, we want to pick
            // a coefficent alpha such that
            // 0.5 == (alpha)^(halflife/elapsed)                take the natural log of both sides
            // log(0.5) == (halflife/elapsed) * log(alpha)      divide by (halflife/elapsed)
            // log(0.5) / (halflife/elapsed) == log(alpha)      raise both sides as the power of e to remove the log
            // e^(log(0.5) / (halflife/elapsed) == alpha
            double elapsedHalflife = halflife.toMillis() / (double)elapsedMillis;
            return 1.0 - Math.exp(LOG_ONE_HALF / elapsedHalflife);
        }

        /**
         * Sample the most recent VCPU and Throttling used values at a given timestamp.
         *
         * @param throttlingUsed The used value of the VCPU_THROTTLING commodity.
         * @param lastUpdatedTime The timestamp at which we received the commodity used samples.
         * @param configuration The configuration
         */
        private void sample(final double throttlingUsed, final long lastUpdatedTime,
                           @Nonnull MovingStatisticsSamplingConfiguration<?> configuration) {
            if (this.lastSampleTimestamp == lastUpdatedTime) {
                return;
            }

            final double fastExponentialSmoothingCoefficient =
                computeExponentialSmoothingCoefficient(configuration.getFastHalflife(), lastUpdatedTime);
            final double slowExponentialSmoothingCoefficient =
                computeExponentialSmoothingCoefficient(configuration.getSlowHalflife(), lastUpdatedTime);

            if (sampleCount == 0) {
                throttlingFastMovingAverage = throttlingUsed;
                throttlingSlowMovingAverage = throttlingUsed;
                throttlingMaxSample = throttlingUsed;
            } else {
                sample(StatisticsSpeed.FAST, throttlingUsed, fastExponentialSmoothingCoefficient);
                sample(StatisticsSpeed.SLOW, throttlingUsed, slowExponentialSmoothingCoefficient);
                throttlingMaxSample = Math.max(throttlingMaxSample, throttlingUsed);
            }

            lastSampleTimestamp = lastUpdatedTime;
            sampleCount++;
        }

        /**
         * Adapted from Section 9 in https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf.
         * <p/>
         * Additional references at: https://www.johndcook.com/blog/standard_deviation/ and
         * https://stackoverflow.com/questions/1023860/exponential-moving-average-sampled-at-varying-times
         *
         * @param speed                     Whether to sample the fast or slow statistics
         * @param sample                    The sample to incorporate
         * @param exponentialSmoothingAlpha The exponential smoothing weight (alpha)
         *                                  A higher alpha (closer to 1.0) means a slower moving average. That is,
         *                                  the closer to 1.0 the alpha, the longer you will remember the past.
         */
        private void sample(final StatisticsSpeed speed, final double sample, final double exponentialSmoothingAlpha) {
            final double movingAverage = speed == StatisticsSpeed.FAST ? throttlingFastMovingAverage : throttlingSlowMovingAverage;
            final double movingVariance = speed == StatisticsSpeed.FAST ? throttlingFastMovingVariance : throttlingSlowMovingVariance;
            double sampleDelta = sample - movingAverage;

            // From formula 122 in https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf
            final double newMovingAverage = movingAverage + exponentialSmoothingAlpha * sampleDelta;

            // From formula 141 in https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf
            final double newMovingVariance = (1.0 - exponentialSmoothingAlpha) * movingVariance
                + exponentialSmoothingAlpha * (sampleDelta) * (sample - newMovingAverage);

            if (speed == StatisticsSpeed.FAST) {
                throttlingFastMovingAverage = newMovingAverage;
                throttlingFastMovingVariance = newMovingVariance;
            } else {
                throttlingSlowMovingAverage = newMovingAverage;
                throttlingSlowMovingVariance = newMovingVariance;
            }
        }

        /**
         * Check whether the stats are within the retention period.
         *
         * @param currentTimeMs The current time in milliseconds.
         * @param retentionPeriodMs The retention period in milliseconds.
         * @return whether the stats are within the retention period.
         */
        private boolean isWithinRetentionPeriod(final long currentTimeMs, final long retentionPeriodMs) {
            final long timeDifferenceMs = currentTimeMs - lastSampleTimestamp;
            return timeDifferenceMs < retentionPeriodMs;
        }

        private double meanPlusSigma(final double standardDeviationsAboveMean) {
            return Math.min(
                Math.max(meanPlusSigma(StatisticsSpeed.FAST, standardDeviationsAboveMean),
                    meanPlusSigma(StatisticsSpeed.SLOW, standardDeviationsAboveMean)), throttlingMaxSample);
        }

        private double meanPlusSigma(final StatisticsSpeed speed,
                                     final double standardDeviationsAboveMean) {
            return speed == StatisticsSpeed.FAST
                ? throttlingFastMovingAverage + Math.sqrt(throttlingFastMovingVariance) * standardDeviationsAboveMean
                : throttlingSlowMovingAverage + Math.sqrt(throttlingSlowMovingVariance) * standardDeviationsAboveMean;
        }

        private ThrottlingCapacityMovingStatistics.Builder serialize() {
            return ThrottlingCapacityMovingStatistics.newBuilder()
                .setVcpuCapacity(vcpuCapacity)
                .setLastSampleTimestamp(lastSampleTimestamp)
                .setSampleCount(sampleCount)
                .setThrottlingMaxSample(throttlingMaxSample)
                .setThrottlingFastMovingAverage(throttlingFastMovingAverage)
                .setThrottlingFastMovingVariance(throttlingFastMovingVariance)
                .setThrottlingSlowMovingAverage(throttlingSlowMovingAverage)
                .setThrottlingSlowMovingVariance(throttlingSlowMovingVariance);
        }

        public String toString(final double standardDeviationsAboveMean) {
            return "\n\t\tmeanPlusSigma: " + meanPlusSigma(standardDeviationsAboveMean)
                + "\n\t\tMax: " + throttlingMaxSample + "; sampleCount" + sampleCount
                + "; lastSampleTimestamp" + lastSampleTimestamp
                + "\n\t\tFastAvg: " + throttlingFastMovingAverage
                + "; FastStdDev: " + Math.sqrt(throttlingFastMovingVariance)
                + "\n\t\tSlowAvg: " + throttlingSlowMovingAverage
                + "; SlowStdDev: " + Math.sqrt(throttlingSlowMovingVariance);
        }
    }
}
