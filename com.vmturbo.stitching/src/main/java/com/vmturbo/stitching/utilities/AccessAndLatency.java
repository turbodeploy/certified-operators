package com.vmturbo.stitching.utilities;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * A small helper class that bundles latency and IOPS commodities together.
 *
 * StorageAccess is another term for IOPS (input/output per second)
 */
public class AccessAndLatency {
    public final Optional<Double> latencyUsed;

    public final Optional<Double> iopsUsed;

    public AccessAndLatency(@Nonnull final Optional<CommodityDTO.Builder> latency,
                            @Nonnull final Optional<CommodityDTO.Builder> iops) {
        latencyUsed = latency
            .filter(CommodityDTO.Builder::hasUsed)
            .map(CommodityDTO.Builder::getUsed);
        iopsUsed = iops
            .filter(CommodityDTO.Builder::hasUsed)
            .map(CommodityDTO.Builder::getUsed)
                // If the probe cannot measure a real value, it often provides a value of 0 or <0
                // and we will calculate a uniform average.
            .map(value -> value <= 0.0 ? 1.0 : value);
    }

    /**
     * Whether the latency is present in the {@link AccessAndLatency}.
     *
     * @return Whether the latency is present in the {@link AccessAndLatency}.
     */
    public boolean hasLatency() {
        return latencyUsed.isPresent();
    }

    /**
     * Return the IOPS weight. If no IOPS is present, returns a 1.0 to act as a uniform weight.
     *
     * @return The IOPS weight. Returns the IOPS value, or if that is not present, returns 1.0.
     *         in order to calculate a uniform average.
     */
    public double iopsWeight() {
        return iopsUsed.orElse(1.0);
    }

    /**
     * The latency value weighted by the IOPS value, that is latency*IOPS.
     * If latency is not present, returns empty.
     *
     * @return The IOPS weight, or 1.0 if the IOPS value is not present.
     */
    public Optional<Double> weightedLatency() {
        return latencyUsed.map(latency -> latency * internalIopsWeight());
    }

    /**
     * A small helper method that always returns the IOPS weight, regardless of the value of the latency weight.
     *
     * @return returns the IOPS value, or if that is not present, returns 1.0.
     */
    private double internalIopsWeight() {
        return iopsUsed.orElse(1.0);
    }

    /**
     * Calculate the IOPS-weighted average of storage latency for a collection of {@link AccessAndLatency}.
     *
     * Per Rich Hammond:
     * The storage probe group has moved to use an IOPS weighted average of the latency values.
     * We found that using max [or even uniform average] resulted in market actions which made no sense to
     * the customer. The intent of the IOPS weighted average is to solve the request size versus latency issue.
     * We cannot tell the size of individual requests, but larger requests, which have higher latency,
     * should not occur as frequently, so weighting by IOPS count will give greater weight to the small,
     * fast, queries without ignoring the larger, slower queries.
     *
     * @return The IOPS-weighted average of storage latency for a collection of {@link AccessAndLatency}.
     */
    public static double latencyWeightedAveraged(@Nonnull final Collection<AccessAndLatency> accessAndLatencies) {
        final double weightedLatencySum = accessAndLatencies.stream()
            .map(AccessAndLatency::weightedLatency)
            .filter(Optional::isPresent)
            .mapToDouble(Optional::get)
            .sum();
        final double iopsSum = accessAndLatencies.stream()
            .filter(AccessAndLatency::hasLatency)
            .mapToDouble(AccessAndLatency::iopsWeight)
            .sum();

        if (iopsSum == 0) {
            return 0; // Avoid division by 0.
        }

        return weightedLatencySum / iopsSum;
    }
}
