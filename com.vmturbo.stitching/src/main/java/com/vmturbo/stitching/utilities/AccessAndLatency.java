package com.vmturbo.stitching.utilities;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.AtomicDouble;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;

/**
 * A small helper class that bundles latency and IOPS commodities together.
 *
 * StorageAccess is another term for IOPS (input/output per second)
 */
public class AccessAndLatency {
    public final Optional<Double> latency;

    public final Optional<Double> access;

    public AccessAndLatency(@Nonnull final Optional<Double> optionalAccess,
                            @Nonnull final Optional<Double> optionalLatency) {
        this.latency = Objects.requireNonNull(optionalLatency);
        this.access = Objects.requireNonNull(optionalAccess);
    }

    /**
     * Whether the latency is present in the {@link AccessAndLatency}.
     *
     * @return Whether the latency is present in the {@link AccessAndLatency}.
     */
    public boolean hasLatency() {
        return latency.isPresent();
    }

    /**
     * Check if the {@link AccessAndLatency} has an IOPS value.
     *
     * @return True if the {@link AccessAndLatency} has an IOPS value, false otherwise.
     */
    public boolean hasIops() {
        return access.isPresent();
    }

    /**
     * Get the IOPS value. If not present, returns 0.
     *
     * @return the IOPS value. If not present, returns 0.
     */
    public double iopsValue() {
        return access.orElse(0.0);
    }

    /**
     * Return the IOPS weight. If no IOPS is present, returns a 1.0 to act as a uniform weight.
     *
     * @return The IOPS weight. Returns the IOPS value, or if that is not present, returns 1.0.
     *         in order to calculate a uniform average.
     */
    public double iopsWeight() {
        return access.orElse(1.0);
    }

    /**
     * The latency value weighted by the IOPS value, that is latency*IOPS.
     * If latency is not present, returns empty.
     *
     * @return The IOPS weight, or 1.0 if the IOPS value is not present.
     */
    public Optional<Double> weightedLatency() {
        return latency.map(latency -> latency * access.orElse(1.0));
    }

    @Override
    public String toString() {
        return "Access: " + access + "; Latency: " + latency + "; WeightedLatency: " + weightedLatency();
    }

    /**
     * Construct a new {@link AccessAndLatency} from the related commodities.
     *
     * @param iops The storage access commodity. If the used value on the commodity is less than or equal to zero,
     *             sets a used value of 1.0.
     * @param latency The storage latency commodity.
     * @return An {@link AccessAndLatency} object for the commodities.
     */
    public static AccessAndLatency accessAndLatencyFromCommodityDtos(
        @Nonnull final Optional<CommodityDTO.Builder> iops, @Nonnull final Optional<CommodityDTO.Builder> latency) {
        final Optional<Double> optionalLatency = latency
            .filter(Builder::hasUsed)
            .map(Builder::getUsed);
        final Optional<Double> optionalAccess = iops
            .filter(Builder::hasUsed)
            .map(Builder::getUsed)
            // If the probe cannot measure a real value, it often provides a value of 0 or <0
            // and we will calculate a uniform average.
            .map(value -> value <= 0.0 ? 1.0 : value);

        return new AccessAndLatency(optionalAccess, optionalLatency);
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
    public static double latencyWeightedAveraged(@Nonnull final Stream<AccessAndLatency> accessAndLatencies) {
        // Use AtomicDoubles so that they can be effectively final and mutated in the lambda.
        final AtomicDouble weightedLatencySum = new AtomicDouble(0);
        final AtomicDouble iopsSum = new AtomicDouble(0);

        accessAndLatencies
            .filter(AccessAndLatency::hasLatency)
            .forEach(accessAndLatency -> {
                weightedLatencySum.addAndGet(accessAndLatency.weightedLatency().get());
                iopsSum.addAndGet(accessAndLatency.iopsWeight());
            });

        if (iopsSum.get() == 0) {
            return 0; // Avoid division by 0.
        }

        return weightedLatencySum.get() / iopsSum.get();
    }

    /**
     * Calculate the sum of the (IOPS) values in the stream of access and latencies.
     *
     * @param accessAndLatencies A stream of {@link AccessAndLatency} values whose access (IOPS)
     *                           should be summed.
     * @return The sum of the access (IOPS) values
     */
    public static double accessSum(@Nonnull final Stream<AccessAndLatency> accessAndLatencies) {
        return accessAndLatencies
            .mapToDouble(AccessAndLatency::iopsValue)
            .sum();
    }
}
