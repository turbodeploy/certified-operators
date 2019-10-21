package com.vmturbo.topology.processor.history.percentile;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Percentile counts for the single stream of data (without time window aspect).
 * - assume the result precision in integers (fractional percents will not be supported)
 * - configurable once (and unchangeable) percent buckets, each of possibly more than single percent
 * - maintain the counts per-bucket and give out the score on request
 */
public class UtilizationCountArray {
    private static final Logger logger = LogManager.getLogger();
    // change of capacity for more than half a % requires rescaling
    private static final float EPSILON = 0.005f;

    private final PercentileBuckets buckets;
    private float capacity = 0f;
    private int[] counts;

    /**
     * Construct the counts array.
     *
     * @param buckets distribution of percent buckets
     */
    public UtilizationCountArray(PercentileBuckets buckets) {
        this.buckets = buckets;
        this.counts = new int[buckets.size()];
    }

    /**
     * Construct a deep copy of <code>other</code> instance.
     *
     * @param other instance to copy from
     * @implNote This constructor copies immutable fields by reference.
     */
    @SuppressWarnings("IncompleteCopyConstructor")
    public UtilizationCountArray(UtilizationCountArray other) {
        buckets = other.buckets;
        capacity = other.capacity;
        counts = Arrays.copyOf(other.counts, other.counts.length);
    }

    /**
     * Add a utilization point to the counts.
     *
     * @param usage usage value
     * @param newCapacity capacity value, if capacity changes from the previous invocation,
     *                    the counts will be proportionally rescaled
     * @param key array identifier for logging
     * @param add whether the count should be added or subtracted
     * @throws HistoryCalculationException when capacity is non-positive
     */
    public void addPoint(float usage, float newCapacity, String key, boolean add) throws HistoryCalculationException {
        if (capacity <= 0d && !add) {
            logger.trace("No percentile counts defined to subtract yet for {}", key);
            return;
        }
        if (newCapacity <= 0d) {
            throw new HistoryCalculationException("Non-positive capacity provided " + newCapacity);
        }
        if (usage < 0) {
            logger.warn("Skipping negative percentile usage point {} for {}", usage, key);
            return;
        }
        if (usage > newCapacity) {
            logger.warn("Percentile usage point {} exceeds capacity {} for {}", usage, capacity, key);
            usage = newCapacity;
        }
        if (capacity != 0d && Math.abs(capacity - newCapacity) > EPSILON && add) {
            // proportionally rescale counts, assume value in the middle of the bucket
            int[] newCounts = new int[counts.length];
            for (int i = 0; i < counts.length; ++i) {
                int newPercent = Math.min(100, (int)Math.ceil(buckets.average(i) * capacity / newCapacity));
                Integer newIndex = buckets.index(newPercent);
                if (newIndex != null && newIndex < counts.length) {
                    newCounts[newIndex] += counts[i];
                } else {
                    logger.warn("Rescaling percentile index {} to capacity {} failed - out of bounds for {}",
                                i, newCapacity, key);
                }
            }
            counts = newCounts;
        }
        if (add) {
            capacity = newCapacity;
        }

        int percent = (int)Math.ceil(Math.abs(usage) * 100 / capacity);
        Integer index = buckets.index(percent);
        if (index != null && index < counts.length) {
            if (add) {
                ++counts[index];
            } else {
                counts[index] = Math.max(0, counts[index] - 1);
            }
        }
    }

    /**
     * Calculate the percentile score for a given rank.
     *
     * @param rank must be between 0 and 100
     * @return percentile score of previously stored points
     * @throws HistoryCalculationException when rank value is invalid
     */
    public int getPercentile(int rank) throws HistoryCalculationException {
        if (rank < 0 || rank > 100) {
            throw new HistoryCalculationException("Requested invalid percentile rank " + rank);
        }
        int total = Arrays.stream(counts).sum();
        int rankIndex = total * rank / 100;
        int score = 0;
        int countToRankIndex = counts[score];
        while (countToRankIndex < rankIndex && score < counts.length) {
            countToRankIndex += counts[++score];
        }
        return (int)Math.ceil(buckets.average(score));
    }

    /**
     * Set the counts from a serialized record.
     *
     * @param record persisted percentile entry record
     * @param key array identifier
     * @throws HistoryCalculationException when passed data are not valid
     */
    public void deserialize(PercentileRecord record, String key) throws HistoryCalculationException {
        if (record.getUtilizationCount() != buckets.size()) {
            throw new HistoryCalculationException("Length " + record.getUtilizationCount()
                                                  + " of serialized percentile counts array is not valid for "
                                                  + key
                                                  + ", expected "
                                                  + buckets.size());
        }
        int i = 0;
        for (Integer count : record.getUtilizationList()) {
            counts[i++] += count;
        }
        capacity = record.getCapacity();
    }

    /**
     * Serialize into the record.
     *
     * @param fieldRef commodity field that this record should be linked to
     * @return percentile record
     */
    public PercentileRecord.Builder serialize(EntityCommodityFieldReference fieldRef) {
        PercentileRecord.Builder builder = PercentileRecord.newBuilder()
                        .setEntityOid(fieldRef.getEntityOid())
                        .setCommodityType(fieldRef.getCommodityType().getType())
                        .setCapacity(capacity);
        if (fieldRef.getProviderOid() != null) {
            builder.setProviderOid(fieldRef.getProviderOid());
        }
        String key = fieldRef.getCommodityType().getKey();
        if (key != null) {
            builder.setKey(key);
        }
        for (int i = 0; i < counts.length; ++i) {
            builder.addUtilization(counts[i]);
        }
        return builder;
    }

    /**
     * Clean up the data.
     */
    public void clear() {
        Arrays.fill(counts, 0);
        capacity = 0f;
    }

    /**
     * Copy counts array from the <code>other</code>.
     *
     * @param other the {@link UtilizationCountArray}
     * @throws HistoryCalculationException when the lengths of the counts arrays do not match
     */
    public void copyCountsFrom(UtilizationCountArray other) throws HistoryCalculationException {
        if (this.counts.length != other.counts.length) {
            throw new HistoryCalculationException(String.format(
                    "The internal %d and external %d the lengths of the counts arrays do not match",
                    this.counts.length, other.counts.length));
        }
        System.arraycopy(other.counts, 0, this.counts, 0, other.counts.length);
    }
}
