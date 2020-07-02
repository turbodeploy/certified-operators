package com.vmturbo.topology.processor.history.percentile;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import javax.annotation.Nonnull;

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
    /**
     * String representation of non-initialized {@link UtilizationCountArray} instance.
     */
    protected static final String EMPTY = "{empty}";
    private static final Logger logger = LogManager.getLogger();
    // change of capacity for more than half a % requires rescaling
    private static final float EPSILON = 0.005f;

    private final PercentileBuckets buckets;
    private float capacity = 0f;
    private int[] counts;
    private long startTimestamp;
    private long endTimestamp;

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
        endTimestamp = other.endTimestamp;
        startTimestamp = other.startTimestamp;
        buckets = other.buckets;
        capacity = other.capacity;
        counts = Arrays.copyOf(other.counts, other.counts.length);
    }

    /**
     * Returns information about latest timestamp stored in database.
     *
     * @return timestamp of the latest point stored in database.
     */
    public long getEndTimestamp() {
        return endTimestamp;
    }

    /**
     * Checks whether current {@link EntityCommodityFieldReference} has enough historical data or
     * not.
     *
     * @param currentTimestamp current timestamp
     * @param fieldReference reference to the field for which count array is
     *                 stored.
     * @param minObservationPeriodDays minimum amount of days for which entity
     *                 should have percentile data.
     * @return {@code true} in case minimum observation period is disabled, i.e. it's value
     *                 equal to 0, or in case there are enough data point collected for specified
     *                 minimum observation period in days, otherwise return {@code false}.
     */
    public boolean isMinHistoryDataAvailable(long currentTimestamp, @Nonnull String fieldReference,
                    int minObservationPeriodDays) {
        if (minObservationPeriodDays <= 0) {
            return true;
        }

        final Instant now = Instant.ofEpochMilli(currentTimestamp);
        final long minTimestampSinceWhichHistory =
                        now.minus(Duration.ofDays(minObservationPeriodDays)).toEpochMilli();
        if (startTimestamp <= 0) {
            logger.debug("Percentile data is not initialized. Requested timestamp is {} and key is {}",
                         currentTimestamp,
                         fieldReference);
            return false;
        }
        final boolean historyDataAvailable =
                        minTimestampSinceWhichHistory > startTimestamp;
        logger.debug("Percentile data available for '{}' since '{}'. Now minus {} days is '{}'.",
                        () -> fieldReference, () -> Instant.ofEpochMilli(startTimestamp),
                        () -> minObservationPeriodDays,
                        () -> Instant.ofEpochMilli(minTimestampSinceWhichHistory));
        return historyDataAvailable;
    }

    /**
     * Add a utilization point to the counts.
     *
     * @param usage usage value
     * @param newCapacity capacity value, if capacity changes from the previous invocation,
     *                    the counts will be proportionally rescaled
     * @param key array identifier for logging
     * @param add whether the count should be added or subtracted
     * @param timestamp of the point which is currently processing
     * @throws HistoryCalculationException when capacity is non-positive
     */
    public void addPoint(float usage, float newCapacity, String key, boolean add, long timestamp) throws HistoryCalculationException {
        final boolean remove = !add;
        if (capacity <= 0d && remove) {
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
        if (startTimestamp == 0 || remove && timestamp > startTimestamp) {
            logger.trace("Updating start timestamp from {} to {} during {} operation for key {}",
                        startTimestamp,
                        timestamp,
                        add ? "add" : "remove",
                        key);
            startTimestamp = timestamp;
        }
        if (usage > newCapacity) {
            logger.warn("Percentile usage point {} exceeds capacity {} for {}", usage, capacity, key);
            usage = newCapacity;
        }
        int percent = (int)Math.ceil(Math.abs(usage) * 100 / newCapacity);
        if (add) {
            rescaleCountsIfNecessary(newCapacity, key);
            capacity = newCapacity;
            endTimestamp = timestamp;
        } else if (capacity != 0d && Math.abs(capacity - newCapacity) > EPSILON) {
            // reverse-rescale the value being subtracted
            percent = Math.min(100, (int)Math.ceil(buckets.average(percent) * newCapacity / capacity));
        }

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
    public int getPercentile(float rank) throws HistoryCalculationException {
        if (rank < 0 || rank > 100) {
            throw new HistoryCalculationException("Requested invalid percentile rank " + rank);
        }
        int total = Arrays.stream(counts).sum();
        int rankIndex = (int)(total * rank / 100);
        int score = 0;
        int countToRankIndex = counts[score];
        while (countToRankIndex < rankIndex && score < counts.length) {
            countToRankIndex += counts[++score];
        }
        return (int)Math.ceil(buckets.average(score));
    }

    /**
     * Add up the counts from a serialized record.
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
        rescaleCountsIfNecessary(record.getCapacity(), key);
        int i = 0;
        for (Integer count : record.getUtilizationList()) {
            counts[i++] += count;
        }
        capacity = record.getCapacity();
        endTimestamp = record.getEndTimestamp();
        startTimestamp = record.getStartTimestamp();
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
                        .setCapacity(capacity)
                        .setEndTimestamp(endTimestamp)
                        .setStartTimestamp(startTimestamp);
        if (fieldRef.getProviderOid() != null) {
            builder.setProviderOid(fieldRef.getProviderOid());
        }
        if (fieldRef.getCommodityType().hasKey()) {
            builder.setKey(fieldRef.getCommodityType().getKey());
        }
        for (int count : counts) {
            builder.addUtilization(count);
        }
        return builder;
    }

    /**
     * Clean up the data.
     */
    public void clear() {
        logger.trace("Cleared array with capacity {}, startTimestamp {} and endTimestamp {}",
                     capacity,
                     startTimestamp,
                     endTimestamp);
        Arrays.fill(counts, 0);
        capacity = 0f;
        startTimestamp = 0;
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

    @Override
    public String toString() {
        return createToString(true);
    }

    /**
     * This uses the toDebugString in order to include the counts in the resulting string.
     * @return the string representation of the utilization store including the counts data.
     */
    public String toDebugString() {
        return createToString(false);
    }

    private String createToString(boolean withoutCounts) {
        return String.format("%s#%s", UtilizationCountArray.class.getSimpleName(),
                        getFieldDescriptions(withoutCounts));
    }

    private String getFieldDescriptions(boolean withoutCounts) {
        final boolean notInitialized = capacity == 0;
        if (notInitialized) {
            return EMPTY;
        }
        if (withoutCounts) {
            return String.format("{capacity=%s}", capacity);
        }
        return String.format("{capacity=%s; counts=%s}", capacity, Arrays.toString(counts));
    }

    private void rescaleCountsIfNecessary(float newCapacity, String key) {
        if (capacity != 0D && newCapacity != 0D && Math.abs(capacity - newCapacity) > EPSILON) {
            // proportionally rescale counts, assume value in the middle of the bucket
            logger.trace("Rescaling percentile counts for {} due to capacity change from {} to {}",
                         key::toString, () -> capacity, () -> newCapacity);
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
    }
}
