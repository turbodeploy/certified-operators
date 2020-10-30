package com.vmturbo.topology.processor.history.percentile;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

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
 * Expected consistency state: 0 capacity and 0 timestamps - uninitialized.
 * (such entries are required to exist to mark them for later processing)
 * Once a point is added, the timestamps and capacity should also be present i.e. > 0.
 */
public class UtilizationCountArray {
    /**
     * String representation of non-initialized {@link UtilizationCountArray} instance.
     */
    protected static final String EMPTY = "{empty}";
    private static final Logger logger = LogManager.getLogger();
    // change of capacity for more than half a % requires rescaling
    private static final float EPSILON = 0.005F;

    private final PercentileBuckets buckets;
    private int[] counts;
    private long startTimestamp;
    private long endTimestamp;
    private CapacityChangeHistory capacityList;

    /**
     * Construct the counts array.
     *
     * @param buckets distribution of percent buckets
     */
    public UtilizationCountArray(PercentileBuckets buckets) {
        this.buckets = buckets;
        this.counts = new int[buckets.size()];
        this.capacityList = new CapacityChangeHistory();
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
        counts = Arrays.copyOf(other.counts, other.counts.length);
        capacityList = new CapacityChangeHistory(other.capacityList);
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
     * @param timestamp of the point which is currently processing
     */
    public void addPoint(float usage, float newCapacity, String key, long timestamp) {
        if (newCapacity <= 0F) {
            logger.trace("Skipping non-positive capacity usage point for " + key + ": " + newCapacity);
            return;
        }

        if (usage < 0F) {
            logger.trace("Skipping negative percentile usage point {} for {}", usage, key);
            return;
        }

        if (Double.isNaN(usage)) {
            logger.trace("Skipping NaN percentile usage point {} for {}", usage, key);
            return;
        }

        if (usage > newCapacity) {
            // TODO consider accumulating the number of these warnings in the context and printing in the end of the stage
            logger.trace("Percentile usage point {} exceeds capacity {} for {}", usage, newCapacity, key);
            usage = newCapacity;
        }

        if (startTimestamp == 0) {
            logger.trace("Updating start timestamp from 0 to {} during add operation for key {}",
                        timestamp,
                        key);
            startTimestamp = timestamp;
        }
        endTimestamp = timestamp;

        // rescale the existing datapoints in necessary
        rescaleCountsIfNecessary(newCapacity, key);

        // If the capacity has changed, record it
        capacityList.add(timestamp, newCapacity);

        // increment the associated bucket
        Integer index = buckets.index((int)Math.ceil(Math.abs(usage) * 100 / newCapacity));
        if (index != null && index < counts.length) {
            ++counts[index];
        }
    }

    /**
     * Decrement a datapoint from a bucket in the count array.
     *
     * @param dailyRecordIndex the index of the bucket that
     * @param numberToDecrement the number of times to decrement.
     * @param dailyRecordCapacity the capacity at the time this datapoint captured.
     * @param timestamp the timestamp for the datapoint.
     * @param key array identifier for logging.
     */
    public void removePoint(int dailyRecordIndex, int numberToDecrement, float dailyRecordCapacity,
                            long timestamp, String key) {
        if (isEmpty()) {
            logger.trace("No percentile counts defined to subtract yet for {}", key);
            return;
        }

        if (dailyRecordCapacity <= 0F) {
            logger.warn("Skipping non-positive capacity usage point for " + key + ": " + dailyRecordCapacity);
            return;
        }

        if (dailyRecordIndex < 0) {
            logger.error("Trying to remove a point negative index from percentile record of {} ",
                key);
            return;
        }

        // update the timestamp
        if (startTimestamp == 0 || timestamp > startTimestamp) {
            logger.trace("Updating start timestamp from {} to {} during remove operation for key {}",
                startTimestamp,
                timestamp,
                key);
            startTimestamp = timestamp;
        }

        // remove the capacity changes that are no longer relevant
        capacityList.removeOlderCapacities(timestamp);

        // find what is the bucket index that should be decremented
        final int bucketToDecrement = getBucketToDecrement(dailyRecordIndex, dailyRecordCapacity);

        if (counts[bucketToDecrement] >= numberToDecrement) {
            counts[bucketToDecrement] -= numberToDecrement;
        } else {
            // something is wrong. log an error
            // TODO (mahdi) it may be good idea to to flag this record as invalid and therefore
            //  should be re-calculated.
            logger.error("The is no datapoint to decrement for {} at index {}. This indicates "
                + "corrupted percentile record.", key, bucketToDecrement);
            counts[bucketToDecrement] = 0;
        }
    }

    /**
     * Finds the index to decrement from the full record.
     * NOTE: It is expected that the capacities with older timestamp that record capacity has
     * been trimmed from capacity list.
     *
     * @param dailyRecordIndex the index in daily record.
     * @param dailyRecordCapacity the capacity in the daily record.
     * @return the index to decrement from the full record.
     */
    private int getBucketToDecrement(int dailyRecordIndex, float dailyRecordCapacity) {
        final int bucketToDecrement;
        if (!isEmpty()) {
            // replay the changes to come up with final index
            int currentBucketIndex = dailyRecordIndex;
            float currentCapacity = dailyRecordCapacity;
            for (float newCapacity : capacityList.capacities) {
                if (Math.abs(currentCapacity - newCapacity) > EPSILON) {
                    currentBucketIndex = getBucketIndexAfterScaling(currentBucketIndex,
                        currentCapacity, newCapacity);
                    currentCapacity = newCapacity;
                }
            }
            bucketToDecrement = currentBucketIndex;
        } else {
            bucketToDecrement = getBucketIndexAfterScaling(dailyRecordIndex,
                dailyRecordCapacity, capacityList.getCapacity());
        }
        return bucketToDecrement;
    }

    /**
     * Gets the index of the bucket that a datapoint has been moved to after a capacity change.
     *
     * @param currentBucketIndex current bucket index.
     * @param currentCapacity current capacity.
     * @param newCapacity the new capacity.
     * @return the new index.
     */
    private int getBucketIndexAfterScaling(int currentBucketIndex, float currentCapacity,
                                           float newCapacity) {
        int currentUtilization = Math.min(100,
            (int)Math.ceil(buckets.average(currentBucketIndex) * currentCapacity / newCapacity));
        return buckets.index(currentUtilization);
    }

    /**
     * Calculate the percentile score for a given rank.
     *
     * @param rank must be between 0 and 100
     * @return percentile score of previously stored points, null if no counts have been recorded
     * @throws HistoryCalculationException when rank value is invalid
     */
    @Nullable
    public Integer getPercentile(float rank) throws HistoryCalculationException {
        if (rank < 0 || rank > 100) {
            throw new HistoryCalculationException("Requested invalid percentile rank " + rank);
        }
        int total = Arrays.stream(counts).sum();
        if (total == 0) {
            return null;
        }
        int rankIndex = (int)Math.ceil(total * rank / 100);
        int score = 0;
        int countToRankIndex = counts[score];
        while (countToRankIndex < rankIndex) {
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

        if (record.getCapacityChangesCount() > 0) {
            // replaying the capacity changes
            for (PercentileRecord.CapacityChange capacityChange : record.getCapacityChangesList()) {
                float newCapacity = capacityChange.getNewCapacity();
                if (newCapacity > 0F) {
                    rescaleCountsIfNecessary(newCapacity, key);
                    capacityList.add(capacityChange.getTimestamp(), newCapacity);
                }
            }
        } else if (record.getCapacity() > 0F) {
            // for backward compatibility with older serialized entries
            rescaleCountsIfNecessary(record.getCapacity(), key);
            capacityList.add(record.getEndTimestamp(), record.getCapacity());
        } else {
            // we may sometimes have 0 capacity uninitialized records in the db
            // but they should also have no counts, consequently there's nothing to add up
            logger.trace("Skipping deserialization of a record {} with non-positive capacity {}, current {}",
                () -> key, record::getCapacity, capacityList::getCapacity);
            return;
        }

        int i = 0;
        for (Integer recordUtilization : record.getUtilizationList()) {
            counts[i++] += recordUtilization;
        }

        // Update existing startTimestamp and endTimestamp based on given serialized record
        startTimestamp = startTimestamp == 0 ? record.getStartTimestamp() : Math.min(startTimestamp, record.getStartTimestamp());
        endTimestamp = Math.max(endTimestamp, record.getEndTimestamp());
    }

    /**
     * Serialize into the record.
     *
     * @param fieldRef commodity field that this record should be linked to
     * @return percentile record, null if the entry contains no data
     */
    public PercentileRecord.Builder serialize(EntityCommodityFieldReference fieldRef) {
        if (isEmpty()) {
            return null;
        }
        PercentileRecord.Builder builder = PercentileRecord.newBuilder()
                        .setEntityOid(fieldRef.getEntityOid())
                        .setCommodityType(fieldRef.getCommodityType().getType())
                        .setCapacity(capacityList.getCapacity())
                        .setEndTimestamp(endTimestamp)
                        .setStartTimestamp(startTimestamp)
                        .addAllCapacityChanges(capacityList.serialize());

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
        Arrays.fill(counts, 0);
        capacityList.clear();
        startTimestamp = 0;
        endTimestamp = 0;
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

    /**
     * Whether the array is empty.
     *
     * @return true if no points have ever been added
     */
    public boolean isEmpty() {
        return capacityList.isEmpty();
    }

    private String createToString(boolean withoutCounts) {
        return String.format("%s#%s", UtilizationCountArray.class.getSimpleName(),
                        getFieldDescriptions(withoutCounts));
    }

    private String getFieldDescriptions(boolean withoutCounts) {
        if (isEmpty()) {
            return EMPTY;
        }
        if (withoutCounts) {
            return String.format("{capacity=%s}", capacityList.toString());
        }
        return String.format("{capacity=%s; counts=%s}", capacityList.toDebugString(), Arrays.toString(counts));
    }

    private void rescaleCountsIfNecessary(float newCapacity, String key) {
        if (shouldRescale(newCapacity)) {
            rescaleCounts(counts.length, capacityList.getCapacity(), newCapacity, key);
        }
    }

    private void rescaleCounts(int size,
                                float currentCapacity,
                                float newCapacity,
                                String loggingKey) {
        // proportionally rescale counts, assume value in the middle of the bucket
        logger.trace("Rescaling percentile counts for {} due to capacity change from {} to {}",
            () -> loggingKey, () -> currentCapacity, () -> newCapacity);
        int[] newCounts = new int[size];

        for (int i = 0; i < size; i++) {
            int newPercent = Math.min(100,
                (int)Math.ceil(buckets.average(i) * currentCapacity / newCapacity));
            Integer newIndex = buckets.index(newPercent);
            if (newIndex != null && newIndex < size) {
                newCounts[newIndex] += counts[i];
            } else {
                logger.warn("Rescaling percentile index {} to capacity {} failed - out of bounds for {}",
                    i, newCapacity, loggingKey);
            }
        }
        counts = newCounts;
    }

    private boolean shouldRescale(float newCapacity) {
        return !isEmpty() && newCapacity != 0F
            && Math.abs(capacityList.getCapacity() - newCapacity) > EPSILON;
    }

    /**
     * Keeps the changes of the capacity for a commodity over time.
     */
    private static class CapacityChangeHistory {
        private LongList timestamps;
        private FloatList capacities;

        CapacityChangeHistory() {
            timestamps = new LongArrayList();
            capacities = new FloatArrayList();
        }

        CapacityChangeHistory(CapacityChangeHistory capacityChangeHistory) {
            timestamps = new LongArrayList(capacityChangeHistory.timestamps);
            capacities = new FloatArrayList(capacityChangeHistory.capacities);
        }

        public boolean add(long timestamp, float newCapacity) {
            if (Math.abs(newCapacity - getCapacity()) > EPSILON) {
                if (!timestamps.isEmpty()
                    && timestamp < timestamps.getLong(timestamps.size() - 1)) {
                    // This may happen if the time on appliance changes.
                    logger.warn("The capacities should be added in order to "
                        + "utilization array. Received a timestamp of {} while the oldest "
                        + "timestamp in array is {}.", timestamp,
                        timestamps.getLong(timestamps.size() - 1));
                }

                timestamps.add(timestamp);
                capacities.add(newCapacity);
                return true;
            } else {
                return false;
            }
        }

        public boolean isEmpty() {
            return timestamps.isEmpty();
        }

        public void clear() {
            timestamps.clear();
            capacities.clear();
        }

        @Override
        public String toString() {
            return String.valueOf(getCapacity());
        }

        public String toDebugString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            String separator = "";
            for (int i = 0; i < timestamps.size(); i++) {
                sb.append(separator);
                separator = ",";
                sb.append("{\"timestamp\": ");
                sb.append(timestamps.getLong(i));
                sb.append(", \"newCapacity\": \"");
                sb.append(capacities.getFloat(i));
                sb.append("\"}");
            }
            sb.append("]");
            return sb.toString();
        }

        public void removeOlderCapacities(long timestamp) {
            if (timestamps.size() < 1 || timestamps.getLong(0) >= timestamp) {
                return;
            }

            int endIndex = 0;
            while (endIndex < timestamps.size() - 1 && timestamps.getLong(endIndex) < timestamp) {
                endIndex++;
            }

            timestamps.removeElements(0, endIndex);
            capacities.removeElements(0, endIndex);
        }

        public float getCapacity() {
            return isEmpty() ? 0F : capacities.getFloat(capacities.size() - 1);
        }

        /**
         * Converts the values in this object to a iterable of DTO objects that can be serialized.
         *
         * @return the iterable on DTO objects.
         */
        public Iterable<? extends PercentileRecord.CapacityChange> serialize() {
            List<PercentileRecord.CapacityChange> serialized = new ArrayList<>(timestamps.size());
            for (int i = 0; i < timestamps.size(); i++) {
                serialized.add(PercentileRecord.CapacityChange.newBuilder()
                    .setTimestamp(timestamps.getLong(i))
                    .setNewCapacity(capacities.getFloat(i))
                    .build());
            }
            return serialized;
        }
    }
}
