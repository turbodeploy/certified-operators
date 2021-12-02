package com.vmturbo.topology.processor.history.percentile;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Utilization counts history for a single commodity field.
 * Use cases:
 * - on 1st broadcast - initialize underlying arrays from the persistent store
 * - upon new data arrival from the mediation (typically 10 min) - store points, serialize latest window array
 * - upon maintenance (typically daily) - update the underlying arrays to reflect the checkpoint
 * - upon observation period change - summarize the full array from a series of smaller ones
 */
public class UtilizationCountStore {
    private static final Logger logger = LogManager.getLogger();

    private final EntityCommodityFieldReference fieldReference;
    private final UtilizationCountArray latest;
    private final UtilizationCountArray full;
    private final PercentileBuckets buckets;
    private int periodDays;

    /**
     * Construct the counts history.
     *
     * @param buckets specification of percent buckets
     * @param fieldReference commodity field for which the history is retained
     * @throws HistoryCalculationException when construction fails
     */
    public UtilizationCountStore(@Nonnull PercentileBuckets buckets,
                    @Nonnull EntityCommodityFieldReference fieldReference)
                    throws HistoryCalculationException {
        if (buckets == null || buckets.size() == 0) {
            throw new HistoryCalculationException("Invalid percentile buckets provided for " + fieldReference);
        }
        latest = new UtilizationCountArray(buckets);
        full = new UtilizationCountArray(buckets);
        this.fieldReference = fieldReference;
        this.buckets = buckets;
    }

    /**
     * Constructs a copy of <code>other</code> instance.
     *
     * @param other instance to copy
     * @implNote This constructor copies immutable fields by reference.
     */
    @SuppressWarnings("IncompleteCopyConstructor")
    public UtilizationCountStore(UtilizationCountStore other) {
        buckets = other.buckets;
        fieldReference = other.fieldReference;
        full = new UtilizationCountArray(other.full);
        latest = new UtilizationCountArray(other.latest);
        this.periodDays = other.periodDays;
    }

    /**
     * Calculate the percentile score for a given rank.
     *
     * @param rank must be between 0 and 100
     * @param reference to the field for which percentile is going to be calculated.
     * @return percentile score of previously stored points, null if no points have been stored
     * @throws HistoryCalculationException when rank value is invalid
     */
    @Nullable
    public synchronized Integer getPercentile(float rank,
                    @Nonnull EntityCommodityFieldReference reference)
                    throws HistoryCalculationException {
        return full.getPercentile(rank, reference);
    }

    /**
     * Checks whether current {@link EntityCommodityFieldReference} has enough historical data or
     * not.
     *
     * @param context pipeline context, including access to commodity builders
     * @param config percentile configuration.
     * @return {@code true} in case minimum observation period is disabled, i.e. it's value
     *                 equal to 0, or in case there are enough data point collected for specified
     *                 minimum observation period in days, otherwise return {@code false}.
     */
    public synchronized boolean isMinHistoryDataAvailable(@Nonnull HistoryAggregationContext context,
                                             @Nonnull PercentileHistoricalEditorConfig config) {

        final long currentTimestamp = config.getClock().millis();
        final String field = fieldReference.toString();
        final int minObservationPeriodDays = config.getMinObservationPeriod(context,
                fieldReference.getEntityOid());
        return latest.isMinHistoryDataAvailable(currentTimestamp, field, minObservationPeriodDays)
                || full.isMinHistoryDataAvailable(currentTimestamp, field, minObservationPeriodDays);
    }

    /**
     * Add the discovered usage points.
     *
     * @param samples percents (not usages or utilizations) - as sent from mediation
     * @param capacity capacity
     * @param timestamp latest timestamp for the samples
     * @throws HistoryCalculationException when passed data are not correct (non-positive capacity)
     */
    public synchronized void addPoints(List<Double> samples, double capacity, long timestamp) throws HistoryCalculationException {
        // prevent double-storing upon broadcast if mediation hasn't changed the value
        if (timestamp <= latest.getEndTimestamp() || timestamp <= full.getEndTimestamp()) {
            logger.trace("Skipping storing the percentile samples for {} - already present", fieldReference::toString);
            return;
        }
        String key = fieldReference.toString();
        for (Double percent : samples) {
            if (percent == null) {
                logger.trace("Skipping the null percentile utilization for {}",
                                fieldReference::toString);
                continue;
            }
            float usage = (float)(percent * capacity / 100);
            // in both full observation window and latest between-checkpoints window
            full.addPoint(usage, (float)capacity, key, timestamp);
            latest.addPoint(usage, (float)capacity, key, timestamp);
        }
    }

    /**
     * Store the data from a persisted percentile record into the full window counts array.
     *
     * @param record serialized record
     * @throws HistoryCalculationException when passed data are not valid
     */
    public synchronized void addFullCountsRecord(PercentileRecord record) throws HistoryCalculationException {
        full.deserialize(record, fieldReference.toString());
    }

    /**
     * Store the data from a persisted percentile record into the latest window counts array.
     *
     * @param record serialized record
     * @throws HistoryCalculationException when passed data are not valid
     */
    public synchronized void addLatestCountsRecord(PercentileRecord record) throws HistoryCalculationException {
        latest.deserialize(record, fieldReference.toString());
    }

    /**
     * Store the data from a persisted percentile record into the latest and full window counts arrays.
     *
     * @param record serialized record
     * @throws HistoryCalculationException when passed data are not valid
     */
    public synchronized void setLatestCountsRecord(PercentileRecord record) throws HistoryCalculationException {
        latest.clear();
        String description = fieldReference.toString();
        latest.deserialize(record, description);
        full.deserialize(record, description);
    }

    /**
     * Serialize the latest window counts array.
     *
     * @return serialized record, null if empty
     */
    public synchronized PercentileRecord.Builder getLatestCountsRecord() {
        return serialize(latest, 1);
    }

    /**
     * Serialize the full window counts array.
     *
     * @return serialized record, null if empty
     */
    public synchronized PercentileRecord.Builder getFullCountsRecord() {
        return serialize(full, periodDays);
    }

    @Nullable
    private PercentileRecord.Builder serialize(@Nonnull UtilizationCountArray full,
                    int periodDays) {
        PercentileRecord.Builder builder = full.serialize(fieldReference);
        return builder == null ? null : builder.setPeriod(periodDays);
    }

    /**
     * Handle the checkpoint of full counts array - when it gets persisted.
     * Subtract the counts of oldest arrays from the full.
     * Clear the latest array.
     *
     * @param oldPages counts arrays for the old periods of time that go out of observation window
     * @param clearLatest should the latest record be cleared.
     * @return serialized counts array for the entire observation window, to be persisted, null if empty
     * @throws HistoryCalculationException when passed data are not valid
     */
    public synchronized PercentileRecord.Builder checkpoint(Collection<PercentileRecord> oldPages,
                                                            boolean clearLatest)
                    throws HistoryCalculationException {
        for (PercentileRecord oldest : oldPages) {
            if (oldest.getUtilizationCount() != buckets.size()) {
                throw new HistoryCalculationException("Length " + oldest.getUtilizationCount()
                                                      + " of serialized percentile counts array is not valid for "
                                                      + fieldReference.toString()
                                                      + ", expected "
                                                      + buckets.size());
            }

            for (int i = 0; i < oldest.getUtilizationCount(); ++i) {
                int count = oldest.getUtilization(i);
                full.removePoint(i, count, oldest.getCapacity(),
                                    oldest.getEndTimestamp(), fieldReference.toString());
            }
        }
        if (clearLatest) {
            latest.clear();
        }
        return serialize(full, periodDays);
    }

    /**
     * Clear the full counts array.
     */
    public synchronized void clearFullRecord() {
        full.clear();
    }

    /**
     * If the last timestamp is older than 90 days and latest is empty, return true so we can clean these records.
     * @param currentTimestamp current moment
     * @return true if this record is empty or older than 90 days.
     */
    public synchronized boolean isEmptyOrOutdated(long currentTimestamp) {
        return full.isEmptyOrOutdated(currentTimestamp) && latest.isEmpty();
    }

    public int getPeriodDays() {
        return periodDays;
    }

    public void setPeriodDays(int periodDays) {
        this.periodDays = periodDays;
    }

    @Override
    public synchronized String toString() {
        return UtilizationCountStore.class.getSimpleName() + "{fieldReference=" + fieldReference +
                        ", full=" + full + ", latestStoredTimestamp=" + latest.getEndTimestamp() +
                        ", periodDays=" + periodDays + '}';
    }

    /**
     * This uses the toDebugString to be used for debug logging with more details.
     * @return the string representation of the utilization store.
     */
    public synchronized String toDebugString() {
        return UtilizationCountStore.class.getSimpleName() + "{fieldReference=" + fieldReference +
                        ", full=" + full.toDebugString() +
                        ", latest=" + latest.toDebugString() +
                        ", latestStoredTimestamp=" + latest.getEndTimestamp() +
                        ", periodDays=" + periodDays + '}';
    }

}
