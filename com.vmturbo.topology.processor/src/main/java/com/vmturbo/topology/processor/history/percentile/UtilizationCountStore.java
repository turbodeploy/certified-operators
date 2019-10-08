package com.vmturbo.topology.processor.history.percentile;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
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
    private long latestStoredTimestamp;

    /**
     * Construct the counts history.
     *
     * @param buckets specification of percent buckets
     * @param fieldReference commodity field for which the history is retained
     * @throws HistoryCalculationException when construction fails
     */
    public UtilizationCountStore(PercentileBuckets buckets,
                                 EntityCommodityFieldReference fieldReference)
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
     * Calculate the percentile score for a given rank.
     *
     * @param rank must be between 0 and 100
     * @return percentile score of previously stored points
     * @throws HistoryCalculationException when rank value is invalid
     */
    public int getPercentile(int rank) throws HistoryCalculationException {
        return full.getPercentile(rank);
    }

    /**
     * Add the discovered usage points.
     *
     * @param samples utilizations (not usages) - as sent from mediation
     * @param capacity capacity
     * @param timestamp latest timestamp for the samples
     * @throws HistoryCalculationException when passed data are not correct (non-positive capacity)
     */
    public void addPoints(List<Double> samples, double capacity, long timestamp) throws HistoryCalculationException {
        // prevent double-storing upon broadcast if mediation hasn't changed the value
        if (timestamp <= latestStoredTimestamp) {
            logger.trace("Skipping storing the percentile samples for {} - already present", fieldReference::toString);
            return;
        }
        latestStoredTimestamp = timestamp;
        String key = fieldReference.toString();
        for (Double util : samples) {
            if (util == null) {
                logger.trace("Skipping the null percentile utilization for {}", fieldReference::toString);
            }
            float usage = (float)(util * capacity);
            // in both full observation window and latest between-checkpoints window
            full.addPoint(usage, (float)capacity, key, true);
            latest.addPoint(usage, (float)capacity, key, true);
        }
    }

    /**
     * Store the data from a persisted percentile record into the full window counts array.
     *
     * @param record serialized record
     * @param clear whether to clear the array before adding points
     * @throws HistoryCalculationException when passed data are not valid
     */
    public void addFullCountsRecord(PercentileRecord record, boolean clear) throws HistoryCalculationException {
        if (clear) {
            full.clear();
        }
        full.deserialize(record, fieldReference.toString());
    }

    /**
     * Store the data from a persisted percentile record into the latest window counts array.
     *
     * @param record serialized record
     * @throws HistoryCalculationException when passed data are not valid
     */
    public void setLatestCountsRecord(PercentileRecord record) throws HistoryCalculationException {
        latest.clear();
        latest.deserialize(record, fieldReference.toString());
    }

    /**
     * Serialize the latest window counts array.
     *
     * @return serialized record
     */
    public PercentileRecord.Builder getLatestCountsRecord() {
        return latest.serialize(fieldReference);
    }

    /**
     * Handle the checkpoint of full counts array - when it gets persisted.
     * Subtract the counts of oldest arrays from the full.
     * Clear the latest array.
     *
     * @param oldPages counts arrays for the old periods of time that go out of observation window
     * @return serialized counts array for the entire observation window, to be persisted
     * @throws HistoryCalculationException when passed data are not valid
     */
    public PercentileRecord.Builder checkpoint(Collection<PercentileRecord> oldPages) throws HistoryCalculationException {
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
                float average = buckets.average(i);
                for (int j = 0; j < count; ++j) {
                    full.addPoint(average * oldest.getCapacity() / 100, oldest.getCapacity(),
                                  fieldReference.toString(), false);
                }
            }
        }
        latest.clear();
        return full.serialize(fieldReference);
    }

}
