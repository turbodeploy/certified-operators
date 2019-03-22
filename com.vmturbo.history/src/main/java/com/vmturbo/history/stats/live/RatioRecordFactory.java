package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.PHYSICAL_MACHINE;

import java.sql.Timestamp;
import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.Record;

import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;

/**
 * Factory/creator class for "ratio" records.
 * See: {@link RatioRecordFactory#makeRatioRecord(Timestamp, String, Map)}.
 */
public class RatioRecordFactory {

    /**
     * Make a "fake" ratio property record. We don't actually record ratio properties in the
     * database, but we insert the fake records here so that the "higher" level
     * aggregation/conversion logic can work with them in the same way that it works with
     * results from the DB.
     *
     * @param timestamp The timestamp to use for the record.
     * @param ratioPropName The name of the ratio property.
     * @param entityTypeCounts (entity type) -> number of entities.
     * @return The fake "record" for the ratio property name.
     */
    @Nonnull
    public Record makeRatioRecord(@Nonnull final Timestamp timestamp,
                                  @Nonnull final String ratioPropName,
                                  @Nonnull final Map<String, Integer> entityTypeCounts) {
        final double ratio;
        switch (ratioPropName) {
            case StringConstants.NUM_VMS_PER_HOST: {
                final int numHosts = entityTypeCounts.getOrDefault(StringConstants.PHYSICAL_MACHINE, 0);
                ratio = numHosts > 0 ?
                    entityTypeCounts.getOrDefault(StringConstants.VIRTUAL_MACHINE, 0) / (double)numHosts : 0;
                break;
            }
            case StringConstants.NUM_VMS_PER_STORAGE: {
                final int numStorages = entityTypeCounts.getOrDefault(StringConstants.STORAGE, 0);
                ratio = numStorages > 0 ?
                    entityTypeCounts.getOrDefault(StringConstants.VIRTUAL_MACHINE, 0) / (double)numStorages : 0;
                break;
            }
            case StringConstants.NUM_CNT_PER_HOST: {
                final int numHosts = entityTypeCounts.getOrDefault(PHYSICAL_MACHINE, 0);
                ratio = numHosts > 0 ?
                    entityTypeCounts.getOrDefault(StringConstants.CONTAINER, 0) / (double)numHosts : 0;
                break;
            }
            case StringConstants.NUM_CNT_PER_STORAGE:
                final int numStorages = entityTypeCounts.getOrDefault(StringConstants.STORAGE, 0);
                ratio = numStorages > 0 ?
                    entityTypeCounts.getOrDefault(StringConstants.CONTAINER, 0) / (double)numStorages : 0;
                break;
            default:
                throw new IllegalStateException("Illegal stat name: " + ratioPropName);
        }

        // We use the "base" table record (instead of the Market table) so that the record
        // has all the fields we expect to be set when processing a list of records.
        // For example, we may try to get the "commodity key" of the record.
        // The "MarketStatsLatestRecord" doesn't have a commodity key.
        final PmStatsLatestRecord countRecord = new PmStatsLatestRecord();
        countRecord.setSnapshotTime(timestamp);
        countRecord.setPropertyType(ratioPropName);
        countRecord.setAvgValue(ratio);
        countRecord.setRelation(RelationType.METRICS);
        return countRecord;
    }
}
