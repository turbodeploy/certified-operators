/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.stats.PropertySubType;

/**
 * {@link StatsRecordsAggregator} aggregates records from stats tables, basing on API request
 * parameters.
 */
public class StatsRecordsAggregator extends AbstractRecordsAggregator<Record> {
    private static final Logger LOGGER = LogManager.getLogger(StatsRecordsAggregator.class);
    private final Collection<Class<? extends Record>> excludingRecordTypes;

    /**
     * Creates {@link StatsRecordsAggregator} instance.
     *
     * @param excludingRecordTypes types of DB records that need to be excluded from
     *                 stats processing.
     */
    public StatsRecordsAggregator(Collection<Class<? extends Record>> excludingRecordTypes) {
        super(Collections.singletonMap(StringConstants.RELATED_ENTITY, STATS_TABLE.PRODUCER_UUID));
        this.excludingRecordTypes = excludingRecordTypes;
    }

    @Override
    public void aggregate(@Nonnull Record record,
                    @Nonnull Collection<CommodityRequest> commodityRequests,
                    @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity) {
        // organize the statRecords by SNAPSHOT_TIME and then by PROPERTY_TYPE + PROPERTY_SUBTYPE
        if (excludingRecordTypes.stream().anyMatch(type -> type.isInstance(record))) {
            return;
        }
        // Filter out the utilization as we are interested in the used values
        final String dbPropertySubType =
                        record.getValue(StringConstants.PROPERTY_SUBTYPE, String.class);
        final PropertySubType propertySubType = PropertySubType.fromApiParameter(dbPropertySubType);
        if (propertySubType == null) {
            LOGGER.warn("Cannot find appropriate '{}' value for '{}' property subtype from DB record {}.",
                            PropertySubType.class.getSimpleName(), dbPropertySubType, record);
        }
        final Timestamp snapshotTime =
                        record.getValue(StringConstants.SNAPSHOT_TIME, Timestamp.class);
        final Multimap<String, Record> snapshotMap = statRecordsByTimeByCommodity
                        .computeIfAbsent(snapshotTime, k -> HashMultimap.create());
        snapshotMap.put(createRecordKey(record, commodityRequests), record);
    }

    @Nonnull
    @Override
    protected String getRelation(@Nonnull Record record) {
        return record.getValue(StringConstants.RELATION, String.class);
    }

    @Nonnull
    @Override
    protected String getPropertyType(@Nonnull Record record) {
        return record.getValue(StringConstants.PROPERTY_TYPE, String.class);
    }
}
