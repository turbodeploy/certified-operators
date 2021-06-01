/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;

/**
 * {@link HistUtilizationRecordRecordsAggregator} aggregates {@link HistUtilizationRecord} records
 * to add them into the appropriate latest statistics group.
 */
public class HistUtilizationRecordRecordsAggregator
                extends AbstractRecordsAggregator<HistUtilizationRecord> {

    /**
     * Creates {@link HistUtilizationRecordRecordsAggregator} instance.
     *
     * @param commodityRequests which contains information that manages aggregation process.
     */
    public HistUtilizationRecordRecordsAggregator(
                    @Nonnull Collection<CommodityRequest> commodityRequests) {
        super(commodityRequests);
    }

    @Override
    public void aggregate(@Nonnull HistUtilizationRecord record, @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity) {
        final Timestamp maxTimestamp =
                        statRecordsByTimeByCommodity.keySet().stream().max(Timestamp::compareTo)
                                        .orElseGet(() -> Timestamp.from(Instant.now()));
        statRecordsByTimeByCommodity.computeIfAbsent(maxTimestamp, (k) -> HashMultimap.create())
                        .put(createRecordKey(record), record);
    }

    @Nonnull
    @Override
    protected String getRelation(@Nonnull HistUtilizationRecord record) {
        final Long producerId = record.getProducerOid();
        return (producerId != null && producerId.longValue() > 0L)
                            ? RelationType.COMMODITIESBOUGHT.getLiteral()
                            : RelationType.COMMODITIES.getLiteral();
    }

    @Nonnull
    @Override
    protected String getPropertyType(@Nonnull HistUtilizationRecord record) {
        return UICommodityType.fromType(record.getPropertyTypeId()).apiStr();
    }
}
