/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;

/**
 * {@link HistUtilizationRecordRecordsAggregator} aggregates {@link HistUtilizationRecord} records
 * to add them into the appropriate latest statistics group.
 */
public class HistUtilizationRecordRecordsAggregator
                extends AbstractRecordsAggregator<HistUtilizationRecord> {

    /**
     * Creates {@link HistUtilizationRecordRecordsAggregator} instance.
     */
    public HistUtilizationRecordRecordsAggregator() {
        super(Collections.singletonMap(StringConstants.RELATED_ENTITY,
                        HistUtilization.HIST_UTILIZATION.PRODUCER_OID));
    }

    @Override
    public void aggregate(@Nonnull HistUtilizationRecord record,
                    @Nonnull Collection<CommodityRequest> commodityRequests,
                    @Nonnull Map<Timestamp, Multimap<String, Record>> statRecordsByTimeByCommodity) {
        final Timestamp maxTimestamp =
                        statRecordsByTimeByCommodity.keySet().stream().max(Timestamp::compareTo)
                                        .orElseGet(() -> Timestamp.from(Instant.now()));
        statRecordsByTimeByCommodity.computeIfAbsent(maxTimestamp, (k) -> HashMultimap.create())
                        .put(createRecordKey(record, commodityRequests), record);
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
