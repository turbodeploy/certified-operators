/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.HistoryUtilizationType;

/**
 * {@link HistUtilizationRecordVisitor} visits {@link HistUtilizationRecord}s only and extracts from
 * them appropriate used and capacity values.
 */
@NotThreadSafe
public class HistUtilizationRecordVisitor extends
                AbstractVisitor<HistUtilizationRecord, Map<Integer, Pair<StatsAccumulator, StatsAccumulator>>> {
    private static final Logger LOGGER = LogManager.getLogger(HistUtilizationRecordVisitor.class);
    private final HistoryUtilizationType supportedType;

    /**
     * Creates {@link HistUtilizationRecordVisitor} instance.
     *
     * @param supportedType value of {@link HistoryUtilizationType} that will be
     *                 processed by instance of the visitor.
     */
    public HistUtilizationRecordVisitor(@Nonnull HistoryUtilizationType supportedType) {
        super(Map::clear);
        this.supportedType = Objects.requireNonNull(supportedType);
    }

    @Override
    public void visit(@Nonnull HistUtilizationRecord record) {
        final HistoryUtilizationType historyUtilizationType = getHistoryUtilizationType(record);
        if (supportedType != historyUtilizationType) {
            return;
        }
        final Integer propertySlot = record.getPropertySlot();
        if (propertySlot == null) {
            LOGGER.error("Cannot get property slot from '{}' record", record);
            return;
        }
        final Pair<StatsAccumulator, StatsAccumulator> usageCapacityAccumulators =
                        ensureState(TreeMap::new, record).computeIfAbsent(propertySlot,
                                        (k) -> new Pair<>(new StatsAccumulator(),
                                                        new StatsAccumulator()));
        final Double capacity =
                        RecordVisitor.getFieldValue(record, StringConstants.CAPACITY, Double.class);
        final BigDecimal utilization = record.getUtilization();
        if (capacity == null || utilization == null) {
            LOGGER.error("Cannot get capacity/utilization from '{}' record", record);
            return;
        }
        usageCapacityAccumulators.first.record(utilization.doubleValue() * capacity);
        usageCapacityAccumulators.second.record(capacity);
    }

    @Nullable
    private static HistoryUtilizationType getHistoryUtilizationType(@Nonnull HistUtilizationRecord record) {
        final Integer valueType = record.getValueType();
        try {
            return HistoryUtilizationType.forNumber(valueType);
        } catch (VmtDbException ex) {
            LOGGER.error("Cannot find '{}' value by '{}' identifier for '{}' record",
                            HistoryUtilizationType.class.getSimpleName(), valueType, record, ex);
        }
        return null;
    }

    @Override
    protected void buildInternally(@Nonnull Builder builder, @Nonnull Record record,
                    @Nonnull Map<Integer, Pair<StatsAccumulator, StatsAccumulator>> state) {
        state.forEach((propertySlot, usageCapacityAccumulators) -> {
            final StatValue usedStatValue = getStatValue(usageCapacityAccumulators.first);
            final StatValue capacityStatValue = getStatValue(usageCapacityAccumulators.second);
            if (capacityStatValue != null && usedStatValue != null) {
                builder.addHistUtilizationValue(HistUtilizationValue.newBuilder()
                                .setType(supportedType.getApiParameterName())
                                .setCapacity(capacityStatValue).setUsage(usedStatValue).build());
            }
        });
    }

    @Nullable
    private static StatValue getStatValue(@Nonnull StatsAccumulator accumulator) {
        return accumulator.getCount() > 0 ? accumulator.toStatValue() : null;
    }
}
