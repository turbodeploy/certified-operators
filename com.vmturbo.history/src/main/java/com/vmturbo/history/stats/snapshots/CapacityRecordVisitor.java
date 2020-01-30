/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Objects;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * {@link CapacityRecordVisitor} visits capacity and effective capacity property in DB record,
 * extracts theirs values and populates {@link StatRecord.Builder} with capacity and reserved
 * values.
 */
@ThreadSafe
public class CapacityRecordVisitor
                extends AbstractVisitor<Record, Pair<StatsAccumulator, StatsAccumulator>> {
    private static final Logger LOGGER = LogManager.getLogger(CapacityRecordVisitor.class);
    private final BiConsumer<StatRecord.Builder, Pair<StatValue, Float>> capacityPopulator;

    /**
     * Creates {@link CapacityRecordVisitor} instance.
     *
     * @param capacityPopulator populates {@link StatRecord.Builder} with capacity
     *                 related stuff.
     */
    public CapacityRecordVisitor(
                    @Nonnull BiConsumer<StatRecord.Builder, Pair<StatValue, Float>> capacityPopulator) {
        super((state) -> {
            state.first.clear();
            state.second.clear();
        });
        this.capacityPopulator = Objects.requireNonNull(capacityPopulator);
    }

    @Override
    public void visit(@Nonnull Record record) {
        final Float capacity =
                        RecordVisitor.getFieldValue(record, StringConstants.CAPACITY, Float.class);
        if (capacity == null) {
            LOGGER.warn("Cannot get '{}' value from '{}'", StringConstants.CAPACITY, record);
            return;
        }
        final Pair<StatsAccumulator, StatsAccumulator> state =
                        ensureState(CapacityRecordVisitor::createAccumulators, record);
        state.first.record(capacity.doubleValue());

        // effective capacity really only makes sense in the context of an
        // actual capacity, so we'll handle it within the capacity clause.
        final Float effectiveCapacity =
                        RecordVisitor.getFieldValue(record, StringConstants.EFFECTIVE_CAPACITY,
                                        Float.class);
        // a null effective capacity should be treated as full "capacity".
        // add one full capacity to the effective capacity total.
        // o/w add the effective capacity.
        state.second.record(effectiveCapacity == null ? capacity : effectiveCapacity);
    }

    @Override
    protected void buildInternally(@Nonnull Builder builder, @Nonnull Record record,
                    @Nonnull Pair<StatsAccumulator, StatsAccumulator> state) {
        final StatsAccumulator capacityAccumulator = state.first;
        final StatsAccumulator effectiveCapacityAccumulator = state.second;
        final float reserved = (float)(capacityAccumulator.getTotal() - effectiveCapacityAccumulator
                        .getTotal());
        capacityPopulator.accept(builder, new Pair<>(capacityAccumulator.toStatValue(), reserved));
    }

    private static Pair<StatsAccumulator, StatsAccumulator> createAccumulators() {
        return new Pair<>(new StatsAccumulator(), new StatsAccumulator());
    }

    /**
     * {@link CapacityPopulator} populates capacity and reserved values in {@link
     * StatRecord.Builder}.
     */
    public static class CapacityPopulator
                    implements BiConsumer<StatRecord.Builder, Pair<StatValue, Float>> {

        @Override
        public void accept(Builder builder, Pair<StatValue, Float> capacityReserved) {
            if (capacityReserved.first != null) {
                builder.setCapacity(capacityReserved.first);
                if (capacityReserved.second != null) {
                    builder.setReserved(capacityReserved.second);
                }
            }
        }
    }
}
