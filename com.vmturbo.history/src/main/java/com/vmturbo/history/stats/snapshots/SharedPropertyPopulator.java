/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;

/**
 * {@link SharedPropertyPopulator} provides methods common for populators that are going to set
 * properties from different DB records.
 *
 * @param <V> type of the value that is going to be populated into {@link
 *                 StatRecord.Builder}.
 */
public abstract class SharedPropertyPopulator<V> implements BiConsumer<StatRecord.Builder, V> {

    /**
     * Checks whether we need to set property value in {@link StatRecord.Builder}.
     *
     * @param parameterInitialized should be passed as {@code true} in case
     *                 parameter has been already initialized in {@link StatRecord.Builder}.
     * @param record record from which we are going to populate data.
     * @return {@code true} in case we need to call setter for the property value.
     */
    protected static boolean whetherToSet(boolean parameterInitialized, @Nullable Record record) {
        return !parameterInitialized || !HistUtilizationRecord.class.isInstance(record);
    }

    /**
     * Populates value collected from the specified record instance.
     *
     * @param builder builder instance which value will be populated
     * @param value that will be used for population
     * @param record DB record from which this value has been extracted.
     */
    public abstract void accept(@Nonnull StatRecord.Builder builder, @Nullable V value,
                    @Nullable Record record);

    @Override
    public void accept(@Nonnull StatRecord.Builder builder, @Nullable V v) {
        accept(builder, v, null);
    }
}
