/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Objects;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;

/**
 * {@link BasePropertyVisitor} visits different simple DB record properties and populates {@link
 * StatRecord.Builder} with value of the property.
 *
 * @param <R> type of the record from which is going to be processed.
 * @param <V> type of the value that should be placed into {@link
 *                 StatRecord.Builder}.
 * @param <D> type of the property in database record.
 */
@ThreadSafe
public class BasePropertyVisitor<R extends Record, V, D> extends AbstractVisitor<R, V> {
    private final String propertyName;
    private final BiFunction<Record, D, V> valueTransformer;
    private final SharedPropertyPopulator<V> builderPopulator;
    private final Class<D> databaseType;

    /**
     * Creates {@link BasePropertyVisitor} instance.
     *
     * @param propertyName name of the DB property that needs to be visited.
     * @param valueTransformer function that applied to DB value to get value that
     *                 needs to be placed into {@link Builder}.
     * @param builderPopulator method that should be called on {@link Builder} to
     *                 place value transformed from DB record.
     * @param databaseType type that is expected to be received from DB record for
     *                 specified parameter name.
     */
    public BasePropertyVisitor(@Nonnull String propertyName,
                    @Nonnull BiFunction<Record, D, V> valueTransformer,
                    @Nonnull SharedPropertyPopulator<V> builderPopulator,
                    @Nonnull Class<D> databaseType) {
        super(null);
        this.propertyName = Objects.requireNonNull(propertyName);
        this.valueTransformer = Objects.requireNonNull(valueTransformer);
        this.builderPopulator = Objects.requireNonNull(builderPopulator);
        this.databaseType = databaseType;
    }

    @Override
    public void visit(@Nonnull R record) {
        final D rawValue = RecordVisitor.getFieldValue(record, propertyName, databaseType);
        if (rawValue != null) {
            ensureState(() -> valueTransformer.apply(record, rawValue), record);
        }
    }

    @Override
    protected void buildInternally(@Nonnull Builder builder, @Nonnull Record record, @Nonnull V firstValue) {
        builderPopulator.accept(builder, firstValue, record);
    }

    @Override
    public String toString() {
        return String.format("%s [propertyName=%s, databaseType=%s, firstValue=%s]",
                        getClass().getSimpleName(), this.propertyName,
                        this.databaseType.getSimpleName(),
                        this.getState() == null ? null : this.getState().toString());
    }

}
