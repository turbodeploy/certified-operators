/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.tuple.MutablePair;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;

/**
 * {@link AbstractVisitor} provides common functionality to keep aggregated state into {@link
 * ThreadLocal} wrapper.
 *
 * @param <R> type of the DB record that visitor can process.
 * @param <S> type of the state that we are going to store for each thread
 *                 independently.
 */
@ThreadSafe
public abstract class AbstractVisitor<R extends Record, S> implements RecordVisitor<R> {
    private final MutablePair<R, S> recordToState = new MutablePair<>();
    private final Consumer<S> cleaner;

    /**
     * Creates {@link AbstractVisitor} instance.
     *
     * @param cleaner function that declares the way how to clear the state in case
     *                 it is present.
     */
    protected AbstractVisitor(@Nullable Consumer<S> cleaner) {
        this.cleaner = cleaner;
    }

    @Override
    public void build(@Nonnull StatRecord.Builder builder) {
        final S state = getState();
        if (state != null) {
            buildInternally(builder, recordToState.getLeft(), recordToState.getRight());
            clear();
        }
    }

    /**
     * Populates {@link StatRecord.Builder} instance with aggregated state.
     *
     * @param builder builder instance which would be populated
     * @param state accumulated state
     * @param record first record from which state have been created.
     */
    protected abstract void buildInternally(@Nonnull StatRecord.Builder builder,
                    @Nonnull Record record, @Nonnull S state);

    /**
     * Returns current state.
     *
     * @return current state for the visitor, returns {@code null} in case state has not
     *                 been aggregated.
     */
    @Nullable
    protected S getState() {
        return recordToState.getRight();
    }

    /**
     * Ensures that current state has been initialized before or will be initialized using specified
     * value supplier.
     *
     * @param valueSupplier provides value which should be used for state
     *                 initialization
     * @param record which initialized visitor state.
     * @return existing state value in case it was initialized before or value provided by
     *                 value supplier.
     */
    @Nonnull
    protected S ensureState(@Nonnull Supplier<S> valueSupplier, @Nonnull R record) {
        if (getState() == null) {
            recordToState.setLeft(record);
            recordToState.setRight(valueSupplier.get());
        }
        return getState();
    }

    private void clear() {
        if (cleaner != null) {
            final S state = getState();
            if (state != null) {
                recordToState.setLeft(null);
                cleaner.accept(state);
            }
        }
        recordToState.setRight(null);
    }
}
