/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Field;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;

/**
 * {@link RecordVisitor} visit DB record and extract values from it and populates appropriate fields
 * in {@link StatRecord.Builder} instance.
 *
 * @param <R> type of the DB record that is going to be visited.
 */
public interface RecordVisitor<R extends Record> {
    /**
     * Visits DB record to aggregate information.
     *
     * @param record which will be processed.
     */
    void visit(@Nonnull R record);

    /**
     * Populates {@link StatRecord.Builder} instance with aggregated values.
     *
     * @param builder that is going to be populated.
     */
    void build(@Nonnull StatRecord.Builder builder);

    /**
     * Extracts field value from record in case field with specified name is existing in appropriate
     * table, in case there is no column or value {@code null} will be returned.
     *
     * @param record from which we want to extract field value.
     * @param fieldName name of the field which value is going to be extracted.
     * @param fieldType type of the field that is going to be extracted.
     * @param <T> type of the field that is going to be extracted.
     * @return value of the specified field name or {@code null}.
     */
    @Nullable
    static <T> T getFieldValue(@Nonnull Record record, @Nonnull String fieldName,
                    @Nonnull Class<T> fieldType) {
        @SuppressWarnings("unchecked")
        final Field<T> field = (Field<T>)record.field(fieldName);
        return field == null ? null : record.getValue(fieldName, fieldType);
    }
}
