package com.vmturbo.history.db;

import java.util.Optional;

import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.history.db.bulk.BulkInserter;

/**
 * This interface is used to declare classes that can be used by a
 * {@link BulkInserter} to transform input records to records that
 * can be inserted into bulk inserter's target table.
 *
 * @param <RI> input record type
 * @param <RO> output record type
 */
@FunctionalInterface
public interface RecordTransformer<RI extends Record, RO extends Record> {

    /**
     * Singleton value of identity transformer.
     */
    RecordTransformer IDENTITY = (record, inTable, outTable) -> Optional.of(record);

    /**
     * Transform an input record to an output record.
     *
     * @param record   input record
     * @param inTable  table corresponding to input record type
     * @param outTable table corresponding to output record type
     * @return optional output record, can be omitted to indicate the input record should be ignored
     */
    Optional<RO> transform(RI record, Table<RI> inTable, Table<RO> outTable);

    /**
     * A no-op record transformer, always outputs its output record unchanged.
     *
     * @param <R> record type
     * @return identity transformer
     */
    static <R extends Record> RecordTransformer<R, R> identity() {
        return IDENTITY;
    }
}
