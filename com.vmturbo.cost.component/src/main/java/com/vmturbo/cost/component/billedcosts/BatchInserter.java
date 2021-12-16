package com.vmturbo.cost.component.billedcosts;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertValuesStepN;
import org.jooq.Key;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.DbException;

/**
 * Inserts records to a table in batches.
 */
public class BatchInserter implements AutoCloseable {

    private final int batchSize;
    private final ExecutorService executorService;

    /**
     * Creates an instance of Batch inserter.
     *
     * @param batchSize size of records to insert in a single batch.
     * @param parallelBatchInserts number of batches to be inserted in parallel.
     */
    public BatchInserter(final int batchSize, final int parallelBatchInserts) {
        this.batchSize = batchSize;
        this.executorService = Executors.newFixedThreadPool(parallelBatchInserts);
    }

    /**
     * Inserts or updated records to table using the context instance based on the value of updateOnDuplicate.
     *
     * @param records to be inserted into the table.
     * @param table to which the records are to be inserted.
     * @param context to use for executing the operations.
     * @param updateOnDuplicate if true, updates already existing records with the same primary key.
     * @throws com.vmturbo.sql.utils.DbException on encountering DataAccessException during query execution.
     */
    public void insert(final List<? extends Record> records, final Table<?> table, final DSLContext context,
                       final boolean updateOnDuplicate) throws DbException {
        for (List<? extends Record> recordList : Lists.partition(records, batchSize)) {
            insertBatch(recordList, table, context, updateOnDuplicate);
        }
    }

    /**
     * Inserts records into the table via an ExecutorService. Returns the list of futures back to the caller.
     *
     * @param records to be inserted.
     * @param table to which records are to be inserted.
     * @param context instance to execute query.
     * @param updateOnDuplicate true if update on duplicate.
     * @return list of futures.
     */
    public List<Future<Integer>> insertAsync(final List<? extends Record> records, final Table<?> table,
                                             final DSLContext context,
                                             final boolean updateOnDuplicate) {
        return Lists.partition(records, batchSize).stream().map(recordsList ->
            executorService.submit(() -> insertBatch(recordsList, table, context, updateOnDuplicate)))
            .collect(Collectors.toList());
    }

    private int insertBatch(final List<? extends Record> records, final Table<?> table, final DSLContext context,
                            final boolean updateOnDuplicate) throws DbException {
        try {
            context.transaction(config -> {
                final InsertValuesStepN<?> insertValuesStepN;
                if (updateOnDuplicate) {
                    insertValuesStepN = createUpsertStatement(records, table, context);
                } else {
                    insertValuesStepN = createInsertStatement(records, table, context);
                }
                DSL.using(config).execute(insertValuesStepN);
            });
        } catch (final DataAccessException ex) {
            throw new DbException(String.format("Insert of %s records into %s failed", records.size(), table.getName()),
                ex);
        }
        return records.size();
    }

    private InsertValuesStepN<?> createUpsertStatement(final List<? extends Record> records, final Table<?> table,
                                                       final DSLContext context) {
        final InsertValuesStepN<?> insertValuesStepN = context.insertInto(table)
            .columns(records.get(0).fields());
        final Set<TableField<?, ?>> allKeys = table.getKeys().stream()
            .map(Key::getFields)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
        records.forEach(r -> {
            Record copy = table.newRecord();
            copy.from(r);
            ((InsertSetMoreStep<?>)insertValuesStepN).set(copy).newRecord();
        });
        ((InsertSetMoreStep<?>)insertValuesStepN).onDuplicateKeyUpdate();
        Map<Field<?>, Field<?>> updates = Stream.of(table.fields())
            .filter(f -> !allKeys.contains(f))
            .collect(Collectors.toMap(Functions.identity(),
                f -> DSL.field(String.format("VALUES(%s)", f.getName()), f.getType())));
        ((InsertOnDuplicateSetStep<?>)insertValuesStepN).set(updates);
        return insertValuesStepN;
    }

    private InsertValuesStepN<?> createInsertStatement(final List<? extends Record> records, final Table<?> table,
                                                       final DSLContext context) {
        final InsertValuesStepN<?> insertValuesStepN = context.insertInto(table)
            .columns(records.get(0).fields());
        records.forEach(record -> {
            final Object[] values = new Object[record.size()];
            for (int i = 0; i < record.size(); i++) {
                values[i] = record.get(i);
            }
            insertValuesStepN.values(values);
        });
        return insertValuesStepN;
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}