package com.vmturbo.cost.component.billedcosts;

import java.time.Clock;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertValuesStepN;
import org.jooq.Key;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.Keys;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * Inserts records to a table in batches.
 */
public class BatchInserter implements AutoCloseable {

    // Postgres does not handle tables well with multiple constraints.
    // In order to resolve this issue for 'billed_cost_daily' table,
    // we specify the unique constraint to use for conflicts.
    private static final Map<String, UniqueKey<?>> preferredUniqueKeys = Collections.singletonMap(
            BilledCostDaily.BILLED_COST_DAILY.getName(),
            Keys.KEY_BILLED_COST_DAILY_UNIQUE_CONSTRAINT_BILLING_ITEM);

    private final int batchSize;
    private final ExecutorService executorService;
    private final RollupTimesStore rollupTimesStore;

    /**
     * Creates an instance of Batch inserter.
     *
     * @param batchSize size of records to insert in a single batch.
     * @param parallelBatchInserts number of batches to be inserted in parallel.
     * @param rollupTimesStore Billed costs rollup times store.
     */
    public BatchInserter(final int batchSize,
                         final int parallelBatchInserts,
                         @Nonnull final RollupTimesStore rollupTimesStore) {
        this.batchSize = batchSize;
        this.executorService = Executors.newFixedThreadPool(parallelBatchInserts);
        this.rollupTimesStore = Objects.requireNonNull(rollupTimesStore);
    }

    /**
     * Inserts or updated records to table using the context instance based on the value of updateOnDuplicate.
     *
     * @param records to be inserted into the table.
     * @param table to which the records are to be inserted.
     * @param context to use for executing the operations.
     * @param updateOnDuplicate if true, updates already existing records with the same primary key.
     * @throws DbException on encountering DataAccessException during query execution.
     */
    public void insert(final List<? extends Record> records, final Table<?> table, final DSLContext context,
                       final boolean updateOnDuplicate) throws DbException {
        for (List<? extends Record> recordList : Lists.partition(records, batchSize)) {
            insertBatch(recordList, table, context, updateOnDuplicate, null);
        }
    }

    /**
     * Inserts records into the table via an ExecutorService. Returns the list of futures back to the caller.
     *
     * @param records to be inserted.
     * @param table to which records are to be inserted.
     * @param context instance to execute query.
     * @param updateOnDuplicate true if update on duplicate.
     * @param minSampleTime Minimal sample time that is inserted.
     * @return list of futures.
     */
    public List<Future<Integer>> insertAsync(final List<? extends Record> records,
                                             final Table<?> table,
                                             final DSLContext context,
                                             final boolean updateOnDuplicate,
                                             final long minSampleTime) {
        return Lists.partition(records, batchSize).stream()
                .map(recordsList -> executorService.submit(() ->
                    insertBatch(recordsList, table, context, updateOnDuplicate, minSampleTime)))
                .collect(Collectors.toList());
    }

    private int insertBatch(final List<? extends Record> records,
                            final Table<?> table,
                            final DSLContext context,
                            final boolean updateOnDuplicate,
                            @Nullable final Long minSampleTime) throws DbException {
        try {
            context.transaction(config -> {
                final InsertValuesStepN<?> insertValuesStepN;
                if (updateOnDuplicate) {
                    insertValuesStepN = createUpsertStatement(records, table, context);
                } else {
                    insertValuesStepN = createInsertStatement(records, table, context);
                }
                DSL.using(config).execute(insertValuesStepN);
                if (minSampleTime != null) {
                    // Update last rollup time to the previous month to trigger rollup recalculation
                    final LocalDate lastTimeByDay = YearMonth
                            .from(TimeUtil.milliToLocalDateUTC(minSampleTime))
                            .minusMonths(1)
                            .atEndOfMonth();
                    final LastRollupTimes lastRollupTimes = rollupTimesStore.getLastRollupTimes();
                    final long newLastTimeByDay = TimeUtil.localDateToMilli(lastTimeByDay,
                            Clock.systemUTC());
                    if (lastRollupTimes.hasLastTimeByDay()
                            && lastRollupTimes.getLastTimeByDay() > newLastTimeByDay) {
                        lastRollupTimes.setLastTimeByDay(newLastTimeByDay);
                        if (lastRollupTimes.getLastTimeUpdated() == 0) {
                            lastRollupTimes.setLastTimeUpdated(System.currentTimeMillis());
                        }
                        rollupTimesStore.setLastRollupTimes(lastRollupTimes);
                    }
                }
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
        Map<Field<?>, Field<?>> updates = Stream.of(table.fields())
            .filter(f -> !allKeys.contains(f))
            .collect(Collectors.toMap(Functions.identity(),
                f -> DSL.field(String.format(JooqUtil.createStringFormatForUpserts(context), f.getName()), f.getType())));
        ((InsertSetMoreStep<?>)insertValuesStepN).onConflict(
                preferredUniqueKeys.getOrDefault(table.getName(), table.getPrimaryKey())
                        .getFields()).doUpdate().set(updates);
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