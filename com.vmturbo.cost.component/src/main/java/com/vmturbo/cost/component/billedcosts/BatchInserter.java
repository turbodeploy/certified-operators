package com.vmturbo.cost.component.billedcosts;

import static org.jooq.impl.DSL.when;

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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger logger = LogManager.getLogger();
    // Postgres does not handle tables well with multiple constraints.
    // In order to resolve this issue for 'billed_cost_daily' table,
    // we specify the unique constraint to use for conflicts.
    private static final Map<String, UniqueKey<?>> preferredUniqueKeys = Collections.singletonMap(
            BilledCostDaily.BILLED_COST_DAILY.getName(),
            Keys.KEY_BILLED_COST_DAILY_UNIQUE_DAILY_BILLING_ITEM);

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

    /**
     * Inserts or updated records to table using the context instance based on the value of updateOnDuplicate.
     *
     * @param records to be inserted into the table.
     * @param table to which the records are to be inserted.
     * @param context to use for executing the operations.
     * @param updateOnDuplicate if true, updates already existing records with the same primary key.
     * @param minSampleTime Minimal sample time that is inserted.
     * @throws DbException on encountering DataAccessException during query execution.
     */
    public int insertBatch(final List<? extends Record> records,
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
                int rowsAffected = DSL.using(config).execute(insertValuesStepN);
                // Update count seems to be affected count minus the input record count.
                logger.trace("Batch insertion done, rows updated: {}, input records: {}, "
                        + "table: {}, updateOnDuplicate? {}", (rowsAffected - records.size()),
                        records.size(), table, updateOnDuplicate);
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
        final InsertSetMoreStep<?> insertSetMoreStep = (InsertSetMoreStep<?>)insertValuesStepN;
        final Set<TableField<?, ?>> allKeys = table.getKeys().stream()
                .map(Key::getFields)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        records.forEach(r -> {
            Record copy = table.newRecord();
            copy.from(r);
            insertSetMoreStep.set(copy).newRecord();
        });
        Map<Field<?>, Field<?>> updates = Stream.of(table.fields())
                .filter(f -> !allKeys.contains(f))
                .collect(Collectors.toMap(Functions.identity(),
                        f -> JooqUtil.upsertValue(f, context.dialect())));

        final ImmutablePair<Field<Long>, Field<Long>> lastUpdatedFieldValue =
                getLastUpdateFieldValue(table, updates);

        insertSetMoreStep.onConflict(
                preferredUniqueKeys.getOrDefault(table.getName(), table.getPrimaryKey())
                        .getFields())
                .doUpdate()
                .set(lastUpdatedFieldValue.getKey(), lastUpdatedFieldValue.getValue())
                .set(updates);
        return (InsertValuesStepN<?>)insertSetMoreStep;
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

    /**
     * This is called when there is a duplicate key conflict and we need to update the values.
     * The last_updated field is updated like this:
     *      if last_updated value being inserted is 0, then set new last_updated to null.
     *      if usage_amount existing value != its new value, then last_updated is the new value.
     *      if cost existing value != its new value, then last_updated is the new value.
     *      else (usage_amount & cost are same values as before), leave last_updated value as is.
     * i.e. last_updated is only set to a valid value (timestamp) if new value of either cost or
     * usage_amount is different from what is already there in DB.
     *
     * @param table DB table, billed_cost_daily.
     * @param updates Map of fields to update. The 'last_updated' field is removed from this map,
     *      as we want to do custom update for this field (done in this method).
     * @return Map of last_updated field and value to be set in the onDuplicateKeyUpdate() clause.
     */
    @SuppressWarnings("unchecked")
    private ImmutablePair<Field<Long>, Field<Long>> getLastUpdateFieldValue(final Table<?> table,
            final Map<Field<?>, Field<?>> updates) {
        final Field<Long> lastUpdatedField = (Field<Long>)table.field("last_updated");

        // Remove the value from 'updates' map, as we are explicitly updating this below.
        Field<Long> lastUpdatedValue = (Field<Long>)updates.remove(lastUpdatedField);

        Field<Double> costField = (Field<Double>)table.field("cost");
        Field<Double> costValue = (Field<Double>)updates.get(costField);

        Field<Double> usageAmountField = (Field<Double>)table.field("usage_amount");
        Field<Double> usageAmountValue = (Field<Double>)updates.get(usageAmountField);

        // The 'value' (what we got from 'updates' map) is the value that we are trying to update
        // this time. The 'field' is the value that is currently in the DB table. E.g we are
        // comparing the usageAmount 'field' (what is in DB currently) to see if it is not-equals
        // to its 'value' (what is currently being inserted), to figure out what lastUpdated should
        // be set to.
        return ImmutablePair.of(lastUpdatedField,
                when(lastUpdatedValue.eq(0L), DSL.inline((Long)null))
                .when(usageAmountField.ne(usageAmountValue), lastUpdatedValue)
                .when(costField.ne(costValue), lastUpdatedValue)
                .otherwise(lastUpdatedField));
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}