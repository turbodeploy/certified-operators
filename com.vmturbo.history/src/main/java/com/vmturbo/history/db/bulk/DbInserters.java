package com.vmturbo.history.db.bulk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Functions;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Insert;
import org.jooq.InsertQuery;
import org.jooq.InsertValuesStepN;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.UpdatableRecord;
import org.jooq.VisitListener;
import org.jooq.impl.DSL;
import org.springframework.dao.DataAccessException;

/**
 * This class defines {@link DbInserter} implementations currently in use in history component.
 */
public class DbInserters {

    private DbInserters() {
    }

    /**
     * Interface for lambdas that perform database operations on lists of records.
     *
     * @param <R> underlying record type
     */
    interface DbInserter<R extends Record> {
        void insert(Table<R> table, List<R> records, DSLContext dsl) throws DataAccessException;
    }

    /**
     * An DbInserter that inserts records via an INSERT statement with multiple inline VALUES
     * clauses.
     *
     * @param <R> record type
     * @return an inserter configured to insert a batch fo records
     */
    public static <R extends Record> DbInserter<R> valuesInserter() {

        return (table, records, dsl) -> {
            final List<Field<?>> fieldList = getFieldList(records);
            final InsertValuesStepN<R> insert = dsl.insertInto(getInsertionTable(table))
                    .columns(fieldList);
            records.forEach(r -> insert.values(getOrderedValuesArray(r, fieldList)));
            insert.execute();
        };
    }

    /**
     * This inserter performs a batch insert using a prepared statement with record field values
     * applied as bindings to the statment.
     *
     * <p>This is not currently used, since testing showed it to be less performant than the
     * an insert statement with multiple inline VALUES clauses. That could change with a different
     * choice of databae, or with soAftware upgrades, so the code will be retained.</p>
     *
     * @param <R>      record type
     * @return an inserter that will insert a batch of records
     */
    public static <R extends Record> DbInserter<R> batchInserter() {
        return (table, records, dsl) -> {
            List<Field<?>> fields = getFieldList(records);
            final InsertValuesStepN<?> insert = dsl.insertInto(getInsertionTable(table))
                    .columns(fields)
                    .values(new Object[fields.size()]); // null values serve as placeholders
            final BatchBindStep batch = dsl.batch(insert);
            for (R record : records) {
                batch.bind(getOrderedValuesArray(record, fields));
            }
            batch.execute();
        };
    }

    /**
     * Obtain a list of all the fields of the underlying inTable type, for which at least one
     * record in the batch has a non-null value. Other fields are not included in the bindings,
     * so that default values will be applied, and NOT NULL constraints will not be violated.
     *
     * @param records records to examine
     * @param <R>     record type
     * @return fields with at least one non-null value
     */
    private static <R extends Record> List<Field<?>> getFieldList(List<R> records) {
        Set<Field<?>> fields = new LinkedHashSet<>();
        for (R record : records) {
            Stream.of(record.fields())
                    .filter(field -> record.getValue(field) != null)
                    .forEach(fields::add);
        }
        return new ArrayList<>(fields);
    }

    /**
     * This inserter performs an "upsert" operation - i.e. an INSERT... ON DUPLICATE KEY UPDATE -
     * with the supplied record.
     *
     * <p>This will either insert or update each record, depending on whether it already exists in
     * the table, based on any of the table's unique keys.</p>
     *
     * <p>The "simple" part of this implementation mostly stems from two approaches taken:</p>
     * <ul>
     *     <li>When determining which fields to set in the update part of the statement, all
     *     fields that are not primary key fields are included. This may not be correct in all
     *     cases where an upsert can be used.</li>
     *
     * </ul>
     * @param <R>      table record type
     * @return inserter function to apply to record batches
     */
    public static <R extends Record> DbInserter<R> simpleUpserter() {
        return (table, records, dsl) -> {
            final Insert<R> upsert = getUpsertStatement(table, records, dsl,
                    Collections.emptySet());
            dsl.execute(upsert);
        };
    }

    /**
     * Inserter that that performs an "upsert" operation. In addition, it skips updating fields
     * provided in the fieldsToExclude argument.
     *
     * @param fieldsToExclude the fields that need to be excluded from updates
     * @param <R> table record type
     * @return inserter function to apply to record batches
     */
    public static <R extends Record> DbInserter<R> excludeFieldsUpserter(
            Set<Field<?>> fieldsToExclude) {
        return (table, records, dsl) -> {
            final Insert<R> upsert = getUpsertStatement(table, records, dsl, fieldsToExclude);
            dsl.execute(upsert);
        };
    }

    private static <R extends Record> Insert<R> getUpsertStatement(Table<R> table,
            Collection<R> records,
            DSLContext dsl,
            Set<Field<?>> fieldsToExclude) {

        final Set<Field<?>> primaryKey = new HashSet<>(table.getPrimaryKey().getFields());
        InsertQuery<R> insert = dsl.insertQuery(getInsertionTable(table));
        records.forEach(r -> {
            // jOOQ's record state management really doesn't make sense for upserts. So to avoid
            // problems we replace each record with an exact copy that will always appear to jOOQ
            // as brand new record.
            R copy = table.newRecord();
            copy.from(r);
            insert.addRecord(copy);
        });
        insert.onDuplicateKeyUpdate(true);
        Map<Field<?>, ? extends Field<?>> updateFields = Stream.of(table.fields())
                .filter(f -> !primaryKey.contains(f))
                .filter(f -> !fieldsToExclude.contains(f))
                .collect(Collectors.toMap(Functions.identity(),
                        f -> DSL.field(String.format("VALUES(%s)", f.getName()), f.getType())));
        insert.addValuesForUpdate(updateFields);
        return insert;
    }

    /**
     * An inserter that utilizes the JooQ batchStore method to perform a mixture of inserts and
     * updates based on the state of each record.
     *
     * <p>This inserter can only be used with record types that implement {@link UpdatableRecord}
     * interface.</p>
     *
     * @param <R> record type
     * @return an inserter that will store a batch of records
     */
    public static <R extends Record> DbInserter<R> batchStoreInserter() {
        return (table, records, dsl) -> {
            @SuppressWarnings("checked") final List<UpdatableRecord<?>> castRecords =
                    (List<UpdatableRecord<?>>)records;
            dsl.batchStore(castRecords).execute();
        };
    }

    /**
     * Return values extracted from the given record, one for each of the given fields, in the
     * same order as the fields appear. These are the values bound to batch statements.
     *
     * @param record record from which to extract values
     * @param fields fields for which values are needed
     * @return extracted fields, in same order as fields list
     */
    private static Object[] getOrderedValuesArray(Record record, List<Field<?>> fields) {
        Object[] values = new Object[fields.size()];
        int i = 0;
        for (Field<?> field : fields) {
            values[i++] = record.get(field.getName());
        }
        return values;
    }

    /**
     * Return a table whose primary name that of the table to be inserted into.
     *
     * <p>This is important for transient table, where the table object is an instance of the
     * table it's patterned after, with an alias set to the transient name. That table, used in
     * a jOOQ insert statement builder, would result in SQL like <code>INSERT INTO table AS transient...</code>
     * whereas we need <code>INSERT INTO transient...</code>.</p>
     *
     * <p>When this is table instance does, in fact have the default name for its table type, then
     * the table instance itself is returned. This is important so that the {@link VisitListener}
     * used in DbCleanupRule during tests correctly records that inserts have been posted
     * to this table and must be removed.</p>
     *
     * @param table the transient table object
     * @param <R> the type of the underlying records
     * @return a correctly-named table object
     */
    private static <R extends Record> Table<R> getInsertionTable(Table<R> table) {
        try {
            String nonTransientName = table.getClass().newInstance().getName();
            if (nonTransientName.equals(table.getName())) {
                return table;
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new org.jooq.exception.DataAccessException(
                    "Failed to identify table for insertion", e);
        }
        Table<R> t = (Table<R>)DSL.table(table.getName());
        t.fields(table.fields());
        return t;
    }
}
