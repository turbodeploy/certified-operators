package com.vmturbo.sql.utils.jooq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.InsertOnDuplicateStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.util.CollectionUtils;

/**
 * Class to construct upsert statements, including provisions useful for rollup operations.
 */
public class UpsertBuilder {

    private Table<?> source;
    private Table<?> target;
    private Field<?>[] insertFields;
    private List<Field<?>> sourceGroupByFields = new ArrayList<>();
    private final List<Condition> conditions = new ArrayList<>();
    private final Map<Field<?>, Field<?>> insertValues = new HashMap<>();
    private final List<UpdateBinding<?>> updates = new ArrayList<>();
    private final List<Field<?>> conflictColumns = new ArrayList<>();

    /**
     * Specify the table that is the target of the upsert operation, i.e. the table that will
     * receive new records and/or whose existing records will be updated.
     *
     * @param target target table
     * @return this builder
     */
    public UpsertBuilder withTargetTable(Table<?> target) {
        this.target = target;
        return this;
    }

    /**
     * Specify a table from records will be retrieved to be upserted into the destination table.
     *
     * @param source table supplying records for the operation
     * @return this builder
     */
    public UpsertBuilder withSourceTable(Table<?> source) {
        this.source = source;
        return this;
    }

    /**
     * Specify conditions to apply when selecting source records for the operation.
     *
     * @param conditions one or more conditions to be applied to the source table
     * @return this builder
     */
    public UpsertBuilder withSourceCondition(Condition... conditions) {
        this.conditions.addAll(Arrays.asList(conditions));
        return this;
    }

    /**
     * Specify group by fields.
     *
     * @param sourceGroupByFields source group by fields
     * @return one or more group by fields to be applied to the source table
     */
    public UpsertBuilder withSourceGroupBy(Field<?>... sourceGroupByFields) {
        this.sourceGroupByFields.addAll(Arrays.asList(sourceGroupByFields));
        return this;
    }

    /**
     * Specify fields (columns) to be inserted in the target table. The same-named fields in the
     * source table will be used to supply values for these columns, except where an override is
     * supplied via {@link #withInsertValue(Field, Field)}.
     *
     * @param insertFields fields to appear in the INSERT column list
     * @return this builder
     */
    public UpsertBuilder withInsertFields(Field<?>... insertFields) {
        this.insertFields = insertFields;
        return this;
    }

    /**
     * Supply a value to be used for one of the insert fields, as an override to the default of a
     * same-named source-table field. E.g. to specify a constant value 1, use `DSL.inline(1)`.
     *
     * @param field field whose value is being specified
     * @param value value to be applied, in the form of a jOOQ {@link Field}
     * @param <T>   field/value type
     * @return this builder
     */
    public <T> UpsertBuilder withInsertValue(Field<T> field, Field<T> value) {
        insertValues.put(field, value);
        return this;
    }

    /**
     * Supply a value to be used for one of the insert fields, as an override to the default of a
     * same-named source-table field. E.g. to specify a constant value 1, use `1`. The supplied
     * value will be wrapped by {@link DSL#inline(Object)}.
     *
     * @param field field whose value is being specified
     * @param value value to be applied, as the value itself, not a jOOQ {@link Field}
     * @param <T>   field/value type
     * @return this builder
     */
    public <T> UpsertBuilder withInsertValue(Field<T> field, T value) {
        insertValues.put(field, DSL.inline(value));
        return this;
    }

    /**
     * Supply a default value to be used for one of the insert fields, which will be used in the
     * case that the source table does not have a matching field.
     *
     * <p>In rollups, this is convenient for columns typically named `samples` that are used as
     * weights for averages. The table that receives non-rolled-up values does not typically have
     * such a column since each record counts as a single observation, so `samples` value is
     * implicitly one. With this method one can specify `withInsertValueDefault(f,
     * DSL.inline(1))`.</p>
     *
     * @param field field whose default value is being specified
     * @param value default value to be applied, in the form of a jOOQ {@link Field}
     * @param <T>   field/value type
     * @return this builder
     */
    public <T> UpsertBuilder withInsertValueDefault(Field<T> field, Field<T> value) {
        Field<?> sourceField = source.field(field.getName(), field.getDataType());
        insertValues.put(field, sourceField != null ? sourceField : value);
        return this;
    }

    /**
     * Supply a default value to be used for one of the insert fields, which will be used in the
     * case that the source table does not have a matching field.
     *
     * <p>This acts just like {@link #withInsertValueDefault(Field, Field)} but allows the value to
     * be specified as an instance of the field type, not a jOOQ field. If used, the value is
     * wrapped by {@link DSL#inline(Object)}.</p>
     *
     * @param field field whose default value is being specified
     * @param value default value to be applied, as the value itself, not a jOOQ {@link Field}
     * @param <T>   field/value type
     * @return this builder
     */
    public <T> UpsertBuilder withInsertValueDefault(Field<T> field, T value) {
        Field<?> sourceField = source.field(field.getName(), field.getDataType());
        insertValues.put(field, sourceField != null ? sourceField : DSL.inline(value));
        return this;
    }

    /**
     * Specify a field and a value for the UPDATE part of the upsert, for any record that results in
     * a collision. THe update value is in the form of an instance of the {@link UpsertValue}
     * functional interface, and can depend on the SQLDialect.
     *
     * <p>The dialect is particularly when one wishes to use the value that <i>would have been</i>
     * inserted into the target-table field if the record had not caused a collision. Some databases
     * provide a special syntax for such values, but the syntax is not standardized. The {@link
     * JooqUtil#upsertValue(Field, SQLDialect)} method will produce the correct syntax for the given
     * dialect.</p>
     *
     * @param field field to be updated
     * @param value value to replace the existing value, in the form of a jOOQ {@link Field}
     * @param <T>   field type
     * @return this builder
     */
    public <T> UpsertBuilder withUpdateValue(Field<T> field, UpsertValue<T> value) {
        updates.add(new UpdateBinding<>(field, value));
        return this;
    }

    /**
     * Specify which columns should be considered when detecting a conflict.
     *
     * <p>N.B. This is meaningless for MariaDB's upsert statement, but in Postgres, the specified
     * columns will appear in an `ON CONFLICT` clause in the generated `INSERT` statement.
     * Developers using this method should understand the implications for the dialects we support.
     * </p>
     *
     * @param fields fields to add to conflicting columns list
     * @return this builder
     */
    public UpsertBuilder withConflictColumns(Field<?>... fields) {
        conflictColumns.addAll(Arrays.asList(fields));
        return this;
    }

    /**
     * Retrieve a jOOQ {@link Query} object encpasulating the constructed upsert operation.
     *
     * @param dsl {@link DSLContext} to use when creating the query
     * @return upsert query object, ready to be executed
     */
    public Query getUpsert(DSLContext dsl) {
        Field<?>[] selectList = new Field<?>[insertFields.length];
        for (int i = 0; i < insertFields.length; i++) {
            Field<?> field = insertFields[i];
            if (insertValues.containsKey(field)) {
                selectList[i] = insertValues.get(field);
            } else {
                selectList[i] = getSourceField(field);
            }
        }
        final Select select;
        if (!CollectionUtils.isEmpty(sourceGroupByFields)) {
            select = dsl.select(selectList).from(source).where(conditions).groupBy(
                    sourceGroupByFields);
        } else {
            select = dsl.select(selectList).from(source).where(conditions);
        }
        InsertOnDuplicateStep<?> insert = dsl.insertInto(target)
                .columns(insertFields)
                .select(select);
        Map<Object, Object> updateMap = new LinkedHashMap<>();
        for (UpdateBinding<?> update : updates) {
            update.addToMap(updateMap, dsl.dialect());
        }
        InsertOnDuplicateSetStep<? extends Record> upsert
                = conflictColumns.isEmpty()
                ? insert.onDuplicateKeyUpdate()
                : insert.onConflict(conflictColumns).doUpdate();
        upsert.set(updateMap);
        return (Query)upsert;
    }

    /**
     * Obtain a source-table field with the same name as the provided destination-table field.
     *
     * @param field destination-table field
     * @param <T>   field type
     * @return corresdponging source-table field
     */
    public <T> Field<T> getSourceField(Field<T> field) {
        return getSameNamedField(field, source);
    }

    /**
     * Given a field, and a table (where the field object is not necessarily attached to that
     * table), find a field of the same name and datatype from the table, if there is one.
     *
     * @param field field with desired name
     * @param table table in which to find field
     * @param <T>   field type
     * @return field with same name and type appearing in table, or null if there is none
     */
    public static <T> Field<T> getSameNamedField(Field<T> field, Table<?> table) {
        return table.field(field.getName(), field.getDataType());
    }

    /**
     * Interface with a method to supply an update value for a field under a given dialect.
     *
     * @param <T> field type
     */
    @FunctionalInterface
    public interface UpsertValue<T> {
        /**
         * Compute a field that will produce a value for a SET clause in the update portion of an
         * upsert statement.
         *
         * @param field   field being updated
         * @param dialect dialect in which upsert will execute
         * @return Field that will yield the computed value
         */
        Field<T> resolve(Field<T> field, SQLDialect dialect);
    }

    /**
     * {@link UpsertValue} implementation that computes the max of a field and its proposed insert
     * value.
     *
     * @param field   field being updated
     * @param dialect dialect for which upsert is being built
     * @param <T>     field type
     * @return field that will compute the max value
     */
    public static <T> Field<T> max(Field<T> field, SQLDialect dialect) {
        return DSL.case_()
                .when(field.gt(JooqUtil.upsertValue(field, dialect)), field)
                .else_(JooqUtil.upsertValue(field, dialect));
    }

    /**
     * {@link UpsertValue} implementation that computes the min of a field and its proposed insert
     * value.
     *
     * @param field   field being updated
     * @param dialect dialect for which upsert is being built
     * @param <T>     field type
     * @return field that will compute the min value
     */
    public static <T> Field<T> min(Field<T> field, SQLDialect dialect) {
        return DSL.case_()
                .when(field.lt(JooqUtil.upsertValue(field, dialect)), field)
                .else_(JooqUtil.upsertValue(field, dialect));
    }

    /**
     * {@link UpsertValue} implementation that will compute the sum of a field and its proposed
     * insert value.
     *
     * @param field   field being updated
     * @param dialect dialect for which upsert is being built
     * @param <T>     field type
     * @return field that will compute the sum
     */
    public static <T> Field<T> sum(Field<T> field, SQLDialect dialect) {
        return field.plus(JooqUtil.upsertValue(field, dialect));
    }

    /**
     * {@link UpsertValue} implementation that will compute the average of a field value and its
     * proposed insert value, both weighted by a second (numeric) field and that field's proposed
     * insert value, respectively.
     *
     * @param weight field providing weights for the average
     * @param <T>    field type
     * @return field that will compute the weighted average
     */
    public static <T> UpsertValue avg(Field<? extends Number> weight) {
        return new UpsertValue<T>() {
            @Override
            public Field<T> resolve(Field<T> field, SQLDialect dialect) {
                Field<T> sumOfProducts =
                        field.times(weight).plus(
                                JooqUtil.upsertValue(field, dialect)
                                        .times(JooqUtil.upsertValue(weight, dialect)));
                Field<? extends Number> sumOfWeights
                        = weight.plus(JooqUtil.upsertValue(weight, dialect));
                return sumOfProducts.divide(sumOfWeights);
            }
        };
    }

    /**
     * {@link UpsertValue} implementation that produces the proposed insertion value for the given
     * field (replacing the existing value).
     *
     * @param field   field being updated
     * @param dialect dialect under which upsert will be executed
     * @param <T>     field type
     * @return field that will provide the proposed insertion value
     */
    public static <T> Field<T> inserted(Field<T> field, SQLDialect dialect) {
        return JooqUtil.upsertValue(field, dialect);
    }

    /**
     * {@link UpsertValue} implementation that produces a fixed update value.
     *
     * @param value update value
     * @param <T>   field type
     * @return UpsertValue instance
     */
    public static <T> UpsertValue<T> inline(T value) {
        return (field, dialect) -> DSL.inline(value);
    }

    /**
     * Class to keep track of fields and their declared {@link UpsertValue}s.
     *
     * <p>This class is needed in order to perform the {@link UpsertValue#resolve(Field,
     * SQLDialect)} operation in a context where the field and the upsert value are known by the
     * compiler to have matching type parameters.</p>
     *
     * @param <T> field type
     */
    private class UpdateBinding<T> {

        private final Field<T> field;
        private final UpsertValue<T> value;

        /**
         * Create a new binding.
         *
         * @param field field whose update value is being bound
         * @param value update value
         */
        UpdateBinding(Field<T> field, UpsertValue<T> value) {
            this.field = field;
            this.value = value;
        }

        public void addToMap(Map<Object, Object> map, SQLDialect dialect) {
            map.put(field, value.resolve(field, dialect));
        }
    }
}
