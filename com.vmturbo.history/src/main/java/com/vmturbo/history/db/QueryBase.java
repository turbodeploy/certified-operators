package com.vmturbo.history.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSeekStepN;
import org.jooq.SelectSelectStep;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.Table;

/**
 * Base class for query builder classes.
 *
 * <p>Currently this provides methods to specify parts of the desired query and to construct the query. Eventually,
 * we intend to add features so that query classes can be used to run experiments to test and/or tune query
 * performance.</p>
 */
public abstract class QueryBase {
    private Map<String, JoinSpec> tables = new LinkedHashMap<>();
    private Map<String, Pair<String, IndexHint>> indexHints = new HashMap<>();
    private List<Field<?>> selectFields = new ArrayList<>();
    private List<Condition> conditions = new ArrayList<>();
    private List<SortField<?>> sortFields = new ArrayList<>();
    private List<Field<?>> groupByFields = new ArrayList<>();
    private int limit = 0;
    private boolean distinct = false;

    /**
     * Assemble the final query.
     *
     * @return jOOQ query object.
     */
    public ResultQuery<?> getQuery() {
        if (selectFields.isEmpty()) {
            // no select fields were specified, so add defaults
            addDefaultSelectFields();
        }
        final SelectSelectStep<Record> fieldsQuery = distinct
                ? HistorydbIO.getJooqBuilder().selectDistinct(selectFields)
                : HistorydbIO.getJooqBuilder().select(selectFields);
        final SelectJoinStep<Record> tablesQuery;
        if (!tables.isEmpty()) {
            // first table is guaranteed to not include a join specification
            fieldsQuery.from(applyHint(tables.values().iterator().next().table));
            tablesQuery = (SelectJoinStep<Record>)fieldsQuery;
            // add remaining tables with join types and conditions
            tables.values().stream()
                    .skip(1) // already handled first table
                    .forEach(j -> tablesQuery.join(applyHint(j.getTable()), j.getJoinType()).on(j.joinConditions));
        } else {
            tablesQuery = (SelectJoinStep<Record>)fieldsQuery;
        }
        final SelectConditionStep<Record> whereQuery = tablesQuery.where(conditions);
        final SelectHavingStep<Record> groupByQuery = groupByFields.isEmpty() ? whereQuery
                : whereQuery.groupBy(groupByFields);
        final SelectSeekStepN<Record> orderedQuery = sortFields.isEmpty()
                ? (SelectSeekStepN<Record>)groupByQuery
                : groupByQuery.orderBy(sortFields);
        return limit > 0 ? orderedQuery.limit(limit) : orderedQuery;
    }

    private void addDefaultSelectFields() {
        if (tables.isEmpty()) {
            // if there are no tables we require select fields
            throw new IllegalArgumentException("Cannot build query with no select fields and no queries.");
        } else {
            // else default to all the fields of the first table
            addSelectFields(tables.values().iterator().next().getTable().fields());
        }
    }

    /**
     * Specify that the query should return distinct results.
     */
    protected void setDistinct() {
        this.distinct = true;
    }

    /**
     * Add un-aliased select fields to the query.
     *
     * @param selectFields {@link Field} values to be added (can include computed fields
     */
    protected void addSelectFields(Field<?>... selectFields) {
        addSelectFields(Iterators.forArray(selectFields));
    }

    /**
     * Add un-aliased select fields to the query.
     *
     * <p>Using {@link Iterator} argument type means we can cheaply use this method for both array and collection
     * based overloads.</p>
     *
     * @param selectFields {@link Field} values to be added
     */
    private void addSelectFields(Iterator<Field<?>> selectFields) {
        selectFields.forEachRemaining(f -> addSelectField(f, null));
    }

    /**
     * Add a select field with an alias.
     *
     * @param field {@link Field} value
     * @param alias alias for field
     */
    protected void addSelectField(Field<?> field, String alias) {
        selectFields.add(alias != null ? field.as(alias) : field);
    }

    /**
     * Add a first table to the query, with no join info and no alias.
     *
     * @param table first table for query
     */
    protected void addTable(Table<?> table) {
        addTable(table, null);
    }

    /**
     * Add a first table to the query with an alias, but no join info.
     *
     * @param table first table for query
     * @param alias table alias, or null for none
     */
    protected void addTable(Table<?> table, String alias) {
        if (!tables.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Table %s without join type and conditions is not first added", table.getName()));
        }
        tables.put(alias != null ? alias : table.getName(), new JoinSpec(table, alias));
    }

    /**
     * Add a table to the query with join information to join with prior tables.
     *
     * @param table          table to add
     * @param alias          alias for the table, or null for none
     * @param joinType       join type to use for this table
     * @param joinConditions 'on' conditions for joining this table with prior tables
     */
    protected void addTable(Table<?> table, String alias, JoinType joinType, Condition... joinConditions) {
        String name = alias != null ? alias : table.getName();
        if (tables.containsKey(alias)) {
            throw new IllegalArgumentException(String.format("Table with name/alias %s already added", name));
        }
        tables.put(alias, new JoinSpec(table, alias, joinType, joinConditions));
    }

    /**
     * Add WHERE clause conditions to this query.
     *
     * @param conditions conditions to add
     */
    protected void addConditions(Condition... conditions) {
        addConditions(Iterators.forArray(conditions));
    }

    /**
     * Add WHERE clause conditions to this query.
     *
     * <p>Iterator can be cheaply used for array and collection based overloads.</p>
     *
     * @param conditions conditions to be added
     */
    protected void addConditions(Iterator<Condition> conditions) {
        conditions.forEachRemaining(c -> this.conditions.add(c));
    }

    /**
     * Add a range condition to the query based on upper and/or lower bounds for a given field.
     *
     * @param field         the field to be constrained
     * @param fromInclusive inclusive lower bound on field value, or null for none
     * @param toExclusive   exclusive upper bound on field value, or null for none
     * @param <T>           field type
     */
    protected <T> void addFieldRangeCondition(Field<T> field, T fromInclusive, T toExclusive) {
        if (fromInclusive != null && toExclusive != null) {
            conditions.add(field.between(fromInclusive, toExclusive));
        } else if (fromInclusive != null) {
            conditions.add(field.ge(fromInclusive));
        } else if (toExclusive != null) {
            conditions.add(field.lt(toExclusive));
        }
    }

    /**
     * Add ascending sort fields to the query.
     *
     * @param fields fields to use for sorting results
     */
    protected void orderBy(Field<?>... fields) {
        orderBy(Iterators.forArray(fields));
    }

    /**
     * Add ascending sort fields to the query.
     *
     * @param fields fields to use for sorting
     */
    private void orderBy(Iterator<Field<?>> fields) {
        fields.forEachRemaining(f -> orderBy(f, SortOrder.ASC));
    }

    /**
     * Add a sort field to the query.
     *
     * @param field     field to use for sorting
     * @param sortOrder sort direction (ascending/descending) for this field
     */
    protected void orderBy(Field<?> field, SortOrder sortOrder) {
        switch (sortOrder) {
            case ASC:
                sortFields.add(field.asc());
                break;
            case DESC:
                sortFields.add(field.desc());
                break;
            default:
                throw new IllegalArgumentException("Unknown SortOrder: " + sortOrder.name());
        }
    }

    protected void groupBy(Field<?>... fields) {
        groupBy(Iterators.forArray(fields));
    }

    protected void groupBy(Iterator<Field<?>> fields) {
        fields.forEachRemaining(groupByFields::add);
    }

    /**
     * Specify that a USE INDEX hint should be applied to the given table.
     *
     * @param table     table for hint
     * @param indexName name of index for hint
     */
    protected void useIndex(Table<?> table, String indexName) {
        useIndex(table.getName(), indexName);
    }

    /**
     * Specify that a USE INDEX hint should be applied to the table with the given alias.
     *
     * @param alias     table alias
     * @param indexName name of index for hint
     */
    protected void useIndex(String alias, String indexName) {
        addIndexHint(alias, indexName, IndexHint.USE);
    }

    /**
     * Speicfy that a FORCE INDEX hint should be applied to the given table.
     *
     * @param table     table for hint
     * @param indexName index name for hint
     */
    protected void forceIndex(Table<?> table, String indexName) {
        forceIndex(table.getName(), indexName);
    }

    /**
     * Specify that a FORCE INDEX hint should be applied to the table with the given alias.
     *
     * @param alias     table alias
     * @param indexName name of index for hint
     */
    protected void forceIndex(String alias, String indexName) {
        addIndexHint(alias, indexName, IndexHint.FORCE);
    }

    /**
     * Specify that an IGNORE INDEX hint should be applied to the given table.
     *
     * @param table     table for hint
     * @param indexName index name for hint
     */
    protected void ignoreIndex(Table<?> table, String indexName) {
        ignoreIndex(table.getName(), indexName);
    }

    /**
     * Specify that an IGNORE INDEX hint should be applied to the table with the given alias.
     *
     * @param alias     table alias
     * @param indexName name of index for hint
     */
    protected void ignoreIndex(String alias, String indexName) {
        addIndexHint(alias, indexName, IndexHint.IGNORE);
    }

    /**
     * Add an index hint to the given table with the given name/alias.
     *
     * @param alias     table name or alias
     * @param indexName name of index
     * @param hint      type of hint
     */
    private void addIndexHint(String alias, String indexName, IndexHint hint) {
        indexHints.put(alias, Pair.of(indexName, hint));
    }

    /**
     * Apply the registered hint, if any, to the given table.
     *
     * <p>for use when constructing the query</p>
     *
     * @param table table being used
     * @return table with hint applied, if any is registered
     */
    private Table<?> applyHint(Table<?> table) {
        Pair<String, IndexHint> hint = indexHints.get(table.getName());
        if (hint != null) {
            switch (hint.getRight()) {
                case USE:
                    return table.useIndex(hint.getLeft());
                case IGNORE:
                    return table.ignoreIndex(hint.getLeft());
                case FORCE:
                    return table.forceIndex(hint.getLeft());
                default:
                    throw new IllegalStateException("Unknown index hint type: " + hint.getRight().name());
            }
        } else {
            return table;
        }
    }

    /**
     * Specify a LIMIT value for the query.
     *
     * @param limit limit value
     */
    protected void limit(int limit) {
        this.limit = limit;
    }

    /**
     * Class to capture join information for a table participating in the query.
     */
    private static class JoinSpec {

        private final Table<?> table;
        private final JoinType joinType;
        private final Condition[] joinConditions;

        /**
         * Create a new join spec for the first table in the query, which does not have any join info.
         *
         * @param table the table
         * @param alias an alias, or null for none
         */
        JoinSpec(Table<?> table, String alias) {
            this(table, alias, null);
        }

        /**
         * Create a new join spec.
         *
         * @param table          table being joined
         * @param alias          alias for table, or null
         * @param joinType       join type for this table
         * @param joinConditions ON conditions for this join
         */
        JoinSpec(Table<?> table, String alias, JoinType joinType, Condition... joinConditions) {
            this.table = alias != null ? table.as(alias) : table;
            this.joinType = joinType;
            this.joinConditions = joinConditions;
        }

        /**
         * Get the table for this join spec.
         *
         * @return the table
         */
        public Table<?> getTable() {
            return table;
        }

        /**
         * Get the join type for this join spec.
         *
         * @return the join type
         */
        public JoinType getJoinType() {
            return joinType;
        }

        /**
         * Get the ON conditions for this join spec.
         *
         * @return the conditions
         */
        public Condition[] getJoinConditions() {
            return joinConditions;
        }
    }

    /**
     * Index hint types.
     */
    private enum IndexHint {
        /**
         * USE INDEX hint type.
         */
        USE,
        /**
         * IGNORE INDEX hint type.
         */
        IGNORE,
        /**
         * FORCE INDEX hint type.
         */
        FORCE
    }
}
