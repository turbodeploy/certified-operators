package com.vmturbo.extractor.models;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Objects;

import org.jooq.Field;
import org.jooq.TableField;

import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.FileType;
import com.vmturbo.extractor.schema.enums.Severity;

/**
 * Class to represent columns in database tables.
 *
 * @param <T> (Java) type of data stored in a column
 */
public class Column<T> {

    private final String name;
    private final ColType colType;

    /**
     * Constructor to create a new column.
     *
     * @param name    name of column
     * @param colType ColType value for rendering
     */
    Column(String name, ColType colType) {
        this.name = name;
        this.colType = colType;
    }

    /**
     * Constructor to create a new column.
     *
     * @param jooqField jooq field for the column
     * @param colType ColType value for rendering
     */
    Column(Field<?> jooqField, ColType colType) {
        this.name = jooqField.getName();
        this.colType = colType;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, colType);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Column) {
            Column<?> otherCol = (Column<?>)other;
            return otherCol.name.equals(name) && otherCol.colType.equals(colType);
        } else {
            return false;
        }
    }

    /**
     * Compute the bytes contributed by this column value to a hash value computed for a containing
     * row.
     *
     * @param value column value
     * @return bytes to contribute to hash
     */
    public byte[] toHashValue(Object value) {
        return colType.toBytes(value);
    }

    /**
     * Compute a CSV rendering of this column value, for use in a CSV representation of an insertion
     * row sent to a postgres COPY operation.
     *
     * @param value column value
     * @return csv representation, suitable for use in a csv row representation
     */
    public String toCsvValue(Object value) {
        return colType.toCsv(value);
    }

    /**
     * Get the postgres type name to use for this column.
     *
     * @return postgres type name
     */
    public String getDbType() {
        return colType.getPostgresType();
    }

    public ColType getColType() {
        return colType;
    }

    /**
     * Create a new int column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Integer> intColumn(final String name) {
        return new Column<>(name, ColType.INT);
    }

    /**
     * Create a new int column.
     *
     * @param field column jooq field
     * @return new column
     */
    public static Column<Integer> intColumn(final Field<Integer> field) {
        return new Column<>(field, ColType.INT);
    }

    /**
     * Create a new short column.
     *
     * @param name name of column
     * @return new column
     */
    public static Column<Short> shortColumn(final String name) {
        return new Column<>(name, ColType.SHORT);
    }

    /**
     * Create a new long column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Long> longColumn(final String name) {
        return new Column<>(name, ColType.LONG);
    }

    /**
     * Create a new long column.
     *
     * @param field The jOOQ field for the column.
     * @return new column
     */
    public static Column<Long> longColumn(final Field<Long> field) {
        return new Column<>(field, ColType.LONG);
    }

    /**
     * Create a new double column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Double> doubleColumn(final String name) {
        return new Column<>(name, ColType.DOUBLE);
    }

    /**
     * Create a new double column.
     *
     * @param field column name
     * @return new column
     */
    public static Column<Double> doubleColumn(Field<Double> field) {
        return new Column<>(field, ColType.DOUBLE);
    }

    /**
     * Create a new float column.
     *
     * @param name column name
     * @return new column
     */

    public static Column<Float> floatColumn(final String name) {
        return new Column<>(name, ColType.FLOAT);
    }

    /**
     * Create a new String column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<String> stringColumn(final String name) {
        return new Column<>(name, ColType.STRING);
    }

    /**
     * Create a new String column.
     *
     * @param field column jooq field.
     * @return new column
     */
    public static Column<String> stringColumn(final Field<String> field) {
        return new Column<>(field, ColType.STRING);
    }

    /**
     * Create a new string array column.
     * @param name column name
     * @return new column
     */
    public static Column<String[]> stringArrayColumn(final String name) {
        return new Column<>(name, ColType.STRING_ARRAY);
    }

    /**
     * Create a new string array column.
     * @param field column name
     * @return new column
     */
    public static Column<String[]> stringArrayColumn(final Field<String[]> field) {
        return new Column<>(field, ColType.STRING_ARRAY);
    }

    /**
     * Create a new boolean column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Boolean> boolColumn(final String name) {
        return new Column<>(name, ColType.BOOL);
    }

    /**
     * Create a new boolean column.
     *
     * @param field column jooq field
     * @return new column
     */
    public static Column<Boolean> boolColumn(final Field<Boolean> field) {
        return new Column<>(field, ColType.BOOL);
    }

    /**
     * Create a new Integer[] column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Integer[]> intArrayColumn(final String name) {
        return new Column<>(name, ColType.INT_ARRAY);
    }

    /**
     * Create a new Integer[] column.
     *
     * @param field column name
     * @return new column
     */
    public static Column<Integer[]> intArrayColumn(final Field<Integer[]> field) {
        return new Column<>(field, ColType.INT_ARRAY);
    }

    /**
     * Create a new Long[] column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Long[]> longArrayColumn(final String name) {
        return new Column<>(name, ColType.LONG_ARRAY);
    }

    /**
     * Create a new Long[] column.
     *
     * @param field column name
     * @return new column
     */
    public static Column<Long[]> longArrayColumn(final Field<Long[]> field) {
        return new Column<>(field, ColType.LONG_ARRAY);
    }

    /**
     * Create a new Double[] column.
     *
     * @param field field name
     * @return new column
     */
    public static Column<Double[]> doubleArrayColumn(final Field<Double[]> field) {
        return new Column<>(field, ColType.DOUBLE_ARRAY);
    }


    /**
     * Create a new Long[] column column where hash value is independent of the order of
     * the values.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Long[]> longSetColumn(final String name) {
        return new Column<>(name, ColType.LONG_SET);
    }

    /**
     * Create a new Float[] column column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Float[]> floatArrayColumn(final String name) {
        return new Column<>(name, ColType.FLOAT_ARRAY);
    }

    /**
     * Create a {@link Timestamp} column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<Timestamp> timestampColumn(final String name) {
        return new Column<>(name, ColType.TIMESTAMP);
    }

    /**
     * Create a {@link Timestamp} column.
     *
     * @param field jOOQ name
     * @return new column
     */
    public static Column<Timestamp> timestampColumn(final Field<?> field) {
        return new Column<>(field, ColType.TIMESTAMP);
    }

    /**
     * Create an {@link OffsetDateTime} column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<OffsetDateTime> offsetDateTimeColumn(final String name) {
        return new Column<>(name, ColType.OFFSET_DATE_TIME);
    }

    /**
     * Create a new JSON column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<JsonString> jsonColumn(final String name) {
        return new Column<>(name, ColType.JSON);
    }

    /**
     * Create a new JSON column.
     *
     * @param name column name
     * @return new column
     */
    public static Column<JsonString> jsonColumn(final Field<?> name) {
        return new Column<>(name, ColType.JSON);
    }


    /**
     * A wrapper for string values for columns that use the Postgres 'jsonb' column type.
     */
    public static class JsonString {

        private final String json;

        /**
         * Create a new instance.
         *
         * @param json JSON value
         */
        public JsonString(final String json) {
            this.json = json;
        }

        @Override
        public String toString() {
            return json;
        }
    }

    /**
     * Create a new entity_type column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column<EntityType> entityTypeColumn(final String name) {
        return new Column<>(name, ColType.ENTITY_TYPE);
    }

    /**
     * Create a new entity_state column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column<EntityState> entityStateColumn(final String name) {
        return new Column<>(name, ColType.ENTITY_STATE);
    }

    /**
     * Create a new attr_type column builder.
     * @param field The field
     * @return The column.
     */
    public static Column<AttrType> attrTypeColumn(TableField<?, AttrType> field) {
        return new Column<>(field, ColType.ATTR_TYPE);
    }


    /**
     * Create a new severity column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column<Severity> severityColumn(final String name) {
        return new Column<>(name, ColType.SEVERITY);
    }

    /**
     * Create a new entity_type column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column<EnvironmentType> environmentTypeColumn(final String name) {
        return new Column<>(name, ColType.ENVIRONMENT_TYPE);
    }


    /**
     * Create a new cost_category column builder with standard functions.
     *
     * @param field jOOQ table field
     * @return new column builder
     */
    public static Column<CostCategory> costCategoryColumn(TableField<?, CostCategory> field) {
        return new Column<>(field, ColType.COST_CATEGORY);
    }

    /**
     * Create a new cost_source column builder with standard functions.
     *
     * @param field jOOQ table field
     * @return new column builder
     */
    public static Column<CostSource> costSourceColumn(TableField<?, CostSource> field) {
        return new Column<>(field, ColType.COST_SOURCE);
    }

    /**
     * Create a new file_type column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column<FileType> fileTypeColumn(final String name) {
        return new Column<>(name, ColType.FILE_TYPE);
    }


}
