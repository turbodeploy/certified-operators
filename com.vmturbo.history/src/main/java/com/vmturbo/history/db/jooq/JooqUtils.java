package com.vmturbo.history.db.jooq;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.history.schema.abstraction.Routines.startOfDay;
import static com.vmturbo.history.schema.abstraction.Routines.startOfHour;
import static com.vmturbo.history.schema.abstraction.Routines.startOfMonth;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.types.UInteger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.schema.RelationType;

/**
 * An assortment of utility classes to assist in getUtilization of jOOQ, especially in construction
 * of queries that dynamically incorporate tables and fields.
 */
public class JooqUtils {

    /**
     * Don't permit instantiation.
     */
    private JooqUtils() {
    }

    // cache of previously retrieved fields
    // table name -> (field name -> Field instance)
    private static Map<String, Map<String, Field>> fieldLookupCache = new ConcurrentHashMap<>();

    /**
     * Returns a field dynamically from the given table.
     *
     * <p>This operation is stable, executed many times per cycle, and not as cheap as one might
     * expect (Jooq always loops through all a table's fields til it finds the one we're asking
     * for), so we memoize.</p>
     *
     * @param tbl     table whose field we're after
     * @param fldName name of the field
     * @return the {@link Field} with the given name in the given table
     * @throws NullPointerException if the field is not found.
     */
    @Nonnull
    public static Field<?> getField(@Nonnull Table<?> tbl, @Nonnull String fldName) {
        final Map<String, Field> tableMap = fieldLookupCache.computeIfAbsent(
                tbl.getName(), name -> new ConcurrentHashMap<>());
        final Field field = tableMap.computeIfAbsent(fldName, name -> tbl.field(name));
        return checkNotNull(field);
    }

    /**
     * Returns a field from the given table, and checks that the field type is as expected.
     *
     * <p>Subtypes of the expected type are OK.</p>
     * @param tbl table whose field we're after
     * @param fldName name of the field
     * @param expectedType the type we expect for the field (the java type, not the db column type)
     * @return the located field
     * @throws NullPointerException if the field is not found
     * @throws IllegalArgumentException if the field is not of the expected type
     * @param <F> field type
     */
    @Nonnull
    public static <F> Field<F> getField(@Nonnull Table<?> tbl, @Nonnull String fldName, @Nonnull Class<F> expectedType) {
        @SuppressWarnings("unchecked")
        final Field<F> field = (Field<F>)getField(tbl, fldName, expectedType, false);
        return field;
    }

    /**
     * Returns a field from the given table, and checks that the field type is as expected.
     *
     * @param tbl table whose field we're after
     * @param fldName name of the field
     * @param expectedType the type we expect for the field (the java type, not the db column type)
     * @param subClsOK true if it's OK for the field type to be a subclass of the expected type
     * @return the located field
     * @throws NullPointerException if the field is not found
     * @throws IllegalArgumentException if the field is not of the expected type
     */
    @Nonnull
    public static Field<?> getField(@Nonnull Table<?> tbl, @Nonnull String fldName, @Nonnull Class<?> expectedType,
            boolean subClsOK) {
        Field<?> fld = getField(tbl, fldName);
        checkNotNull(fld);
        checkFieldType(fld.getType(), expectedType, subClsOK);
        return fld;
    }

    /**
     * Check that the given field type is identical to the expected type.
     *
     * @param given    field type
     * @param expected expected type
     */
    public static void checkFieldType(@Nonnull Class<?> given, @Nonnull Class<?> expected) {
        checkFieldType(given, expected, false);
    }

    /**
     * Check that the given field type is identical to or assignable to the expected type.
     *
     * @param given    field type
     * @param expected expected type
     * @param subClsOK true if types need not be identical
     */
    public static void checkFieldType(@Nonnull Class<?> given, @Nonnull Class<?> expected,
            boolean subClsOK) {
        checkArgument(subClsOK ? expected.isAssignableFrom(given) : given == expected,
                "Incorrect field type %s (expected %s)",
                given.getName(), expected.getName());
    }

    /**
     * Get a field from a table and check that it's an EnvironmentType field.
     *
     * @param table     table containing field
     * @param fieldName name of field
     * @return Field instance
     */
    @Nonnull
    public static Field<EnvironmentType> getEnvField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, EnvironmentType.class);
    }

    /**
     * Get a field from a table and check that it's an Integer field.
     *
     * @param table     table containing field
     * @param fieldName name of field
     * @return Field instance
     */
    @Nonnull
    public static Field<Integer> getIntField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Integer.class);
    }

    /**
     * Get a field from a table and check that it's an unsigned Integer field.
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<UInteger> getUIntField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, UInteger.class);
    }

    /**
     * Get a field from a table and check that it's a Long field.
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<Long> getLongField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Long.class);
    }

    /**
     * Get a field from a table and check that it's a Byte field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<Byte> getByteField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Byte.class);
    }

    /**
     * Get a field from a table and check that it's a String field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<String> getStringField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, String.class);
    }

    /**
     * Get a field from a table and check that it's a RelationType field.
     *
     * @param table     that contains the field
     * @param fieldName the name of the field
     * @return {@link Field} with the field
     */
    @Nonnull
    public static Field<RelationType> getRelationTypeField(@Nonnull Table<?> table,
            @Nonnull String fieldName) {
        return getField(table, fieldName, RelationType.class);
    }

    /**
     * Get a field from a table and check that it's a Timestamp field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<Timestamp> getTimestampField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Timestamp.class);
    }

    /**
     * Get a field from a table and check that it's a Date field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<Date> getDateField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Date.class);
    }

    /**
     * Get a field from a table and check that it's a Date or Timestamp field.
     *
     * <p>Note that java.util.Date is a superclass of: java.sql.Date and java.sql.Timestamp</p>
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static Field<java.util.Date> getDateOrTimestampField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return (Field<java.util.Date>)getField(table, fieldName, java.util.Date.class, true);
    }

    /**
     * Get a field from a table and check that it's a Double field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<Double> getDoubleField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return getField(table, fieldName, Double.class);
    }

    /**
     * Get a field from a table and check that it's a BigDecimal field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @Nonnull
    public static Field<BigDecimal> getBigDecimalField(@Nonnull Table<?> table,
            @Nonnull String fieldName) {
        return getField(table, fieldName, BigDecimal.class);
    }

    /**
     * Get a field from a table and check that it's some sort of Number field.
     *
     * @param table     the table
     * @param fieldName the field name
     * @return the field
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static Field<? extends Number> getNumberField(@Nonnull Table<?> table, @Nonnull String fieldName) {
        return (Field<? extends Number>)getField(table, fieldName, Number.class, true);
    }

    /**
     * Truncates the minutes &| hours &| days of the given field, based on the
     * time frame.
     * <ul>
     *  <li>Latest returns the original time, untruncated</li>
     *  <li>Hour cuts off minutes</li>
     *  <li>Day cuts off minutes and hours</li>
     *  <li>Month cuts off minutes, hours and days</li>
     * </ul>
     *
     * @param table The table containing the field field to truncate
     * @param fieldName the name of the field to truncate
     * @param tFrame The time frame to truncate to
     * @return A field that truncates the date based on the given time-frame
     */
    @Nonnull
    public static Field<?> floorDateTime(@Nonnull Table<?> table, @Nonnull String fieldName, TimeFrame tFrame) {
        switch (tFrame) {
            case LATEST:
                return getTimestampField(table, fieldName);
            case HOUR:
                return startOfHour(getTimestampField(table, fieldName));
            case DAY:
                return startOfDay(getTimestampField(table, fieldName));
            case MONTH:
                return startOfMonth(getTimestampField(table, fieldName));
            default:
                throw new IllegalArgumentException("Unsupported TimeFrame value: " + tFrame.name());
        }
    }

    private static ImmutableMap<Class<?>, Object> zeroValues = ImmutableMap.<Class<?>, Object>builder()
            .put(Double.class, 0.0)
            .put(Float.class, 0.0f)
            .put(Long.class, 0L)
            .put(Short.class, (short)0)
            .put(Byte.class, (byte)0)
            .put(Integer.class, 0)
            .put(BigInteger.class, BigInteger.ZERO)
            .put(BigDecimal.class, BigDecimal.ZERO)
            .build();

    /**
     * Wrapper around getValue from {@link Record#getValue(int)}, that defaults to 0 if the
     * returned value is null.
     * @param rec     the record containing the value
     * @param fldName the name of the field to get
     * @param type    the type of the field
     * @return the field value, or zero if the field has no value
     * @param <T> type of the field
     */
    @SuppressWarnings("unchecked")
    public static <T extends Number> T getValue(Record rec, String fldName, Class<T> type) {
        T val = rec.getValue(fldName, type);
        if (val == null) {
            val = (T)zeroValues.get(type);
        }
        if (val != null) {
            return val;
        } else {
            throw new IllegalArgumentException("Invalid numeric type: " + type);
        }
    }
}
