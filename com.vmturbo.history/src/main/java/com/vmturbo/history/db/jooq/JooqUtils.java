package com.vmturbo.history.db.jooq;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.history.schema.abstraction.Routines.startOfDay;
import static com.vmturbo.history.schema.abstraction.Routines.startOfHour;
import static com.vmturbo.history.schema.abstraction.Routines.startOfMonth;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.schema.RelationType;

public class JooqUtils {
    /*
     * Convenience methods to return table types
     */
    public static Table<?> statsTableByTimeFrame(EntityType eType, TimeFrame tFrame){
        Table<?> tbl = null;
        switch (tFrame) {
            case LATEST: tbl = eType.getLatestTable(); break;
            case HOUR:   tbl = eType.getHourTable(); break;
            case DAY:    tbl = eType.getDayTable(); break;
            case MONTH:  tbl = eType.getMonthTable(); break;
            default: break;
        }
        return tbl;
    }

    /**
     * Returns a field dynamically from the given table.
     * @throws NullPointerException if the field is not found.
     */
    public static Field<?> dField(Table<?> tbl, String fldName){
        return checkNotNull(tbl.field(fldName));
    }

    /*
     * Type-safe wrappers for casting Fields to the required generic type.
     */
    private static void checkFieldType(Class<?> given, Class<?> expected){
        checkFieldType(given, expected, false);
    }

    private static void checkFieldType(Class<?> given, Class<?> expected, boolean subClsOK){
        checkArgument(subClsOK ? expected.isAssignableFrom(given) : given==expected,
            "Incorrect field type %s (expected %s)",
            given.getName(), expected.getName());
    }

    @SuppressWarnings("unchecked")
    public static Field<RelationType> relation(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), RelationType.class);
        return (Field<RelationType>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<EnvironmentType> envType(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), EnvironmentType.class);
        return (Field<EnvironmentType>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<Integer> integer(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Integer.class);
        return (Field<Integer>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<String> str(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), String.class);
        return (Field<String>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<Timestamp> timestamp(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Timestamp.class);
        return (Field<Timestamp>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<Date> date(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Date.class);
        return (Field<Date>)field;
    }

    /**
     * java.util.Date is a superclass of: java.sql.Date and java.sql.Timestamp
     */
    @SuppressWarnings("unchecked")
    public static Field<java.util.Date> dateOrTimestamp(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), java.util.Date.class, true);
        return (Field<java.util.Date>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<Double> doubl(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Double.class);
        return (Field<Double>)field;
    }

    @SuppressWarnings("unchecked")
    public static Field<RelationType> relType(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), RelationType.class);
        return (Field<RelationType>)field;
    }

    /**
     * Get a field from a table and check that it's a BigDecimal field
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static Field<BigDecimal> getBigDecimalField(@Nonnull Table<?> table,
                                                @Nonnull String fieldName) {
        return (Field<BigDecimal>) dField(table, fieldName);
    }

    /**
     * Get a field from a table and check that it's some sort of Number field.
     */
    @SuppressWarnings("unchecked")
    public static Field<? extends Number> number(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Number.class, true);
        return (Field<? extends Number>)field;
    }


    /*
     * Convenience utilities
     */
    public static Condition and(Condition first, Condition... rest){
        Condition cond = first;
        for (Condition addl : rest) {
            cond.and(addl);
        }
        return cond;
    }

    public static <T,S> Condition andEq(Field<T> f1, T v1,
                                        Field<S> f2, S v2) {
        return f1.eq(v1).and(f2.eq(v2));
    }

    public static <T,S,X> Condition andEq(Field<T> f1, T v1,
                                          Field<S> f2, S v2, Field<X> f3, X v3) {
        return f1.eq(v1).and(f2.eq(v2)).and(f3.eq(v3));
    }

    public static <T,S,X,Y> Condition andEq(Field<T> f1, T v1,
                                            Field<S> f2, S v2, Field<X> f3, X v3,
                                            Field<Y> f4, Y v4) {
        return f1.eq(v1).and(f2.eq(v2)).and(f3.eq(v3)).and(f4.eq(v4));
    }

    public static <T,S,X,Y,Z> Condition andEq(Field<T> f1, T v1,
                                              Field<S> f2, S v2, Field<X> f3, X v3,
                                              Field<Y> f4, Y v4, Field<Z> f5, Z v5) {
        return f1.eq(v1).and(f2.eq(v2)).and(f3.eq(v3)).and(f4.eq(v4)).and(f5.eq(v5));
    }

    /*
     * Date manipulation
     */

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
     * @param dField The field to truncate
     * @param tFrame The time frame to truncate to
     * @return A field that truncates the date based on the given time-frame
     */
    public static Field<?> floorDateTime(Field<?> dField, TimeFrame tFrame) {
        switch (tFrame) {
            case LATEST:return timestamp(dField);
            case HOUR:  return startOfHour(timestamp(dField));
            case DAY:   return startOfDay(timestamp(dField));
            case MONTH: return startOfMonth(timestamp(dField));
            default:
                return null;
        }
    }

    /**
     * Wrapper around getValue from {@link Record#getValue(int)}, that defaults to 0 if the
     * returned value is null.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Number> T getValue(Record rec, String fldName, Class<T> type) {
        T val = rec.getValue(fldName, type);
        if(val != null)
            return val;
        else if(type==Double.class)
            return (T) Double.valueOf(0.0);
        else if(type==Float.class)
            return (T) Float.valueOf(0.0f);
        else if(type==Long.class)
            return (T) Long.valueOf(0L);
        else if(type==Short.class)
            return (T) Short.valueOf((short) 0);
        else if(type==Byte.class)
            return (T) Byte.valueOf((byte) 0);
        else //Integer
            return (T) Integer.valueOf(0);
    }
}
