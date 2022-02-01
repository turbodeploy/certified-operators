package com.vmturbo.cost.component.rollup;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Utility methods used in rollup queries.
 */
public class RollupUtils {

    private RollupUtils() {
    }

    /**
     * Simple jOOQ raw-SQL-API method to make it possible to use `VALUES` function available in
     * MySQL UPSERT statements.
     *
     * <p>`SET field=VALUES(field)` in the update part of an upsert means to
     * use the value that would have been inserted into that field if a duplicate key had not
     * occurred with this record. That syntax is not available in jOOQ, but this effectively
     * adds it.</p>
     *
     * <p>See Lukas Eder's response <a href="https://stackoverflow.com/questions/39793406/jooq-mysql-multiple-row-insert-on-duplicate-key-update-using-values-funct">here</a></p>
     *
     * @param field field to be mentioned in `VALUES` expression
     * @param <T> type of field
     * @return a jOOQ {@link Field} that will provide the needed `VALUES` expression
     */
    @Nonnull
    public static <T> Field<T> values(@Nonnull final Field<T> field) {
        return DSL.field("values({0})", field.getDataType(), field);
    }

    /**
     * Simple jOOQ raw-SQL-API method to make it possible to use `VALUES` or `EXCLUDED`  function
     * available in MySQL/Postgres UPSERT statements.
     *
     * @param dslContext The {@link DSLContext} used to determine the SQL dialect and execute the
     * corresponding SQL statement.
     * @param field field to be mentioned in `VALUES` or `EXCLUDED` expression, depending
     * on the sqlDialect.
     * @param <T> type of field
     * @return a jOOQ {@link Field} that will provide the needed `VALUES` or `EXCLUDED` expression
     */
    @Nonnull
    public static <T> Field<T> values(DSLContext dslContext, @Nonnull final Field<T> field) {
        final SQLDialect dialect = dslContext.configuration().family();
        switch (dialect) {
            case POSTGRES:
                return DSL.field("excluded.{0}", field.getDataType(), field.getUnqualifiedName());
            case MARIADB:
            case MYSQL:
                return values(field);
            default:
                throw new UnsupportedOperationException(
                        String.format("Dialect '%s' is not supported", dialect));
        }
    }
}
