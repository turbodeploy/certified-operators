package com.vmturbo.sql.utils.jooq;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.DeleteLimitStep;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

/**
 * Various utilities to be used with jOOQ.
 */
public class JooqUtil {

    /**
     * There is no similar config variable in postgres, the query size limit is 2G, but we use 1G
     * here to be consistent with mariadb in terms of performance.
     */
    private static final int POSTGRES_QUERY_SIZE_LIMIT = 1073741824;

    private JooqUtil() {
    }


    /**
     * Create a new temporary table in the DB whose column structure is identical to table known to
     * jOOQ via codegen.
     *
     * @param conn  DB connection on which this temp table will be visible
     * @param basis known table on which the temp table will be based
     * @param name  name for the temp table
     * @param <R>   table record type
     * @return a {@link TempTable} object from which a jOOQ table and fields are easily accessible
     */
    public static <R extends Record> TempTable<R> createTemporaryTable(
            Connection conn, Table<R> basis, String name) {
        final TempTable<?> tempTable = createTemporaryTable(conn, basis, name, basis.fields());
        //noinspection unchecked
        return (TempTable<R>)tempTable;
    }

    /**
     * Create a new temporary table in the DB with a given name and list of columns.
     *
     * @param conn   DB connection on which this temp table will be visible
     * @param basis  table known to jOOQ whose qualified name prefix will be used for the new temp
     *               table
     * @param name   name for the temp table
     * @param fields jOOQ {@link Field} objects that provide column definitions for the temp table;
     *               may be empty
     * @return a {@link TempTable} object from which a jOOQ table and fields are easily accessible
     */
    public static TempTable<?> createTemporaryTable(
            Connection conn, Table<?> basis, String name, Field<?>... fields) {
        try (DSLContext dsl = DSL.using(conn)) {
            final TempTable<?> tempTable = new TempTable<>(basis, name, fields);
            dsl.createTemporaryTable(tempTable.table()).columns(fields).execute();
            return tempTable;
        }
    }

    /**
     * Delete chunks of records until there are no more records to delete.
     *
     * @param deleteLimitStep The delete step to perform.
     * @param chunkSize The chunk size.
     * @return The total number of deleted rows.
     * @throws DataAccessException If anything goes wrong.
     */
    public static int deleteInChunks(@Nonnull final DeleteLimitStep<?> deleteLimitStep,
                                     final int chunkSize) throws DataAccessException  {
        int totalDeleted = 0;
        int numDeleted;
        do {
            numDeleted = deleteLimitStep
                    .limit(chunkSize)
                    .execute();
            totalDeleted += numDeleted;
        } while (numDeleted > 0);
        return totalDeleted;
    }

    /**
     * Class to make it simpler to use jOOQ with temporary tables.
     *
     * <p>These tables are not available when jOOQ performs its codegen, so {@link Table} and
     * related classes are not defined. This class dynamically creates a {@link Table} object and
     * several {@link Field} objects, all easily available from this wrapper for use in with the
     * jOOQ query builder.</p>
     *
     * <p>This class does not actually create a temporary table.</p>
     *
     * @param <R> Underlying jOOQ record type, if known
     */
    public static class TempTable<R extends Record> {

        private final Table<R> table;
        private final Map<String, Field<?>> fields = new HashMap<>();

        /**
         * Create a new instance.
         *
         * @param basis  table known to jOOQ; if provided, it's used to improve name qualifications
         * @param name   unqualified name of temporary table
         * @param fields list of fields corresponding to temp table columns; each should have a name
         *               and a datatype at minimum; jOOQ generated field constants work great
         */
        public TempTable(Table<R> basis, String name, Field<?>... fields) {
            final Name tableName = basis != null
                    ? DSL.name(basis.getSchema().getQualifiedName(), DSL.name(name))
                    : DSL.name(name);
            final Table<Record> table = DSL.table(tableName);
            //noinspection unchecked
            this.table = (Table<R>)table;
            for (final Field<?> field : fields) {
                addField(field);
            }
        }

        /**
         * Get the jOOQ {@link Table} object naming the temp table.
         *
         * <p>The table will have a qualified name based on the temp table name and the
         * basis table, if any, provided in the constructor.</p>
         *
         * @return jOOQ table
         */
        public Table<R> table() {
            return table;
        }

        /**
         * Get a jOOQ {@link Field} by name, from among those provided in the constructor.
         *
         * <p>The field will have a qualified name that consists of the temp table's qualified
         * name followed by the provided field's unqualified name.</p>
         *
         * @param name name of field
         * @return the named field
         */
        public Field<?> field(String name) {
            return fields.get(name);
        }

        /**
         * Get a jOOQ {@link Field} from among those provided in the constructor, identified by
         * another jOOQ {@link Field}.
         *
         * <p>The provided field is used only to obtain its unqualified name. This makes it
         * convenient to use field constants defined from jOOQ codegen, for example.</p>
         *
         * @param basis field whose name will be used to find the temp table field
         * @param <T>   field type
         * @return the corresponding temp table field
         */
        public <T> Field<T> field(Field<T> basis) {
            final Field<?> field = field(basis.getUnqualifiedName().last());
            //noinspection unchecked
            return (Field<T>)field;
        }

        /**
         * Create a temp-table {@link Field} with the same unqualified name and datatype as the
         * given field.
         *
         * <p>The temp-table's qualified name is used to qualify this field name. That will be
         * a fully qualified name if a jOOQ-generated table was provided as a basis; otherwise it
         * will simply be the temp table name followed by the field name.</p>
         *
         * @param field jOOQ {@link Field} object providing name and type information
         */
        private void addField(Field<?> field) {
            String name = field.getName();
            Name qualifiedName = DSL.name(table.getQualifiedName(), DSL.name(name));
            fields.put(name, DSL.field(qualifiedName, field.getDataType()));
        }
    }


    /**
     * Disables foreign key constraints within the session associated with {@code dslContext}.
     * @param dslContext The {@link DSLContext} used to determine the SQL dialect and execute the
     *                   corresponding SQL statement.
     */
    public static void disableForeignKeyConstraints(@Nonnull DSLContext dslContext) {
        final SQLDialect dialect = dslContext.configuration().family();
        final String disableStatement;
        switch (dialect) {
            case POSTGRES:
                // TODO find a way to disable foreign key constraint for Postgres without requiring admin user.
                // Postgres foreign key constrain is enforced by “system triggers” on a table.
                // Only admin user can disable "system triggers". For now before restoring cost diags,
                // we need to execute "set session_replication_role to replica;" and then
                // "set session_replication_role to default;"; after finished.
                return;
            case MARIADB:
            case MYSQL:
                disableStatement = "SET FOREIGN_KEY_CHECKS=0;";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Dialect '%s' not supported", dialect));
        }

        dslContext.execute(disableStatement);
    }

    /**
     * Enables foreign key constraints within the session associated with {@code dslContext}.
     * @param dslContext The {@link DSLContext} used to determine the SQL dialect and execute the
     *                   corresponding SQL statement.
     */
    public static void enableForeignKeyConstraints(@Nonnull DSLContext dslContext) {
        final SQLDialect dialect = dslContext.configuration().family();
        final String enableStatement;
        switch (dialect) {
            case POSTGRES:
                return;
            case MARIADB:
            case MYSQL:
                enableStatement = "SET FOREIGN_KEY_CHECKS=1;";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Dialect '%s' not supported", dialect));
        }
        dslContext.execute(enableStatement);
    }

    /**
     * Get the max allowed packet in bytes for the database underlying the given DSLContext.
     *
     * @param dslContext {@link DSLContext}
     * @return max allowed bytes for a single SQL statement
     */
    public static int getMaxAllowedPacket(DSLContext dslContext) {
        final SQLDialect dialect = dslContext.configuration().family();
        switch (dialect) {
            case MARIADB:
            case MYSQL:
                Result<Record> maxPackageSize = dslContext.fetch(
                        "SHOW VARIABLES LIKE 'max_allowed_packet';");
                return maxPackageSize.get(0).getValue("Value", int.class);
            case POSTGRES:
                return POSTGRES_QUERY_SIZE_LIMIT;
            default:
                throw new UnsupportedOperationException(
                        String.format("Dialect '%s' is not supported", dialect));
        }
    }

    /**
     * Get a jOOQ {@link Field} object that can be used in a SET clause of an upsert statement to
     * refer to the value that would have been inserted for the given field had this record not
     * resulted in a conflict.
     *
     * @param f   field into which insert value would have been inserted
     * @param dialect SQL dialect in which upsert will be executed
     * @param <T> type of field
     * @return field that will yield the insertion value for this field, in a SET clause of the
     *         update side of an upsert
     */
    public static <T> Field<T> upsertValue(Field<T> f, SQLDialect dialect) {
        Class<T> type = f.getType();
        Name name = DSL.name(f.getName());
        switch (dialect) {
            case POSTGRES:
                return DSL.field("excluded.{0}", type, name);
            case MARIADB:
            case MYSQL:
                return DSL.field("values({0})", type, name);
            default:
                throw new UnsupportedOperationException(
                        String.format("Dialect %s is not supported", dialect));
        }
    }
}