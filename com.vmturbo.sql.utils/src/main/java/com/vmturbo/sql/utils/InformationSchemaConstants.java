package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Class containing constants representing tables, fields, etc. in the information_schema.
 */
public class InformationSchemaConstants {
    private InformationSchemaConstants() {}

    /** Overall schema. */
    public static final Schema INFORMATION_SCHEMA = DSL.schema(DSL.name("information_schema"));

    /** TABLES table. */
    public static final Table<?> TABLES = tableNamed("tables");
    /** TABLE_NAME column. */
    public static final Field<String> TABLE_NAME = stringFieldNamed("table_name");
    /** TABLE_SCHEMA column. */
    public static final Field<String> TABLE_SCHEMA = stringFieldNamed("table_schema");
    /** TABLE_TYPE column. */
    public static final Field<String> TABLE_TYPE = stringFieldNamed("table_type");

    /** TABLE_TYPE for a regular table (not a view, etc.). */
    public static final String BASE_TABLE_TYPE = "BASE TABLE";

    @Nonnull
    private static Table<Record> tableNamed(String tableName) {
        return DSL.table(DSL.name(INFORMATION_SCHEMA.getUnqualifiedName(), DSL.name(tableName)));
    }

    @Nonnull
    private static Field<String> stringFieldNamed(String name) {
        return DSL.field(name, String.class);
    }
}
