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

    private static final String INFORMATION_SCHEMA_SCMNAME = "information_schema";
    private static final String TABLES_TABLE_TBLNAME = "tables";
    private static final String TABLE_NAME_COLNAME = "table_name";
    private static final String TABLE_SCHEMA_COLNAME = "table_schema";
    private static final String TABLE_TYPE_COLNAME = "table_type";
    private static final String COLUMNS_TBLNAME = "columns";
    private static final String COLUMN_NAME_COLNAME = "column_name";
    private static final String DATA_TYPE_COLNAME = "data_type";
    private static final String COLLATION_NAME_COLNAME = "collation_name";
    private static final String PARTITIONS_TBLNAME = "partitions";
    private static final String PARTITION_NAME_COLNAME = "partition_name";
    private static final String PARTITION_ORDINAL_POSITION_COLNAME = "partition_ordinal_position";
    private static final String PARTITION_DESCRIPTION_COLNAME = "partition_description";

    /** Overall schema. */
    public static final Schema INFORMATION_SCHEMA = DSL.schema(
            DSL.name(INFORMATION_SCHEMA_SCMNAME));

    /** TABLES table. */
    public static final Table<?> TABLES = tableNamed(TABLES_TABLE_TBLNAME);
    /** TABLE_NAME column. */
    public static final Field<String> TABLE_NAME = stringFieldNamed(TABLE_NAME_COLNAME);
    /** TABLE_SCHEMA column. */
    public static final Field<String> TABLE_SCHEMA = stringFieldNamed(TABLE_SCHEMA_COLNAME);
    /** TABLE_TYPE column. */
    public static final Field<String> TABLE_TYPE = stringFieldNamed(TABLE_TYPE_COLNAME);

    /** TABLE_TYPE for a regular table (not a view, etc.). */
    public static final String BASE_TABLE_TYPE = "BASE TABLE";

    /** columns table. */
    public static final Table<?> COLUMNS = tableNamed(COLUMNS_TBLNAME);
    /** column_name column. */
    public static final Field<String> COLUMN_NAME = stringFieldNamed(COLUMN_NAME_COLNAME);
    /** data_type column. */
    public static final Field<String> DATA_TYPE = stringFieldNamed(DATA_TYPE_COLNAME);
    /** collation_name column. */
    public static final Field<String> COLLATION_NAME = stringFieldNamed(COLLATION_NAME_COLNAME);

    /** partitions table (mariadb/mysql only). */
    public static final Table<?> PARTITIONS = tableNamed(PARTITIONS_TBLNAME);
    /** partition_name column. */
    public static final Field<String> PARTITION_NAME = stringFieldNamed(PARTITION_NAME_COLNAME);
    /** partition_ordinal_position column. */
    public static final Field<Integer> PARTITION_ORDINAL_POSITION =
            intFieldNamed(PARTITION_ORDINAL_POSITION_COLNAME);
    /** partition_description column. */
    public static final Field<String> PARTITION_DESCRIPTION =
            stringFieldNamed(PARTITION_DESCRIPTION_COLNAME);

    @Nonnull
    private static Table<Record> tableNamed(String tableName) {
        return DSL.table(DSL.name(INFORMATION_SCHEMA.getUnqualifiedName(), DSL.name(tableName)));
    }

    @Nonnull
    private static Field<String> stringFieldNamed(String name) {
        return DSL.field(name, String.class);
    }

    @Nonnull
    private static Field<Integer> intFieldNamed(String name) {
        return DSL.field(name, Integer.class);
    }
}
