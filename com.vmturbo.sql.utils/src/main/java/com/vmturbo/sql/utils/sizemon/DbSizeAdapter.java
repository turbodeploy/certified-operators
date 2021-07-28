package com.vmturbo.sql.utils.sizemon;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Class to obtain size-related info for a database schema. Subclasses adapt to different supported
 * database dialects.
 */
public abstract class DbSizeAdapter {

    protected static final Table<Record> TABLES_TABLE = DSL.table("information_schema.tables");
    protected static final Field<String> TABLE_NAME_FIELD = DSL.field("table_name", String.class);
    protected static final Field<String> TABLE_SCHEMA_FIELD = DSL.field("table_schema", String.class);
    protected static final Field<String> TABLE_TYPE_FIELD = DSL.field("table_type", String.class);
    protected static final String BASE_TABLE_TYPE_VALUE = "BASE TABLE";
    protected static final Field<Long> DATA_LENGTH_FIELD = DSL.field("data_length", Long.class);
    protected static final Field<Long> INDEX_LENGTH_FIELD = DSL.field("index_length", Long.class);
    protected static final Field<Long> TABLE_ROWS_FIELD = DSL.field("table_rows", Long.class);

    protected final DSLContext dsl;
    protected final Schema schema;

    /**
     * Create a new instance.
     *  @param dsl         {@link DSLContext for db access}
     * @param schema      schema to interrogate
     */
    protected DbSizeAdapter(DSLContext dsl, Schema schema) {
        this.dsl = dsl;
        this.schema = schema;
    }

    /**
     * Create a new instance using a super-class determined by the dialect configured for the given
     * {@link DSLContext}.
     *
     * @param dsl         {@link DSLContext} for db access
     * @param schema      schema to interrogate
     * @return new adapter instance
     */
    public static DbSizeAdapter of(DSLContext dsl, Schema schema) {
        final SQLDialect dialect = dsl.configuration().dialect();
        switch (dialect) {
            case POSTGRES:
                return new PostgresSizeAdapter(dsl, schema);
            case MYSQL:
            case MARIADB:
                return new MariaMysqlSizeAdapter(dsl, schema);
            default:
                throw new UnsupportedOperationException(
                        String.format("DbSizeAdapter not implemented for dialect %s", dialect));
        }
    }

    /**
     * Get a list of all the tables in this schema.
     *
     * @return list of tables
     */
    public List<Table<?>> getTables() {
        return dsl.select(TABLE_NAME_FIELD)
                .from(TABLES_TABLE)
                .where(TABLE_SCHEMA_FIELD.eq(schema.getName()))
                .and(DSL.field("TABLE_TYPE", String.class).eq(BASE_TABLE_TYPE_VALUE))
                .fetch(0, String.class)
                .stream().sorted()
                .map(schema::getTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Obtain {@link SizeItem} objects for the given table. Each item will correspond to a log
     * message.
     *
     * @param table table to be interrogated
     * @return size items for the table
     */
    public abstract List<SizeItem> getSizeItems(Table<?> table);

}
