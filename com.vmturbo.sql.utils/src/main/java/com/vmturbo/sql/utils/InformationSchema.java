package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.InformationSchemaConstants.BASE_TABLE_TYPE;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLES;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_SCHEMA;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_TYPE;

import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Class to perform operations on a databases' information_schema.
 *
 * <p>This class is not intended to be anything close to comprehensive. It is intended to grow
 * incremnetally to meet new needs over time.</p>
 */
public class InformationSchema {
    private InformationSchema() {}

    /**
     * Obtain a list of normal tables (e.g. not views) that exist in the current schema, by querying
     * the database's information schema. Only normal tables (e.g. not views) are retrieved.
     * Dynamically constructed jOOQ {@link Table} objects are returned unless `jooqSchema` is
     * provided.
     *
     * <p>If `jooqSchema` is not null, only tables known in that model are returned, and the
     * returned object will be the pre-instantiated {@link Table} object for the table (i.e. `==
     * jooqSchema.getTable(name)`; others are silently omitted. Importantly in tests, these {@link
     * Table} objects will name the normal production schema, not whatever temporary schema has been
     * created for test execution, in their qualified names.</p>
     *
     * @param jooqSchema jooq Schema to restrict results to those known to jOOQ, and to return
     *                   pre-instantiated {@link Table} objects
     * @param dsl        DSLContext for database access
     * @return tables that appear in the database and in the jOOQ schema
     */
    public static Collection<Table<?>> getTables(@Nullable Schema jooqSchema, DSLContext dsl) {
        dsl.settings().withRenderSchema(true);
        return dsl.select(TABLE_NAME, TABLE_SCHEMA)
                .from(TABLES)
                .where(TABLE_SCHEMA.eq(DSL.currentSchema()).and(TABLE_TYPE.eq(BASE_TABLE_TYPE)))
                .fetch().stream()
                .map(r -> jooqSchema != null
                          ? jooqSchema.getTable(r.value1())
                          : DSL.table(DSL.name(r.value2(), r.value1())))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Table::getName))
                .collect(Collectors.toList());
    }
}
