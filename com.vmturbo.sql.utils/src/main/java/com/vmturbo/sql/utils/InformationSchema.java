package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.InformationSchemaConstants.BASE_TABLE_TYPE;
import static com.vmturbo.sql.utils.InformationSchemaConstants.PARTITIONS;
import static com.vmturbo.sql.utils.InformationSchemaConstants.PARTITION_DESCRIPTION;
import static com.vmturbo.sql.utils.InformationSchemaConstants.PARTITION_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.PARTITION_ORDINAL_POSITION;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLES;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_SCHEMA;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_TYPE;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * Class to perform operations on a databases' information_schema.
 *
 * <p>This class is not intended to be anything close to comprehensive. It is intended to grow
 * incrementally to meet new needs over time.</p>
 */
public class InformationSchema {

    private InformationSchema() {}

    /**
     * Obtain a list of normal tables (e.g. not views) that exist in the current schema, by querying
     * the database's information schema. Dynamically constructed jOOQ {@link Table} objects are
     * returned unless `jooqSchema` is provided.
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
        dsl = getSchemaRenderingDsl(dsl);
        String schemaName = getSchemaName(jooqSchema, dsl);
        return dsl.select(TABLE_NAME, TABLE_SCHEMA)
                .from(TABLES)
                .where(TABLE_SCHEMA.eq(schemaName).and(TABLE_TYPE.eq(BASE_TABLE_TYPE)))
                .fetch().stream()
                .map(r -> jooqSchema != null
                          ? jooqSchema.getTable(r.value1())
                          : DSL.table(DSL.name(r.value2(), r.value1())))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Table::getName))
                .collect(Collectors.toList());
    }

    /**
     * Obtain information about all the partitions appearing in the given schema, by querying the
     * databases' information schema. This currently works only for MaraiDB/MySQL databases, not
     * Postgres.
     *
     * <p>Results are in the form of a map from {@link Table} objects to jOOQ {@link Record}
     * objects from the result set of the `partitions` table. Included columns include the
     * following, all defined in the {@link InformationSchemaConstants} class.</p>
     * <ul>
     *     <li>TABLE_SCHEMA</li>
     *     <li>TABLE_NAME</li>
     *     <li>PARTITION_NAME</li>
     *     <li>PARTITION_DESCRIPTION</li>
     * </ul>
     *
     * <p>Non-partitioned tables are omitted from the, as are partitions with `0` or `MAXVALUE` as
     * boundaries, as these partitions are not intended to become home to any actual data records
     * and are irrelevant to the operations we need to perform.</p>
     *
     * @param schemaName jOOQ {@link Schema}, or null to dynamically discover current schema
     * @param dsl        {@link DSLContext} for DB access
     * @return Record objects with partition details
     */
    public static Map<String, List<Record>> getPartitions(String schemaName, DSLContext dsl) {
        SQLDialect dialect = dsl.dialect();
        if (dialect != SQLDialect.MARIADB && dialect != SQLDialect.MYSQL) {
            throw new UnsupportedOperationException(
                    String.format("Partition discovery not supported for %s DB dialect", dialect));
        }
        dsl = getSchemaRenderingDsl(dsl);
        String mappedSchemaName = getSchemaName(schemaName, dsl);
        return dsl.select(TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, PARTITION_DESCRIPTION)
                .from(PARTITIONS)
                .where(TABLE_SCHEMA.eq(mappedSchemaName))
                .and(PARTITION_DESCRIPTION.ne("0"))
                .and(PARTITION_DESCRIPTION.ne("MAXVALUE"))
                .orderBy(TABLE_NAME, PARTITION_ORDINAL_POSITION)
                .fetch().stream()
                .collect(Collectors.groupingBy(r -> (String)r.get(TABLE_NAME.getName())));
    }

    private static DSLContext getSchemaRenderingDsl(DSLContext dsl) {
        Configuration renderingConfig = dsl.configuration().derive();
        renderingConfig.settings().setRenderSchema(true);
        return DSL.using(renderingConfig);
    }

    private static String getSchemaName(Schema jooqSchema, DSLContext dsl) {
        return getSchemaName(jooqSchema != null ? jooqSchema.getName() : null, dsl);
    }

    private static String getSchemaName(String schemaName, DSLContext dsl) {
        if (schemaName != null) {
            return JooqUtil.getMappedSchemaName(schemaName, dsl);
        } else {
            Throwable cause = null;
            try {
                Record1<String> record = dsl.select(DSL.currentSchema()).fetchOne();
                if (record != null) {
                    return record.value1();
                }
            } catch (DataAccessException e) {
                cause = e;
            }
            // either query failed or it returned no records
            throw new IllegalStateException("Failed to determine database schema", cause);
        }
    }
}
