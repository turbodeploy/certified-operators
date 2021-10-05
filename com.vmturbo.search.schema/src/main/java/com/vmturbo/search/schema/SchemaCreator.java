package com.vmturbo.search.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.CreateTableColumnStep;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.mariadb.jdbc.MariaDbDataSource;

import com.vmturbo.search.metadata.DbFieldDescriptor;
import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Implementation of DDL operations over search schema.
 * Intents:
 * - create during build (without a persisted SQL script) in order to run jOOQ code generation
 * - use in runtime to import temporary data
 * - optimize schema for selection/typical queries i.e. paginated and ordered by one of 'standard' fields
 */
public class SchemaCreator implements ISchemaCreator {
    private static final Logger logger = LogManager.getLogger();

    // data writer does not use ORM, or rather uses hand-written 'extractor ORM'
    // but data reader uses jOOQ generated definitions for tables and columns
    private static final String ENTITIES = DbFieldDescriptor.Location.Entities.getTable();
    private static final String ACTIONS = DbFieldDescriptor.Location.Actions.getTable();
    private static final String STRINGS = DbFieldDescriptor.Location.Strings.getTable();
    private static final String NUMERICS = DbFieldDescriptor.Location.Numerics.getTable();
    // TODO string map table (for tags)

    private static final String ID = "id";
    private static final String OID = "oid";
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final int FIELD_NAME_SIZE = 256;

    private final DSLContext dsl;
    private final Map<Location, Function<String, String>> tableToCreateTableQuery =
                    new ImmutableMap.Builder<Location, Function<String, String>>()
                                    .put(Location.Entities,
                                                    (name) -> createMainTable(name,
                                                                    Location.Entities))
                                    .put(Location.Actions,
                                                    (name) -> createMainTable(name,
                                                                    Location.Actions))
                                    .put(Location.Numerics, (name) -> commonTableFields(name)
                                                    .column(NAME, SQLDataType
                                                                    .VARCHAR(FIELD_NAME_SIZE)
                                                                    .nullable(false))
                                                    .column(VALUE, DbFieldDescriptor.NUMERIC_DB_TYPE
                                                                    .nullable(false))
                                                    .constraint(DSL.primaryKey(ID)).toString())
                                    .put(Location.Strings, (name) -> commonTableFields(name).column(
                                                    NAME,
                                                    SQLDataType.VARCHAR(FIELD_NAME_SIZE)
                                                                    .nullable(false))
                                                    .column(VALUE, SQLDataType.VARCHAR(
                                                                    DbFieldDescriptor.STRING_SIZE)
                                                                    .nullable(false))
                                                    .constraint(DSL.primaryKey(ID)).toString())
                                    .build();

    /**
     * Construct the schema creator instance.
     *
     * @param dsl jOOQ context - for query construction, not execution
     */
    public SchemaCreator(@Nonnull DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    @Nonnull
    public List<String> createWithoutIndexes(@Nonnull String suffix, @Nullable Location location) {
        List<String> queries = new ArrayList<>(dropSchema(suffix, location));
        if (location == null) {
            // clustered autoincrement ids to speed up insertion in all tables
            // no nulls allowed anywhere - default values for indexing

            // all search queries are paginated with 'left-off' technique
            // e.g.:
            //    select oid from search_entity where type = 10
            //    and (severity < 1 or severity = 1 and oid > 73162192619280)
            //    order by severity desc, oid asc limit 50
            // for above to be efficient 'severity' needs to be in the same table as type and oid
            // so at least 'standard' fields (that appear by default in UI) are pulled up to entities table
            // to speed up typical paginated/ordered by them queries in large environments
            // NB this only can speed up order by's from single table
            queries.add(tableToCreateTableQuery.get(Location.Entities).apply(ENTITIES + suffix));

            // actions' data are queried and written separately from the rest, upon notifications from AO
            // for faster insertion they need a separate table
            // and that table has to contain redundant oid and type, to avoid joins when paginating
            queries.add(tableToCreateTableQuery.get(Location.Actions).apply(ACTIONS + suffix));

            queries.add(tableToCreateTableQuery.get(Location.Strings).apply(STRINGS + suffix));
            queries.add(tableToCreateTableQuery.get(Location.Numerics).apply(NUMERICS + suffix));
            return queries;
        }
        queries.add(tableToCreateTableQuery.get(location).apply(location.getTable() + suffix));
        return queries;
    }

    @Nonnull
    private String createMainTable(@Nonnull String table, @Nonnull Location location) {
        // entities or actions
        DbFieldDescriptor<?> entityTypeDesc = SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE.getDbDescriptor();
        CreateTableColumnStep commonFields = commonTableFields(table)
                        .column(entityTypeDesc.getColumn(), entityTypeDesc.getDbType());
        Arrays.stream(SearchMetadataMapping.values())
                        // filter out 'oid' and 'type' since they have to be in both tables
                        // and are added explicitly
                        .filter(smm -> smm != SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE
                                        && smm != SearchMetadataMapping.PRIMITIVE_OID)
                        .filter(smm -> smm.getDbDescriptor().getLocations().contains(location))
                        .map(SearchMetadataMapping::getDbDescriptor)
                        .forEach(field -> commonFields.column(field.getColumn(),
                                        field.getDbType().nullable(false)));
        return commonFields.constraint(DSL.primaryKey(ID)).toString();
    }

    private CreateTableColumnStep commonTableFields(String table) {
        return dsl.createTable(table)
                        .column(ID, SQLDataType.BIGINT.nullable(false).identity(true))
                        .column(OID, SQLDataType.BIGINT.nullable(false));
    }

    @Override
    @Nonnull
    public List<String> createIndexes(@Nonnull String suffix) {
        // TODO add indexes - by entity type, oid, field name
        return Collections.emptyList();
    }

    @Override
    @Nonnull
    public List<String> replace(@Nonnull String srcSuffix, @Nonnull String dstSuffix,
            @Nullable Location location) {
        List<String> queries = new ArrayList<>(dropSchema(dstSuffix, location));
        if (location == null) {
            //TODO this should be replaced with Stream.of(Location.values()) when tags support is implemented
            Stream.of(ENTITIES, ACTIONS, STRINGS, NUMERICS).forEach(table -> {
                queries.add(dsl.alterTable(table + srcSuffix)
                        .renameTo(table + dstSuffix)
                        .toString());
                // TODO rename indexes as well
            });
            return queries;
        } else {
            final String table = location.getTable();
            queries.add(dsl.alterTable(table + srcSuffix).renameTo(table + dstSuffix).toString());
            // TODO rename indexes to one table as well
        }
        return queries;
    }

    @Nonnull
    private List<String> dropSchema(@Nonnull String suffix, @Nullable Location location) {
        if (location == null) {
            // if the schema ever starts to change drastically, consider dropping all tables
            // using rdbms schema access methods - for now, hard-code the current tableset
            List<String> queries = new ArrayList<>();
            queries.add(dsl.dropTableIfExists(ENTITIES + suffix).toString());
            queries.add(dsl.dropTableIfExists(ACTIONS + suffix).toString());
            queries.add(dsl.dropTableIfExists(STRINGS + suffix).toString());
            queries.add(dsl.dropTableIfExists(NUMERICS + suffix).toString());
            return queries;
        }
        return Collections.singletonList(
                dsl.dropTableIfExists(location.getTable() + suffix).toString());
    }

    /**
     * Create a schema in the given db.
     * To be executed during build on temporary db, to generate jOOQ wrappers.
     *
     * @param args url, username, password
     */
    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: SchemaCreator [-create|-drop] url database username password");
            return;
        }
        boolean drop = "-drop".equals(args[0]);
        // not using Spring context/DbEndpoint config/auth during build time
        try {
            MariaDbDataSource dataSource = new MariaDbDataSource();
            dataSource.setUrl(args[1]);
            dataSource.setUser(args[3]);
            dataSource.setPassword(args[4]);
            dataSource.setDatabaseName("mysql");
            if (drop) {
                try (Connection conn = dataSource.getConnection()) {
                    DSL.using(conn).execute("DROP DATABASE IF EXISTS " + args[2]);
                }
            } else {
                try (Connection conn = dataSource.getConnection()) {
                    DSL.using(conn).execute("CREATE DATABASE IF NOT EXISTS " + args[2]
                                    + " DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = utf8_unicode_ci;");
                }

                dataSource.setUrl(args[1] + "/" + args[2]);
                try (Connection conn = dataSource.getConnection()) {
                    SchemaCreator creator = new SchemaCreator(DSL.using(conn));
                    // not bothering with transactions as there's only 1.5 rdbms that support ddl ones
                    for (String query : creator.createWithoutIndexes("", null)) {
                        DSL.using(conn).execute(query);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to initialize search schema", e);
            System.exit(1);
        }
    }
}
