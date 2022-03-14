package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.InformationSchemaConstants.COLLATION_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.COLUMNS;
import static com.vmturbo.sql.utils.InformationSchemaConstants.COLUMN_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.DATA_TYPE;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_NAME;
import static com.vmturbo.sql.utils.InformationSchemaConstants.TABLE_SCHEMA;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Unit test for verifying that the tables/columns/... for different databases are legit. For
 * example, all the columns in postgres should have a case insensitive collation, unless the
 * column is designed to be case sensitive.
 */
public class MultiDbSchemaTest extends MultiDbTestBase {

    private static final List<String> POSTGRES_STRING_DATA_TYPES = ImmutableList.of(
            "text", "character", "character varying");

    private static final String POSTGRES_CASE_INSENSITIVE_COLLATION_NAME = "ci";

    private static final String FLYWAY_SCHEMA_VERSION = "schema_version";

    private final DSLContext dsl;
    private final Set<Pair<String, String>> caseSensitiveColumns;

    /**
     * Construct a rule chaining for a given scenario.
     *
     * @param schema {@link Schema} that will be provisioned for this test class
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @param schemaName used in configuring some DbEndpoint properties
     * @param endpointByDialect obtain an endpoint when using configurable dialect
     */
    public MultiDbSchemaTest(Schema schema, boolean configurableDbDialect, SQLDialect dialect,
            String schemaName, @Nullable Function<SQLDialect, DbEndpoint> endpointByDialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        this(schema, configurableDbDialect, dialect, schemaName, endpointByDialect,
                Collections.emptyList());
    }

    /**
     * Construct a rule chaining for a given scenario.
     *
     * @param schema {@link Schema} that will be provisioned for this test class
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @param schemaName used in configuring some DbEndpoint properties
     * @param endpointByDialect obtain an endpoint when using configurable dialect
     * @param caseSensitiveColumns columns which are case sensitive
     */
    public MultiDbSchemaTest(Schema schema, boolean configurableDbDialect, SQLDialect dialect,
            String schemaName, @Nullable Function<SQLDialect, DbEndpoint> endpointByDialect,
            List<Pair<String, String>> caseSensitiveColumns)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(schema, configurableDbDialect, dialect, schemaName, endpointByDialect);
        this.caseSensitiveColumns = new HashSet<>(caseSensitiveColumns);
        this.dsl = super.getDslContext();
    }

    /**
     * Test that case sensitive columns do not have a collation set.
     *
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    @Test
    public void testPostgresCaseSensitiveColumns()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (!dsl.dialect().equals(SQLDialect.POSTGRES)) {
            return;
        }
        Configuration config = dsl.configuration().derive();
        config.settings().withRenderSchema(true);

        // check case sensitive columns
        Set<Pair<String, String>> actualCaseSensitiveTableColumns =
                DSL.using(config).select(TABLE_NAME, COLUMN_NAME)
                        .from(COLUMNS)
                        .where(TABLE_SCHEMA.eq(getTestEndpoint().getDbEndpoint().getConfig().getSchemaName())
                                .and(TABLE_NAME.ne(FLYWAY_SCHEMA_VERSION))
                                .and(DATA_TYPE.in(POSTGRES_STRING_DATA_TYPES))
                                .and(COLLATION_NAME.isNull()))
                        .stream()
                        .map(record -> Pair.of(record.component1(), record.component2()))
                        .collect(Collectors.toSet());
        assertEquals(caseSensitiveColumns, actualCaseSensitiveTableColumns);
    }

    /**
     * Test that case insensitive columns have a collation set.
     *
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    @Test
    public void testPostgresCaseInsensitiveColumns()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (!dsl.dialect().equals(SQLDialect.POSTGRES)) {
            return;
        }
        Configuration config = dsl.configuration().derive();
        config.settings().withRenderSchema(true);

        // there should be no columns left if we exclude known case sensitive columns, and the
        // columns using a case insensitive collation "ci"
        Condition con = COLLATION_NAME.isDistinctFrom(POSTGRES_CASE_INSENSITIVE_COLLATION_NAME);
        for (Pair<String, String> pair : caseSensitiveColumns) {
            con = con.and(TABLE_NAME.ne(pair.getKey()).and(COLUMN_NAME.ne(pair.getValue())));
        }

        int count = DSL.using(config).selectCount()
                .from(COLUMNS)
                .where(TABLE_SCHEMA.eq(getTestEndpoint().getDbEndpoint().getConfig().getSchemaName())
                        .and(TABLE_NAME.ne(FLYWAY_SCHEMA_VERSION))
                        .and(DATA_TYPE.in(POSTGRES_STRING_DATA_TYPES))
                        .and(con))
                .fetchOne()
                .value1();
        assertEquals(0, count);
    }
}
