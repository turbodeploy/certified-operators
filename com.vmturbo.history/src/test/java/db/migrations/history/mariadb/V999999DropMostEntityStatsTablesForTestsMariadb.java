package db.migrations.history.mariadb;

import java.sql.Connection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.BaseJdbcMigration;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.sql.utils.SchemaCleaner;

/**
 * This is a test-only migration that will drop all entity-stats tables that are not in the
 * `keptTables` list. This dramatically reduces the overhead of scanning for empty tables during
 * {@link SchemaCleaner} operations.
 *
 * <p>This migration will not appear in any product images because it appears in the test branch.
 * Its super-high migration version is intended to ensure that it will always be the last migration
 * applied by Flyway.</p>
 *
 * <p>All the entity-stats table families are identically structured and used, so it should rarely
 * be the case that a new test needs to make use of a table dropped by this migration. But if that
 * happens (e.g. a test needs to make use of two different entity-stats families), you can add
 * needed tables to the list. Otherwise, attempts to access these tables will seem fine until it
 * comes time to access the table in a live databasea test, at which point the database operation
 * will fail.</p>
 */
public class V999999DropMostEntityStatsTablesForTestsMariadb extends BaseJdbcMigration
        // implementing MigrationInfoProvider allows us to use checkstyle conforming class name
        // by providing version & description separately.
        implements MigrationInfoProvider {

    protected static final Set<Table<?>> keptTables = ImmutableSet.of(
            Tables.VM_STATS_LATEST,
            Tables.VM_STATS_BY_HOUR,
            Tables.VM_STATS_BY_DAY,
            Tables.VM_STATS_BY_MONTH
    );

    protected SQLDialect getDialect() {
        return SQLDialect.MARIADB;
    }

    @Override
    public void migrate(Connection connection) throws Exception {
        DSLContext dsl = DSL.using(connection, getDialect());
        dsl.settings().setRenderSchema(false);
        EntityType.allEntityTypes().stream()
                .filter(t -> t.persistsStats() && t.rollsUp()
                        && !keptTables.contains(t.getLatestTable().orElse(null)))
                .forEach(t -> {
                    t.getLatestTable().map(dsl::dropTable).ifPresent(Query::execute);
                    t.getHourTable().map(dsl::dropTable).ifPresent(Query::execute);
                    t.getDayTable().map(dsl::dropTable).ifPresent(Query::execute);
                    t.getMonthTable().map(dsl::dropTable).ifPresent(Query::execute);
                });
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("999999");
    }

    @Override
    public String getDescription() {
        return "Drop most entity stats tables for tests";
    }
}
