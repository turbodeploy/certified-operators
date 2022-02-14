package db.migrations.history.postgres;

import org.jooq.SQLDialect;

import db.migrations.history.mariadb.V999999DropMostEntityStatsTablesForTestsMariadb;

/**
 * See {@link V999999DropMostEntityStatsTablesForTestsMariadb} for an explanation of this
 * migration.
 */
public class V999999DropMostEntityStatsTablesForTestsPostgres
        extends V999999DropMostEntityStatsTablesForTestsMariadb {

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.POSTGRES;
    }
}
