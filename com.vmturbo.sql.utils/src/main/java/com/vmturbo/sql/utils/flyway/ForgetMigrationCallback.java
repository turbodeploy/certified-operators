package com.vmturbo.sql.utils.flyway;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class implements a Flyway Callback that removes the record for a given migration version
 * from the migration history table, if the record exists.
 *
 * <p>This migraiton is most commonly needed when two development branches need to be merged,
 * and they both define a migration with the given version. One way to move forward in such a
 * scnenario is to renumber both of the existing migrations to new acceptable version numbers,
 * and then use this callback to remove that version from the migrations history. It is important,
 * in such cases, that the renumbered migrations perform correctly in databases in which either of
 * the original migrations has already been executed. This may require that either or both of the
 * renumbered migrations differ from the originals.</p>
 */
public class ForgetMigrationCallback extends PreValidationMigrationCallbackBase {

    private final String migrationVersionToForget;

    /**
     * Create a new callback instance.
     *
     * @param migrationVersionToForget the version number to be removed from migration history
     */
    public ForgetMigrationCallback(String migrationVersionToForget) {
        this.migrationVersionToForget = migrationVersionToForget;
    }

    @Override
    protected void performCallback(final Connection connection) throws SQLException {
        connection.createStatement().executeUpdate(String.format(
                "DELETE FROM %s WHERE version='%s'",
                getMigrationTableName(), migrationVersionToForget));
    }

    /**
     * Perform the fix if the record for our version appears in the migrations table.
     *
     * <p>We could skip this and just always perform the fix with no performance penalty, but
     * then we'd always get the log message, even when the record is long gone and the callback
     * isn't changing anything.</p>
     *
     * @param connection JDBC connection
     * @return true if our record exists
     */
    @Override
    protected boolean isCallbackNeeded(final Connection connection) throws SQLException {
        final String sql = String.format("SELECT 1 FROM %s WHERE version = '%s'",
                getMigrationTableName(), migrationVersionToForget);
        return connection.createStatement().executeQuery(sql).next();
    }

    @Override
    protected String describeCallback() {
        return String.format("Remove migration %s from migration history", migrationVersionToForget);
    }
}
