package com.vmturbo.sql.utils.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Flyway callback to reset the checksum for a migration prior to kicking off a new migrate
 * operation.
 *
 * <p>This can be used when it is necessary to alter a migration after it has been released
 * into the field. The new migration should have the same intended impact - or at least the
 * same intended impact - as any prior migrations with this version number. This must be
 * ensured during design and tested during development; it cannot be verified by this
 * callback.</p>
 */
public class ResetMigrationChecksumCallback extends PreValidationMigrationCallbackBase {

    protected final String migrationVersionToReset;
    private final Set<Integer> obsoleteChecksums;
    private final Integer correctChecksum;

    /**
     * Create a new instance of the callback, to be registered with Flyway prior to starting the
     * migration operation in which it is to operate.
     *
     * @param migrationVersionToReset version number string of migration to reset
     * @param obsoleteChecksums       checksums of all prior migrations that might have been
     *                                applied for this version
     * @param correctChecksum         correct checksum for the current migration for this version,
     *                                can be null (may be appropriate for a Java migration)
     */
    public ResetMigrationChecksumCallback(
            @Nonnull final String migrationVersionToReset,
            @Nonnull final Set<Integer> obsoleteChecksums,
            @Nullable Integer correctChecksum) {
        this.migrationVersionToReset = migrationVersionToReset;
        this.obsoleteChecksums = obsoleteChecksums;
        this.correctChecksum = correctChecksum;

        // log in the name of the executing subclass
    }

    /**
     * Reset the migration checksum to the correct value.
     *
     * <p>This method should only be called when we know the migration table exists and that
     * it already contains a record for the migration version being resset.</p>
     *
     * @param connection JDBC connection
     * @throws SQLException if we encounter a problem
     */
    @Override
    protected void performCallback(Connection connection) throws SQLException {
        final String sql = String.format("UPDATE %s SET checksum = %s WHERE version = '%s'",
                getMigrationTableName(), correctChecksum, migrationVersionToReset);
        connection.createStatement().executeUpdate(sql);
    }

    @Override
    protected String describeCallback() {
        return String.format("reset checksum for migration %s to %s",
                migrationVersionToReset, correctChecksum);
    }

    /**
     * Check if the fix is needed.
     *
     * <p>We only need to apply the fix if the flyway migrations table exists
     * and a record exists for the version being manipulated, with the incorrect checksum.
     *
     * @param connection DB connection
     * @return true if the fix is needed
     * @throws SQLException if there's a problem any of our db operations
     */
    @Override
    protected boolean isCallbackNeeded(Connection connection) throws SQLException {
        final String sql = String.format("SELECT checksum FROM %s where version = '%s'",
                getMigrationTableName(), migrationVersionToReset);
        try (ResultSet result = connection.createStatement().executeQuery(sql)) {
            if (result.next()) {
                final Integer currentChecksum = (Integer)result.getObject(1);
                if (Objects.equals(currentChecksum, correctChecksum)) {
                    // correct checksum is in table - no need to apply fix
                    return false;
                } else if (obsoleteChecksums.contains(currentChecksum)) {
                    // one of the prior checksums is in place - we can fix it
                    return true;
                } else {
                    // current checksum is not correct, and it's not recognized as a known
                    // prior value, so we do nothing, which should mean the migariton will
                    // fail (unfortuantely, the FlywayCallback interface does not provide a
                    // means for the callback to prevent the operation from continuing).
                    logger.error("Invalid existing checksum for migration V{}: {}",
                            migrationVersionToReset, currentChecksum);
                    return false;
                }
            } else {
                // no record retrieved... so we must not have gotten this far in prior
                // migrations. We do not apply the fix in this case.
                return false;
            }
        }
    }
}
