package com.vmturbo.repository.migration;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.driver.ArangoGraphDatabaseDriver;
import com.vmturbo.repository.topology.TopologyID;

/**
 * This migration will purge all legacy (7.17 and prior) plans when upgrading to 7.21 which
 * includes Plan Overhaul, multi-tenancy and other breaking changes/improvements.
 *
 * <p>This is necessary despite the fact that we are deleting all of the legacy plans in a separate
 * Plan Orchestrator migration. The Plan Orchestrator will make a request to the Repository to
 * delete each of the old topologies, but those requests will fail due to the naming convention
 * changes introduced for multi-tenancy. Therefore, we need this migration to clean up the old
 * topologies.</p>
 */
public class V_01_00_00__PURGE_ALL_LEGACY_PLANS implements Migration {

    /**
     * For logging migration status.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * The pattern that can be used to convert a legacy database name to a {@link TopologyID}.
     */
    private static final Pattern LEGACY_DB_NAME_PATTERN = Pattern.compile(
        "^topology-(?<contextId>\\d+)-(?<type>SOURCE|PROJECTED)-(?<topologyId>\\d+)");

    /**
     * The name of the special legacy database which stored all the raw JSON protobufs.
     */
    private static final String LEGACY_RAW_TOPOLOGIES_DATABASE_NAME = "topology-protobufs";

    /**
     * The system database is the "special" Arango database that has extra properties
     * and a special role when interacting with other databases.
     *
     * <p>See: https://docs.arangodb.com/3.2/Manual/DataModeling/Databases/WorkingWith.html#issystem</p>
     */
    private static final String SYSTEM_DATABASE_NAME = "_system";

    /**
     * The single underlying ArangoDB connection, shared between all components.
     *
     * <p>Used to list and delete all obsolete plan topologies.</p>
     */
    private final ArangoDatabaseFactory arangoFactory;

    /**
     * For reporting migration progress.
     */
    //TODO: Improve this with an abstract superclass.
    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo = MigrationProgressInfo.newBuilder();

    /**
     * For synchronizing access to the migrationInfo.
     */
    private final Object migrationInfoLock = new Object();

    /**
     * Create an instance of the Purge all Legacy Plans migration.
     *
     * @param arangoFactory provides a connection to ArangoDB
     */
    public V_01_00_00__PURGE_ALL_LEGACY_PLANS(@Nonnull final ArangoDatabaseFactory arangoFactory) {
        this.arangoFactory = Objects.requireNonNull(arangoFactory);
    }

    /**
     * Retrieve the current status of the migration.
     *
     * <p>See {@link MigrationStatus} for the list of states.</p>
     *
     * @return the current {@link MigrationStatus}
     */
    @Override
    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    /**
     * Retrieve the current info about the migration.
     *
     * @return the current {@link MigrationProgressInfo}
     */
    @Override
    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    /**
     * Start the migration, deleting all obsolete plan topologies.
     *
     * @return {@link MigrationProgressInfo} describing the details
     * of the migration
     */
    @Override
    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.RUNNING);
        }
        return deleteLegacyDatabases();
    }

    private MigrationProgressInfo deleteLegacyDatabases() {
        try {
            logger.info("Deleting all plans in the legacy format...");
            // Find all ArangoDB databases
            final Set<String> databases = listDatabases();
            databases.stream()
                // Filter by databases using our old naming convention. Each database represents a
                // single plan topology (source or projected), stored in the ArangoDB graph format.
                .filter(this::matchesLegacyFormat)
                .forEach(this::deleteDatabase);
            // Delete the special legacy database which stored all the raw JSON protobufs
            if (databases.contains(LEGACY_RAW_TOPOLOGIES_DATABASE_NAME)) {
                deleteDatabase(LEGACY_RAW_TOPOLOGIES_DATABASE_NAME);
            }
            return migrationSucceeded();
        } catch (RuntimeException e) {
            logger.error(e.getMessage(), e);
            return migrationFailed(e.getMessage());
        }
    }

    private boolean matchesLegacyFormat(final String databaseName) {
        return LEGACY_DB_NAME_PATTERN.matcher(databaseName).find();
    }

    /**
     * Copied from the 7.17 version of ArangoDatabaseDriverBuilder.
     *
     * @return a list of non-system databases present in ArangoDB
     */
    private Set<String> listDatabases() {
        return arangoFactory.getArangoDriver().getAccessibleDatabases().stream()
            .filter(name -> !name.equals(SYSTEM_DATABASE_NAME))
            .collect(Collectors.toSet());
    }

    /**
     * Adapted from the 7.17 version of ArangoDatabaseDriverBuilder.
     *
     * @param databaseName name of the database to delete
     * @return true, if the database was deleted successfully
     */
    private boolean deleteDatabase(final String databaseName) {
        logger.info("Deleting legacy plan database " + databaseName);
        return arangoFactory.getArangoDriver().db(databaseName).drop();
    }

    /**
     * Generate a migrationInfo indicating a successful migration.
     *
     * @return migrationInfo indicating a successful migration
     */
    @Nonnull
    private MigrationProgressInfo migrationSucceeded() {
        return migrationInfo
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.SUCCEEDED)
            .setCompletionPercentage(100)
            .build();
    }

    /**
     * Generate a migrationInfo indicating a failed migration.
     *
     * @param errorMessage a message indicating why the migration failed
     * @return migrationInfo indicating a failed migration
     */
    @Nonnull
    private MigrationProgressInfo migrationFailed(@Nonnull String errorMessage) {
        return migrationInfo
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.FAILED)
            .setStatusMessage("Migration failed: " + errorMessage)
            .build();
    }
}
