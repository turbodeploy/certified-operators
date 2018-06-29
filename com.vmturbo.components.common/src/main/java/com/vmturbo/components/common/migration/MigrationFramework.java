package com.vmturbo.components.common.migration;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.common.Migration.MigrationInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationRecord;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * This class provides a general framework for running migrations.
 *
 * Only one thread can call the startMigration() method at a time. The read methods
 * can be called by multiple threads.
 */
public class MigrationFramework {

    private final Logger logger = LogManager.getLogger(MigrationFramework.class);

    private static final String MIGRATION_PREFIX = "migrations/";

    final KeyValueStore kvStore;

    volatile List<Migration> migrations;

    volatile ConcurrentMap<String, MigrationRecord.Builder> migrationRecords;

    volatile Migration currentRunningMigration;

    public MigrationFramework(@Nonnull KeyValueStore kvStore) {
        this.kvStore = kvStore;
        this.migrationRecords =
            this.kvStore.getByPrefix(MIGRATION_PREFIX)
                .entrySet().stream()
                .map(entry -> {
                    final MigrationRecord.Builder migrationRecordBuilder =
                        MigrationRecord.newBuilder();
                    try {
                        JsonFormat.parser().merge(entry.getValue(), migrationRecordBuilder);
                        return migrationRecordBuilder;
                    } catch (InvalidProtocolBufferException e){
                        logger.error("Failed to load migration record from KV Store.", e);
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toConcurrentMap(
                            MigrationRecord.Builder::getMigrationName,
                            Function.identity()));
    }

    /**
     * Start migrations.
     *
     * Migrations are run one at a time.
     *
     * @param migrations Migrations which have to be executed. The input is a
     *                   mapping from MigrationName -> Migration.
     * @param forceStartFailedMigration If flag is set, restart FAILED migrations.
     *
     */
    public synchronized void startMigrations(SortedMap<String, Migration> migrations,
                                             boolean forceStartFailedMigration) {
        // Get the migration status from KV Store.
        // Only start those migrations which are:
        //  a) in NOT_STARTED state.
        // OR b) not in the KV store(these are new migrations).
        // OR c) in FAILED state and forceStartFailedMigration is set to true
        logger.info("Starting all data migrations.");
        migrations.forEach((migrationName, migration) -> {
            final MigrationRecord.Builder migrationRecordBuilder =
                migrationRecords.getOrDefault(migrationName,
                    MigrationRecord.newBuilder());

            if (migrationRecordBuilder.getInfo().getStatus() == MigrationStatus.NOT_STARTED
                    || (migrationRecordBuilder.getInfo().getStatus() == MigrationStatus.FAILED
                        && forceStartFailedMigration)) {

                logger.info("Starting migration: {}", migrationName);
                currentRunningMigration = migration;
                migrationRecordBuilder.setMigrationName(migrationName);
                migrationRecordBuilder.setStartTime(LocalDateTime.now().toString());
                migrationRecords.put(migrationName, migrationRecordBuilder);
                MigrationInfo migrationInfo = migration.startMigration();
                // Should we rerun on FAILURE?
                migrationRecordBuilder.setInfo(migrationInfo);
                migrationRecordBuilder.setEndTime(LocalDateTime.now().toString());
                migrationRecords.put(migrationName, migrationRecordBuilder);
                try {
                    kvStore.put(MIGRATION_PREFIX + migrationName,
                            JsonFormat.printer().print(migrationRecordBuilder.build()));
                } catch (InvalidProtocolBufferException e) {
                    logger.error("Failed to persist migration info for {} in KV Store.",
                        migrationName, e);
                }
                logger.info("Finished migration: {}", migrationName);
            }
        });
        logger.info("Finished all migrations.");
    }

    /**
     * List all the migrations.
     *
     */
    public List<MigrationRecord> listMigrations() {
        // Include already executed migrations(stored in KV store)
        // as well as new migrations.
        List<MigrationRecord> records = migrationRecords.values().stream()
                .map(MigrationRecord.Builder::build)
                .collect(Collectors.toList());

        if (migrations != null) {
            migrations.stream()
                .filter(migration ->
                    !migrationRecords.containsKey(migration.getClass().getCanonicalName()))
                .map(migration -> MigrationRecord.newBuilder()
                        .setMigrationName(migration.getClass().getName())
                        .build())
                .map(rec -> records.add(rec));
        }

        return records;
    }

    /**
     * Return the migration record for the give migration.
     *
     * @param migrationName Name of the migration whose migration record is requested.
     *
     */
    public Optional<MigrationRecord> getMigrationRecord(String migrationName) {
        return Optional.ofNullable(migrationRecords.get(migrationName))
                    .map(MigrationRecord.Builder::build);
    }
}
