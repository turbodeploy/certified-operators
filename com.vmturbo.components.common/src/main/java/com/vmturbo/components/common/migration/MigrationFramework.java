package com.vmturbo.components.common.migration;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
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

    private static final String DATA_VERSION_PATH = "dataVersion";

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
    public synchronized void startMigrations(@Nonnull SortedMap<String, Migration> migrations,
                                             boolean forceStartFailedMigration) {
        // Get the migration status from KV Store.
        // Only start those migrations which are:
        //  a) in NOT_STARTED state.
        // OR b) not in the KV store(these are new migrations).
        // OR c) in FAILED state and forceStartFailedMigration is set to true
        String currentDataVersion = getCurrentDataVersion(migrations.keySet());
        logger.info("Starting all data migrations.");
        migrations.forEach((migrationName, migration) -> {
            String migrationVersion = extractVersionNumberFromMigrationName(migrationName);
            if (migrationVersion.isEmpty()) {
               throw new RuntimeException("Wrong format for migrationName: " + migrationName);
            }

            final MigrationRecord.Builder migrationRecordBuilder =
                    migrationRecords.getOrDefault(migrationName,
                            MigrationRecord.newBuilder());

            if (migrationRecordBuilder.getProgressInfo().getStatus() == MigrationStatus.NOT_STARTED
                    || (migrationRecordBuilder.getProgressInfo().getStatus() == MigrationStatus.FAILED
                        && forceStartFailedMigration)) {

                logger.info("Starting migration: {}", migrationName);
                currentRunningMigration = migration;
                migrationRecordBuilder.setMigrationName(migrationName);
                migrationRecordBuilder.setStartTime(LocalDateTime.now().toString());
                migrationRecords.put(migrationName, migrationRecordBuilder);
                MigrationProgressInfo migrationInfo;
                if (migrationVersion.compareTo(currentDataVersion) <= 0 ) {
                    String msg = "Skipping Migration: " + migrationVersion +
                            " as it's version is <= current data version:" + currentDataVersion;
                    logger.info(msg);
                    migrationInfo = MigrationProgressInfo.newBuilder()
                                        .setCompletionPercentage(100)
                                        .setStatus(MigrationStatus.SUCCEEDED)
                                        .setStatusMessage(msg)
                                        .build();
                } else {
                    migrationInfo = migration.startMigration();
                }
                // Should we rerun on FAILURE?
                migrationRecordBuilder.setProgressInfo(migrationInfo);
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
            kvStore.put(DATA_VERSION_PATH, migrationVersion);
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

    /**
     *  Fetch the Data Version for this component from Consul.
     *
     *  If there is no data version set, return an empty string.
     */
    private String fetchDataVersion() {
        return kvStore.get(DATA_VERSION_PATH).orElse("");
    }

    // Fetch the current data version for the component from Consul.
    // If no version exists, set the version number to max(0, highest_version_number in migrationName)
    // and update this value in Consul.
    public String getCurrentDataVersion(Collection<String> migrationNames) {
        String currentDataVersion = fetchDataVersion();
        if (currentDataVersion.isEmpty()) {
            currentDataVersion = migrationNames.isEmpty() ? "" :
                    extractVersionNumberFromMigrationName(new TreeSet<String>(migrationNames).last());
            if (currentDataVersion.isEmpty()) {
                currentDataVersion = "00_00_00";
            }
            kvStore.put(DATA_VERSION_PATH, currentDataVersion);
        }

        return currentDataVersion;
    }

    /**
     * Return the version number from the given migrationName
     * The name should be of the form:
     * V_XX_XX_XX__name
     * Where X is a number from [0-9]
     *
     * The function will return the version number string in the form: XX_XX_XX
     *
     */
    public static String extractVersionNumberFromMigrationName(String migrationName) {
        if (migrationName == null || migrationName.isEmpty()) return "";

        Pattern p = Pattern.compile("V_((\\d{2}_\\d{2}_\\d{2}))__\\S+");
        Matcher m = p.matcher(migrationName);

        return m.find() ? m.group(1) : "";
    }
}
