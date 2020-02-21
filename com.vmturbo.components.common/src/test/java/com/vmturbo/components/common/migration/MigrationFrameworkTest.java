package com.vmturbo.components.common.migration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.util.JsonFormat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationRecord;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.kvstore.KeyValueStore;

public class MigrationFrameworkTest {

    KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

    MigrationFramework migrationFramework;

    MigrationRecord migrationRecord1 =
        MigrationRecord.newBuilder()
            .setMigrationName("V_01_00_00__migration1")
            .setProgressInfo(MigrationProgressInfo.newBuilder()
                .setStatus(MigrationStatus.NOT_STARTED)
                .build())
            .build();

    Map<String, String> migrationRecordsMap;

    @Before
    public void setup() throws Exception {
        migrationRecordsMap = new HashMap<>();
        migrationRecordsMap.put(migrationRecord1.getMigrationName(),
            convertToJson(migrationRecord1));

        when(kvStore.getByPrefix(any())).thenReturn(migrationRecordsMap);
        when(kvStore.get(any())).thenReturn(Optional.empty());
        migrationFramework = new MigrationFramework(kvStore);
    }

    @Test
    public void testStartMigrationsNoDataVersionPresent() {
        Migration testMigration = new TestMigration();
        String migrationName = "V_02_00_00__TestMigration1";
        migrationFramework.startMigrations(ImmutableSortedMap.of(
                migrationName, testMigration),
                false);
        List<MigrationRecord> migrationRecords = migrationFramework.listMigrations();

        assertThat(migrationRecords.size(), is(2));

        MigrationRecord migrationRecord = MigrationRecord.newBuilder()
                .setMigrationName(migrationName)
                .setProgressInfo(MigrationProgressInfo.newBuilder()
                        .setStatus(MigrationStatus.SUCCEEDED)
                        .setCompletionPercentage(100f)
                        .setStatusMessage("Skipping Migration: 02_00_00 as it's version is <= current data version:02_00_00")
                        .build())
                .build();

        assertThat(migrationRecords.stream()
                    .filter(rec -> rec.getMigrationName().equals(migrationName))
                    .findFirst()
                    .map(rec -> rec.getProgressInfo())
                    .get(),
                is(migrationRecord.getProgressInfo()));

        assertThat(migrationRecords.stream()
                    .filter(record -> record.getMigrationName().equals(migrationRecord1.getMigrationName()))
                    .allMatch(record -> record.getProgressInfo().getStatus() == MigrationStatus.NOT_STARTED),
                is(true));
    }

    @Test
    public void testStartMigrationsDataVersionPresent() {

        String migrationName = "V_02_00_00__TestMigration1";
        migrationFramework.startMigrations(ImmutableSortedMap.of(
                migrationRecord1.getMigrationName(), new TestMigration(),
                migrationName, new TestMigration()),
                false);

        List<MigrationRecord> migrationRecords = migrationFramework.listMigrations();

        assertThat(migrationRecords.size(), is(2));

        assertThat(migrationRecords.stream()
                        .map(record -> record.getProgressInfo())
                        .allMatch(info -> info.getStatus() == MigrationStatus.SUCCEEDED
                                && info.getCompletionPercentage() == 100f),
                is(true));
    }

    @Test
    public void testListMigrations() {
        assertThat(migrationFramework.listMigrations(),
                containsInAnyOrder(migrationRecord1));
    }

    @Test
    public void testGetMigrationRecord() {
            assertThat(migrationFramework.getMigrationRecord(migrationRecord1.getMigrationName()).get(),
                is(migrationRecord1));
    }

    @Test
    public void testGetMigrationRecordNonExistingMigration() {
            assertThat(
                migrationFramework.getMigrationRecord("migrationDoesntExist").isPresent(),
                is(false));
    }

    @Test
    public void testExtractVersionNumber() {
        String validVersionName = "V_07_04_00__migration_1";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(validVersionName), is("07_04_00"));

        validVersionName = "V_00_00_99__m";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(validVersionName), is("00_00_99"));

        String inValidVersionName = "V_07_04_aa";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(inValidVersionName), is(""));

        inValidVersionName = "V_07_04__migration";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(inValidVersionName), is(""));

        inValidVersionName = "07_04_00_migration";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(inValidVersionName), is(""));

        inValidVersionName = "V_7_4_00_migration";
        assertThat(MigrationFramework.extractVersionNumberFromMigrationName(inValidVersionName), is(""));
    }

    private String convertToJson(MigrationRecord record) throws Exception{
            return JsonFormat.printer().print(record);
    }

    class TestMigration implements Migration {

        MigrationProgressInfo.Builder migrationBuilder =
                MigrationProgressInfo.newBuilder();


        @Override
        public MigrationProgressInfo startMigration() {
            return migrationBuilder
                    .setStatus(MigrationStatus.SUCCEEDED)
                    .setCompletionPercentage(100.0f)
                    .setStatusMessage("Migrated 100 records")
                    .build();
        }
    }
}
