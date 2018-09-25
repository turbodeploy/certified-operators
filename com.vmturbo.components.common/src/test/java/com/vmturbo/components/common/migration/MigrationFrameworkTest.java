package com.vmturbo.components.common.migration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.common.Migration.MigrationInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationRecord;
import com.vmturbo.kvstore.KeyValueStore;

public class MigrationFrameworkTest {

    KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

    MigrationFramework migrationFramework;

    MigrationRecord migrationRecord1 =
        MigrationRecord.newBuilder()
            .setMigrationName("migration1")
            .setInfo(MigrationInfo.newBuilder()
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
        migrationFramework = new MigrationFramework(kvStore);
    }

    @Test
    public void testStartMigrations() {
        Migration testMigration = new TestMigration();
        String migrationName = "V_01_00_00__TestMigration1";
        migrationFramework.startMigrations(ImmutableSortedMap.of(migrationName,
                    testMigration), false);
        MigrationRecord.newBuilder().setInfo(testMigration.getMigrationInfo());
        List<MigrationRecord> migrationRecords = migrationFramework.listMigrations();

        assertThat(migrationRecords.size(), is(2));

        assertThat(migrationRecords.stream()
                    .filter(rec -> rec.getMigrationName().equals(migrationName))
                    .findFirst()
                    .map(rec -> rec.getInfo())
                    .get(),
                is((new TestMigration()).startMigration()));
    }

    @Test
    public void testListMigrations() {
        assertThat(migrationFramework.listMigrations(),
                containsInAnyOrder(migrationRecord1));
    }

    @Test
    public void testGetMigrationRecord() {
            assertThat(migrationFramework.getMigrationRecord("migration1").get(),
                is(migrationRecord1));
    }

    @Test
    public void testGetMigrationRecordNonExistingMigration() {
            assertThat(
                migrationFramework.getMigrationRecord("migrationDoesntExist").isPresent(),
                is(false));
    }

    private String convertToJson(MigrationRecord record) throws Exception{
            return JsonFormat.printer().print(record);
    }

    class TestMigration implements Migration {

        MigrationInfo.Builder migrationBuilder =
                MigrationInfo.newBuilder();


        @Override
        public MigrationInfo startMigration() {
            return migrationBuilder
                    .setStatus(MigrationStatus.FINISHED)
                    .setCompletionPercentage(100.0f)
                    .setAdditionalInfo("Migrated 100 records")
                    .build();
        }

        @Override
        public MigrationInfo getMigrationInfo() {
            return migrationBuilder
                    .setStatus(MigrationStatus.RUNNING)
                    .setCompletionPercentage(60.0f)
                    .build();
        }

        @Override
        public MigrationStatus getMigrationStatus() {
            return MigrationStatus.RUNNING;
        }
    }
}
