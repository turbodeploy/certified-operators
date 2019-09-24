package com.vmturbo.group.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class V_01_00_00__Group_Table_Add_Entity_TypeTest {

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private DSLContext dslContext;

    private V_01_00_00__Group_Table_Add_Entity_Type migration;

    private static final long GROUP_ID = 7L;

    @Before
    public void setup() {
        dslContext = dbConfig.prepareDatabase();
        migration = new V_01_00_00__Group_Table_Add_Entity_Type(dslContext);
    }

    /**
     * Release all resources occupied by test.
     */
    @After
    public void tearDown() {
        dbConfig.clean();
    }

    @Test
    public void testSetEntityTypeGroup() {
        // Make sure the DB is empty.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE).from(Tables.GROUPING)
            .fetch(Tables.GROUPING.ENTITY_TYPE)
            .size(), is(0));

        makeRecord(GROUP_ID, -1, Group.Type.GROUP_VALUE, GroupInfo.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build().toByteArray()).store();
        // Make sure the proper group got injected.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE)
                .from(Tables.GROUPING)
                .fetchSet(Tables.GROUPING.ENTITY_TYPE),
            is(Collections.singleton(-1)));

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Make sure the entity type got updated.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ENTITY_TYPE),
                is(Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE)));
    }

    @Test
    public void testSetEntityTypeComputeCluster() {
        // Make sure the DB is empty.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE).from(Tables.GROUPING)
                .fetch(Tables.GROUPING.ENTITY_TYPE)
                .size(), is(0));

        makeRecord(GROUP_ID, -1, Group.Type.CLUSTER_VALUE,
            ClusterInfo.newBuilder()
                .setClusterType(Type.COMPUTE)
                .build().toByteArray()).store();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Make sure the entity type got updated.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ENTITY_TYPE),
                is(Collections.singleton(EntityType.PHYSICAL_MACHINE_VALUE)));
    }

    @Test
    public void testSetEntityTypeStorageCluster() {
        // Make sure the DB is empty.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE).from(Tables.GROUPING)
                .fetch(Tables.GROUPING.ENTITY_TYPE)
                .size(), is(0));

        makeRecord(GROUP_ID, -1, Group.Type.CLUSTER_VALUE,
                ClusterInfo.newBuilder()
                        .setClusterType(Type.STORAGE)
                        .build().toByteArray()).store();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Make sure the entity type got updated.
        assertThat(dslContext.select(Tables.GROUPING.ENTITY_TYPE)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ENTITY_TYPE),
                is(Collections.singleton(EntityType.STORAGE_VALUE)));
    }

    @Test
    public void testDeleteInvalidGroupTypeGroup() {
        // Entity type needs to be -1 because otherwise the migration won't apply to this entity.
        makeRecord(GROUP_ID, -1,
            // The group type is invalid!
            123,
            GroupInfo.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build().toByteArray()).store();

        // Make sure the proper group got injected.
        assertThat(dslContext.select(Tables.GROUPING.ID)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ID),
                is(Collections.singleton(GROUP_ID)));

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Make sure the entity type got updated.
        assertThat(dslContext.select(Tables.GROUPING.ID)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ID),
                is(Collections.emptySet()));
    }

    @Test
    public void testIgnoreSetEntityTypes() {
        // In kind of a roundabout way - we create an invalid entry with a valid entity type,
        // and make sure the migration has no effect.
        makeRecord(GROUP_ID, EntityType.VIRTUAL_MACHINE_VALUE,
            // This group type is invalid!
            123,
            GroupInfo.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build().toByteArray()).store();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Since we know (according to other test) that if the migration detected an invalid entry
        // it would be deleted, the presence of the entry means the migration ignores it.
        assertThat(dslContext.select(Tables.GROUPING.ID)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ID),
                is(Collections.singleton(GROUP_ID)));
    }

    @Test
    public void testDeleteInvalidGroupDataGroup() {
        // Entity type needs to be -1 because otherwise the migration won't apply to this entity.
        makeRecord(GROUP_ID, -1, Group.Type.GROUP_VALUE,
                // This group data is invalid!
                new byte[]{1, 2, 3}).store();

        // Make sure the proper group got injected.
        assertThat(dslContext.select(Tables.GROUPING.ID)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ID),
                is(Collections.singleton(GROUP_ID)));

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // Make sure the entity type got updated.
        assertThat(dslContext.select(Tables.GROUPING.ID)
                        .from(Tables.GROUPING)
                        .fetchSet(Tables.GROUPING.ID),
                is(Collections.emptySet()));
    }


    private GroupingRecord makeRecord(final long id,
                                      final int entityType,
                                      final int groupType,
                                      final byte[] groupData) {
        final GroupingRecord record = dbConfig.dsl().newRecord(Tables.GROUPING);
        record.setId(id);
        record.setName("Fuel Injected " + id);
        record.setType(groupType);
        record.setOrigin(GroupDTO.Group.Origin.USER_VALUE);
        record.setEntityType(entityType);
        record.setGroupData(groupData);
        return record;
    }
}
