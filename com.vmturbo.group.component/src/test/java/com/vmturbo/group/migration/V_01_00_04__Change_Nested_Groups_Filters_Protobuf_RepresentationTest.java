package com.vmturbo.group.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTOREST.Group.Origin;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestSQLDatabaseConfig.class})
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class V_01_00_04__Change_Nested_Groups_Filters_Protobuf_RepresentationTest {
    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private DSLContext dslContext;

    private V_01_00_04__Change_Nested_Groups_Filters_Protobuf_Representation migration;

    @Before
    public void setup() {
        dslContext = dbConfig.prepareDatabase();
        migration = new V_01_00_04__Change_Nested_Groups_Filters_Protobuf_Representation(dslContext);
    }

    /**
     * Tests treatment of a static nested group: no translation should happen
     * and the record should stay in the database.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testCorrect716StaticNestedGroup() throws Exception {
        final String name = "name";
        final List<Long> oids = ImmutableList.of(1L, 2L, 3L);
        final long groupId = 10L;

        final NestedGroupInfo nestedGroupInfoIn =
            NestedGroupInfo.newBuilder()
                .setName(name)
                .setCluster(ClusterInfo.Type.COMPUTE)
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(oids))
                .build();
        Assert.assertFalse(migration.isDynamic717(nestedGroupInfoIn));
        Assert.assertFalse(migration.isDynamic716(nestedGroupInfoIn));

        final GroupingRecord recordIn = new GroupingRecord(
                                            groupId, name, Origin.USER.getValue(),
                                            Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                                            null, nestedGroupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        final List<GroupingRecord> allRecordsOut = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(1, allRecordsOut.size());
        final GroupingRecord recordOut = allRecordsOut.get(0);
        Assert.assertEquals(recordOut, recordIn);

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        final List<GroupingRecord> allRecordsOut1 = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(allRecordsOut, allRecordsOut1);
    }

    /**
     * Tests translation of a dynamic nested group that parses correctly with the 7.16 format.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testCorrect716DynamicNestedGroup() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final String regex = ".*";
        final StringFilter stringFilter = StringFilter.newBuilder().setStringPropertyRegex(regex).build();

        final NestedGroupInfo nestedGroupInfoIn =
            NestedGroupInfo.newBuilder()
                .setName(name)
                .setCluster(ClusterInfo.Type.COMPUTE)
                .setPropertyFilterList(
                    GroupPropertyFilterList.newBuilder()
                        .addDeprecatedPropertyFiltersOld(
                            GroupPropertyFilterList.GroupPropertyFilter.newBuilder()
                                .setDeprecatedNameFilter(stringFilter)))
                .build();
        Assert.assertFalse(migration.isDynamic717(nestedGroupInfoIn));
        Assert.assertTrue(migration.isDynamic716(nestedGroupInfoIn));

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, nestedGroupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        final List<GroupingRecord> allRecordsOut = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(1, allRecordsOut.size());
        final GroupingRecord recordOut = allRecordsOut.get(0);
        Assert.assertEquals(groupId, (long)recordOut.getId());
        Assert.assertEquals(name, recordOut.getName());
        Assert.assertEquals(Origin.USER.getValue(), (int)recordOut.getOrigin());
        Assert.assertEquals(Type.NESTED_GROUP_VALUE, (int)recordOut.getType());
        Assert.assertEquals(EntityType.PHYSICAL_MACHINE.getValue(), (int)recordOut.getEntityType());
        Assert.assertNull(recordOut.getDiscoveredById());

        final NestedGroupInfo nestedGroupInfoOut = NestedGroupInfo.parseFrom(recordOut.getGroupData());
        Assert.assertTrue(migration.isDynamic717(nestedGroupInfoOut));
        Assert.assertFalse(migration.isDynamic716(nestedGroupInfoOut));
        Assert.assertEquals(name, nestedGroupInfoOut.getName());
        Assert.assertEquals(ClusterInfo.Type.COMPUTE, nestedGroupInfoOut.getCluster());
        Assert.assertEquals(1, nestedGroupInfoOut.getPropertyFilterList().getPropertyFiltersCount());
        final PropertyFilter propertyFilterOut =
                nestedGroupInfoOut.getPropertyFilterList().getPropertyFilters(0);
        Assert.assertEquals(SearchableProperties.DISPLAY_NAME, propertyFilterOut.getPropertyName());
        Assert.assertEquals(stringFilter, propertyFilterOut.getStringFilter());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        final List<GroupingRecord> allRecordsOut1 = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(allRecordsOut, allRecordsOut1);
    }

    /**
     * Tests treatment of a nested group that parses correctly with the 7.17 format.
     * No translation should happen, but the group should remain in the database.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testCorrect717NestedGroup() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final String regex = ".*";
        final StringFilter stringFilter = StringFilter.newBuilder().setStringPropertyRegex(regex).build();

        final NestedGroupInfo nestedGroupInfoIn =
            NestedGroupInfo.newBuilder()
                .setName(name)
                .setCluster(ClusterInfo.Type.COMPUTE)
                .setPropertyFilterList(
                    GroupPropertyFilterList.newBuilder()
                        .addPropertyFilters(
                            PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(stringFilter)))
                .build();
        Assert.assertTrue(migration.isDynamic717(nestedGroupInfoIn));
        Assert.assertFalse(migration.isDynamic716(nestedGroupInfoIn));

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, nestedGroupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        final List<GroupingRecord> allRecordsOut = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(1, allRecordsOut.size());
        final GroupingRecord recordOut = allRecordsOut.get(0);
        Assert.assertEquals(recordOut, recordIn);

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        final List<GroupingRecord> allRecordsOut1 = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(allRecordsOut, allRecordsOut1);
    }

    /**
     * Tests treatment of a byte array that does not parse correctly.
     * The group should be removed from the database.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testBadBlob() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final byte []emptyByteArray = new byte[1];

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, emptyByteArray);
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());
    }

    /**
     * Tests treatment of invalid nested group info.
     * The group should be removed from the database.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testBadProtobuf() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final byte[] emptyByteArray = new byte[1];

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, emptyByteArray);
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());
    }

    /**
     * Tests treatment of nested group info that cannot be categorized into either 7.16 or 7.17.
     * The error is that the property filter does not have a property name.
     * The group should be removed from the database.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testBadNestedGroupCase1() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final String regex = ".*";
        final StringFilter stringFilter = StringFilter.newBuilder().setStringPropertyRegex(regex).build();

        final NestedGroupInfo nestedGroupInfoIn =
            NestedGroupInfo.newBuilder()
                .setCluster(ClusterInfo.Type.COMPUTE)
                .setPropertyFilterList(
                    GroupPropertyFilterList.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder().setStringFilter(stringFilter)))
                .build();
        Assert.assertFalse(migration.isDynamic717(nestedGroupInfoIn));
        Assert.assertFalse(migration.isDynamic716(nestedGroupInfoIn));

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, nestedGroupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());
    }

    /**
     * Tests treatment of nested group info that cannot be categorized into either 7.16 or 7.17.
     * The error is that fields from both formats exist.
     * The group should be removed from the database.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testBadNestedGroupCase2() throws Exception {
        final String name = "name";
        final long groupId = 10L;
        final String regex = ".*";
        final StringFilter stringFilter = StringFilter.newBuilder().setStringPropertyRegex(regex).build();

        final NestedGroupInfo nestedGroupInfoIn =
            NestedGroupInfo.newBuilder()
                .setCluster(ClusterInfo.Type.COMPUTE)
                .setPropertyFilterList(
                    GroupPropertyFilterList.newBuilder()
                        .addPropertyFilters(
                            PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(stringFilter))
                        .addDeprecatedPropertyFiltersOld(
                            GroupPropertyFilterList.GroupPropertyFilter.newBuilder()
                                    .setDeprecatedNameFilter(stringFilter)))
                .build();
        Assert.assertFalse(migration.isDynamic717(nestedGroupInfoIn));
        Assert.assertFalse(migration.isDynamic716(nestedGroupInfoIn));

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.NESTED_GROUP_VALUE, EntityType.PHYSICAL_MACHINE.getValue(),
                null, nestedGroupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        Assert.assertEquals(0, dslContext.selectFrom(Tables.GROUPING).fetch().size());
    }
}
