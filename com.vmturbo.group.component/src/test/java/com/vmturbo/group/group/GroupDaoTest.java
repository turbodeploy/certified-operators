package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroupId;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test to cover {@link GroupDAO} functionality.
 */
public class GroupDaoTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final Set<MemberType> EXPECTED_MEMBERS =
            ImmutableSet.of(MemberType.newBuilder().setEntity(1).build(),
                    MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                    MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build());
    private static final long OID1 = 100001L;
    private static final long OID2 = 100002L;
    private static final long OID3 = 100003L;
    private static final long OID4 = 100004L;
    private static final long OID5 = 100005L;

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GroupDAO groupStore;
    private TestGroupGenerator groupGenerator;

    /**
     * Initialize local variables.
     */
    @Before
    public void setup() {
        this.groupGenerator = new TestGroupGenerator();
        groupStore = new GroupDAO(dbConfig.getDslContext());
    }

    /**
     * Method tests how updating of target groups work in group store. It is expected, that a new
     * collection passed into group store will completely replace the previous collection of groups
     * (scoped for the target).
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testUpdateDiscoveredGroupCollection() throws Exception {
        final Origin user = groupGenerator.createUserOrigin();
        final GroupDefinition userGroup1Def = createGroupDefinition();
        final GroupDefinition userGroup2Def = createGroupDefinition();
        groupStore.createGroup(OID1, user, userGroup1Def, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, user, userGroup2Def, EXPECTED_MEMBERS, true);
        final String src1 = "discovered-group-1";
        final String src2 = "discovered-group-2";
        final String src3 = "discovered-group-3";
        final DiscoveredGroup group1 = createUploadedGroup(src1, Arrays.asList(1L, 2L));
        final DiscoveredGroup group2 = createUploadedGroup(src2, Arrays.asList(1L, 2L, 3L));
        final DiscoveredGroup group3 = createUploadedGroup(src3, Arrays.asList(1L, 2L, 3L, 4L));
        groupStore.updateDiscoveredGroups(Collections.singleton(group1), Collections.emptyList(),
                Collections.emptySet());
        Assert.assertEquals(Collections.singleton(group1.getOid()),
                groupStore.getDiscoveredGroupsIds()
                        .stream()
                        .map(DiscoveredGroupId::getIdentity)
                        .map(DiscoveredObjectVersionIdentity::getOid)
                        .collect(Collectors.toSet()));
        final GroupDTO.Grouping agroup1 = getGroupFromStore(group1.getOid());
        assertGroupsEqual(group1, agroup1);
        Assert.assertEquals(Arrays.asList(1L, 2L),
                agroup1.getOrigin().getDiscovered().getDiscoveringTargetIdList());

        groupStore.updateDiscoveredGroups(Arrays.asList(group2, group3), Collections.emptyList(),
                Collections.singleton(group1.getOid()));
        Assert.assertEquals(Sets.newHashSet(group2.getOid(), group3.getOid()),
                groupStore.getDiscoveredGroupsIds()
                        .stream()
                        .map(DiscoveredGroupId::getIdentity)
                        .map(DiscoveredObjectVersionIdentity::getOid)
                        .collect(Collectors.toSet()));
        final GroupDTO.Grouping agroup2 = getGroupFromStore(group2.getOid());
        final GroupDTO.Grouping agroup3 = getGroupFromStore(group3.getOid());

        assertGroupsEqual(group2, agroup2);
        assertGroupsEqual(group3, agroup3);
        final Grouping userGroup1 = getGroupFromStore(OID1);
        final Grouping userGroup2 = getGroupFromStore(OID2);
        Assert.assertEquals(userGroup1Def, userGroup1.getDefinition());
        Assert.assertEquals(userGroup2Def, userGroup2.getDefinition());

        groupStore.updateDiscoveredGroups(Collections.singleton(group1),
                Arrays.asList(group2, group3), Collections.emptySet());
        Assert.assertEquals(Sets.newHashSet(group1.getOid(), group2.getOid(), group3.getOid()),
                groupStore.getDiscoveredGroupsIds()
                        .stream()
                        .map(DiscoveredGroupId::getIdentity)
                        .map(DiscoveredObjectVersionIdentity::getOid)
                        .collect(Collectors.toSet()));
    }

    /**
     * Tests the case where the name of the group is does not fit the db column.
     *
     * @throws StoreOperationException if something goes wrong.
     */
    @Test
    public void testCreateGroupWithOversizeName() throws StoreOperationException {
        String displayName = StringUtils.repeat("abc", 100);
        final GroupDefinition groupDefinition = createGroupDefinition()
            .toBuilder().setDisplayName(displayName).build();
        final DiscoveredGroup group1 = new DiscoveredGroup(OID1, groupDefinition, "grp1",
            Collections.singleton(100L),
            Collections.singleton(MemberType.newBuilder().setEntity(1).build()), false);
        groupStore.updateDiscoveredGroups(Collections.singleton(group1), Collections.emptyList(),
            Collections.emptySet());

        final GroupDTO.Grouping storeGroup = getGroupFromStore(OID1);
        Assert.assertEquals(displayName.substring(0, 200),
            storeGroup.getDefinition().getDisplayName().substring(0, 200));
    }

    /**
     * Tests editing of discovered group. This operation is prohibited because discovered groups
     * could not be edited manually. {@link StoreOperationException} is expected
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testUpdateDiscoveredGroup() throws Exception {
        final DiscoveredGroup group1 = createUploadedGroup("smth", Arrays.asList(1L, 2L));
        groupStore.updateDiscoveredGroups(Collections.singleton(group1), Collections.emptyList(),
                Collections.emptySet());
        final long groupId = group1.getOid();
        final GroupDefinition newDefinition = createGroupDefinition();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT));
        groupStore.updateGroup(groupId, newDefinition, Collections.emptySet(), false);
    }

    /**
     * Tests editing of group that is absent in the DB. {@link StoreOperationException} is expected.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testUpdateAbsentGroup() throws Exception {
        final GroupDefinition newDefinition = createGroupDefinition();
        expectedException.expect(new StoreExceptionMatcher(Status.NOT_FOUND));
        groupStore.updateGroup(-1, newDefinition, Collections.emptySet(), false);
    }

    /**
     * Tests invalid group definition passed. {@link StoreOperationException} is expected.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testUpdateGroupIncorrectDefinition() throws Exception {
        final GroupDefinition newDefinition =
                GroupDefinition.newBuilder(createGroupDefinition()).clearDisplayName().build();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT));
        groupStore.updateGroup(-1, newDefinition, Collections.emptySet(), false);
    }

    /**
     * Tests updat that is introducing a duplicated group name. {@link StoreOperationException} is
     * expected to be thrown
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testUpdateGroupDuplicatingName() throws Exception {
        final GroupDefinition groupDef1 = createGroupDefinition();
        final GroupDefinition groupDef2 = createGroupDefinition();
        final GroupDefinition groupDef3 = GroupDefinition.newBuilder(groupDef1)
                .setDisplayName(groupDef2.getDisplayName())
                .build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(OID1, origin, groupDef1, EXPECTED_MEMBERS, false);
        groupStore.createGroup(OID2, origin, groupDef2, EXPECTED_MEMBERS, false);
        expectedException.expect(new StoreExceptionMatcher(Status.ALREADY_EXISTS));
        groupStore.updateGroup(OID1, groupDef3, Collections.emptySet(), false);
    }

    /**
     * Test the case where a group is created by system and then gets updated.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testUpdateSystemGroup() throws Exception {
        final GroupDefinition groupDefinition = createGroupDefinition();
        final Origin origin = createSystemOrigin();
        final Set<MemberType> memberTypes =
                Collections.singleton(MemberType.newBuilder().setEntity(1).build());

        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, true);

        final Grouping originalGroup = getGroupFromStore(OID1);

        GroupDefinition updatedGroupDefinition = GroupDefinition.newBuilder(groupDefinition)
                .setDisplayName("Updated display name")
                .build();

        Grouping updatedGrouping =
                groupStore.updateGroup(OID1, updatedGroupDefinition, memberTypes, true);

        Assert.assertEquals("Updated display name",
                updatedGrouping.getDefinition().getDisplayName());

        Assert.assertEquals(originalGroup.getOrigin(), updatedGrouping.getOrigin());
        Assert.assertEquals(originalGroup.getExpectedTypesList(),
                updatedGrouping.getExpectedTypesList());
        Assert.assertEquals(groupDefinition, updatedGrouping.getDefinition()
                .toBuilder()
                .setDisplayName(groupDefinition.getDisplayName())
                .build());
    }

    /**
     * Tests creation of system group.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateSystemGroups() throws Exception {
        final GroupDefinition groupDefinition = createGroupDefinition();
        final Origin origin = createSystemOrigin();
        final Set<MemberType> memberTypes = new HashSet<>(
                Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                        MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build()));

        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, true);
        final GroupDTO.Grouping group1 = getGroupFromStore(OID1);
        Assert.assertEquals(origin, group1.getOrigin());
        Assert.assertEquals(groupDefinition, group1.getDefinition());
        Assert.assertEquals(memberTypes, new HashSet<>(group1.getExpectedTypesList()));
        Assert.assertTrue(group1.getSupportsMemberReverseLookup());
    }

    /**
     * Tests creation of user group.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateUserGroup() throws Exception {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.STORAGE_CLUSTER)
                .build();
        final Origin origin = createUserOrigin();
        final Set<MemberType> memberTypes = new HashSet<>(
                Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build()));

        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, false);
        final GroupDTO.Grouping group1 = getGroupFromStore(OID1);
        Assert.assertEquals(origin, group1.getOrigin());
        Assert.assertEquals(groupDefinition, group1.getDefinition());
        Assert.assertEquals(memberTypes, new HashSet<>(group1.getExpectedTypesList()));
        Assert.assertFalse(group1.getSupportsMemberReverseLookup());

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(
                                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER))
                                .addMembers(OID1)))
                .build();

        groupStore.createGroup(OID2, origin, groupDefinition2, memberTypes, false);
        final GroupDTO.Grouping group2 = getGroupFromStore(OID2);
        Assert.assertEquals(origin, group2.getOrigin());
        Assert.assertEquals(groupDefinition2, group2.getDefinition());
        Assert.assertEquals(memberTypes, new HashSet<>(group2.getExpectedTypesList()));
        Assert.assertFalse(group2.getSupportsMemberReverseLookup());
    }

    /**
     * Tests creating  a group with a property filter of a different type compared to the one we
     * are expecting.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void createUserGroupWithInvalidTypeOfProperty() throws Exception {
        final GroupDefinition groupDefinition = createGroupDefinitionWithInvalidId();
        final Origin origin = createUserOrigin();
        final Set<MemberType> memberTypes = new HashSet<>(
            Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build()));
        expectedException.expect(IllegalArgumentException.class);
        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, false);

    }

    /**
     * Tests creating of a group with property filter for the property that is not supported.
     * Runtime exception is expected.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void createUserGroupWithInvalidGroupProperty() throws Exception {
        final String nonExistingProperty = "some fantastical not existing property";
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .clearStaticGroupMembers()
                .setGroupFilters(GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                                .addPropertyFilters(PropertyFilter.newBuilder()
                                        .setPropertyName(nonExistingProperty))))
                .build();
        final Set<MemberType> memberTypes = new HashSet<>(
                Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build()));
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(nonExistingProperty);
        groupStore.createGroup(OID1, createUserOrigin(), groupDefinition, memberTypes, false);
    }

    /**
     * Tests creation and retrieval of a empty static group.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateEmptyStaticGroup() throws Exception {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder().build())
                .build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(OID1, origin, groupDefinition, Collections.emptySet(), true);
        final GroupDTO.Grouping group = getGroupFromStore(OID1);
        Assert.assertEquals(groupDefinition, group.getDefinition());
    }

    /**
     * Tests creation of user group where a parent group has a wrong reference to a child group -
     * wrong group type.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateUserGroupInvalidStaticGroupMemberType() throws Exception {
        final GroupDefinition groupDefinition = createGroupDefinition();
        final Origin origin = createUserOrigin();
        final Set<MemberType> memberTypes = new HashSet<>(
                Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                        MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build()));

        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, false);

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(
                                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER))
                                .addMembers(OID1)))
                .build();

        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT,
                GroupType.STORAGE_CLUSTER.toString()));
        groupStore.createGroup(OID2, origin, groupDefinition2, memberTypes, false);
    }

    /**
     * Tests creation of user group where a parent group has a wrong reference to a child group -
     * an absent group.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateUserGroupInvalidStaticGroupMemberOid() throws Exception {
        final Origin origin = createUserOrigin();
        final Set<MemberType> memberTypes = new HashSet<>(
                Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                        MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build()));

        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(
                                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER))
                                .addMembers(-1)))
                .build();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT, "-1"));
        groupStore.createGroup(OID1, origin, groupDefinition, memberTypes, false);
    }

    /**
     * Tests creation of discovered group. The call is expected to fail. Discovered groups could
     * not be created using this call.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testCreateDiscoveredGroup() throws Exception {
        final Origin origin = Origin.newBuilder()
                .setDiscovered(Origin.Discovered.newBuilder()
                        .setSourceIdentifier("src-id")
                        .addDiscoveringTargetId(333L))
                .build();
        final GroupDefinition groupDefinition = createGroupDefinition();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT));
        groupStore.createGroup(OID1, origin, groupDefinition, Collections.emptySet(), false);
    }

    /**
     * Tests covers several groups added to store. 2 groups are added and retrieved successfully.
     * Finally, a group with duplicated name is added. This addition is expected to fail.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateVariousGroups() throws Exception {
        final Origin origin1 = createUserOrigin();
        final Origin origin2 = createUserOrigin();
        final Set<MemberType> memberTypes = Collections.emptySet();
        final GroupDefinition groupDefinition1 = GroupDefinition.newBuilder(createGroupDefinition())
                .clearStaticGroupMembers()
                .setEntityFilters(EntityFilters.getDefaultInstance())
                .setOwner(34546L)
                .build();
        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .clearStaticGroupMembers()
                .setGroupFilters(GroupFilters.getDefaultInstance())
                .build();
        final GroupDefinition groupDuplicated = GroupDefinition.newBuilder(createGroupDefinition())
                .setDisplayName(groupDefinition1.getDisplayName())
                .build();

        groupStore.createGroup(OID1, origin1, groupDefinition1, memberTypes, false);
        groupStore.createGroup(OID2, origin2, groupDefinition2, memberTypes, false);
        final GroupDTO.Grouping group1 = getGroupFromStore(OID1);
        final GroupDTO.Grouping group2 = getGroupFromStore(OID2);
        Assert.assertThat(group1.getDefinition(), new GroupDefinitionMatcher(groupDefinition1));
        Assert.assertThat(group2.getDefinition(), new GroupDefinitionMatcher(groupDefinition2));

        expectedException.expect(new StoreExceptionMatcher(Status.ALREADY_EXISTS));
        groupStore.createGroup(OID3, origin1, groupDuplicated, memberTypes, false);
    }

    /**
     * Tests creating of a group without a selection criteria. {@link StoreOperationException} is
     * expected
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateGroupWithoutSelectionCriteria() throws Exception {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .clearStaticGroupMembers()
                .build();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT));
        groupStore.createGroup(OID1, createUserOrigin(), groupDefinition, Collections.emptySet(),
                false);
    }

    /**
     * Tests creating of an empty static group. While it is pretty strange a thing, this test
     * ensures that the DAO will restore the original {@link GroupDefinition} if it contains
     * empty list of static members.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateStaticEmptyGroup() throws Exception {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .clearStaticGroupMembers()
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER)))
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(445))))
                .build();
        final Set<MemberType> memberTypes = ImmutableSet.of(
                MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                MemberType.newBuilder().setEntity(445).build());
        groupStore.createGroup(OID1, createUserOrigin(), groupDefinition, memberTypes, false);
        final Grouping group = getGroupFromStore(OID1);
        Assert.assertThat(group.getDefinition(), new GroupDefinitionMatcher(groupDefinition));
    }

    /**
     * Tests removing of discovered group. This operation is prohibited because discovered groups
     * could not be edited manually. {@link StoreOperationException} is expected
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testDeleteDiscoveredGroup() throws Exception {
        final DiscoveredGroup group1 = createUploadedGroup("smth", Arrays.asList(1L, 2L));
        groupStore.updateDiscoveredGroups(Collections.singleton(group1), Collections.emptyList(),
                Collections.emptySet());
        final long groupId = group1.getOid();
        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT));
        groupStore.deleteGroup(groupId);
    }

    /**
     * Tests removing of group that is not present in the DB. {@link StoreExceptionMatcher} is
     * expected
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testDeleteAbsentGroup() throws Exception {
        expectedException.expect(new StoreExceptionMatcher(Status.NOT_FOUND));
        groupStore.deleteGroup(-1);
    }

    /**
     * Tests deletion of a user group. The group is expected to be removed as a result. Another
     * group present in the DB is not expected to be changed
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteUserGroup() throws Exception {
        final Origin origin = createUserOrigin();
        final GroupDefinition groupDefinition1 = createGroupDefinition();
        final GroupDefinition groupDefinition2 = createGroupDefinition();
        groupStore.createGroup(OID1, origin, groupDefinition1, EXPECTED_MEMBERS, false);
        groupStore.createGroup(OID2, origin, groupDefinition2, EXPECTED_MEMBERS, false);
        groupStore.deleteGroup(OID1);
        Assert.assertEquals(Collections.emptySet(),
                new HashSet<>(groupStore.getGroupsById(Collections.singleton(OID1))));
        final GroupDTO.Grouping group = getGroupFromStore(OID2);
        Assert.assertEquals(groupDefinition2, group.getDefinition());
        Assert.assertEquals(origin, group.getOrigin());
    }

    /**
     * Tests how tags are inserted if we have two tags with the same key and value. The DB requires the pair of
     * Group ID, Tag Key and Tag Value to be unique. This test verifies that only one record is inserted.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDuplicateTagsForGroups() throws Exception {
        final Origin origin = createUserOrigin();
        final String tagName1 = "tag1";
        final String tagValue11 = "tag1-1";
        final String tagValue12 = tagValue11;
        final String tagName2 = "tag1 ";

        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tagName1, TagValuesDTO.newBuilder()
                                .addAllValues(Arrays.asList(tagValue11, tagValue12)).build())
                        .putTags(tagName2, TagValuesDTO.newBuilder()
                            .addAllValues(Collections.singletonList(tagValue11)).build())).build();

        Assert.assertEquals(2, groupDefinition.getTags().getTagsCount());
        groupStore.createGroup(OID2, origin, groupDefinition, EXPECTED_MEMBERS, true);
        final Map<Long, Map<String, Set<String>>> actualAllTags =
                groupStore.getTags(Collections.emptyList());
        Assert.assertEquals(1, actualAllTags.size());
        Assert.assertEquals(1, actualAllTags.entrySet().iterator().next().getValue()
                .entrySet().iterator().next().getValue().size());
    }

    /**
     * Test the cases that there are tag with oversized keys or values.
     *
     * @throws StoreOperationException if something goes wrong.
     */
    @Test
    public void testOversizeTagValues() throws StoreOperationException {
        final Origin origin = createUserOrigin();
        final String tagKey1 = StringUtils.repeat("tag", 100);
        final String tagValue1 = "tag1-1";
        final String tagKey2 = "tag2Key";
        final String tagValue2 = "tag1 ";
        final String tagValue3 = StringUtils.repeat("val", 100);

        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
            .setTags(Tags.newBuilder()
                .putTags(tagKey1, TagValuesDTO.newBuilder()
                    .addAllValues(Collections.singletonList(tagValue1)).build())
                .putTags(tagKey2, TagValuesDTO.newBuilder()
                    .addAllValues(Arrays.asList(tagValue2, tagValue3)).build())).build();

        groupStore.createGroup(OID2, origin, groupDefinition, EXPECTED_MEMBERS, true);
        final Map<Long, Map<String, Set<String>>> allTags =
            groupStore.getTags(Collections.emptyList());
        Assert.assertEquals(1, allTags.size());
        Map<String, Set<String>> tagMap = allTags.values().iterator().next();

        Assert.assertEquals(2, tagMap.keySet().size());
        Optional<String> oversizedTagKey = tagMap.keySet().stream()
            .filter(str -> !tagKey2.equals(str))
            .findAny();
        Assert.assertTrue(oversizedTagKey.isPresent());
        Assert.assertEquals(tagKey1.substring(0, 200), oversizedTagKey.get().substring(0, 200));
        Assert.assertEquals(Collections.singleton(tagValue1),
            tagMap.get(oversizedTagKey.get()));
        Assert.assertEquals(2, tagMap.get(tagKey2).size());
        Assert.assertTrue(tagMap.get(tagKey2).contains(tagValue2));
        Optional<String> oversizedTagValue = tagMap.get(tagKey2).stream()
            .filter(str -> !tagValue2.equals(str))
            .findAny();
        Assert.assertTrue(oversizedTagValue.isPresent());
        Assert.assertEquals(tagValue3.substring(0, 200), oversizedTagValue.get().substring(0, 200));
    }

    /**
     * Tests how tags are queried from the database. If requested certain groups retain tags
     * related to these groups. If requested groups are not set then return tags for all existed
     * groups in group component.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTagsForGroups() throws Exception {
        final Origin origin = createUserOrigin();
        final String tagName1 = "tag1";
        final String tagName2 = "tag2";
        final String tagName3 = "tag3";
        final String tagValue11 = "tag1-1";
        final String tagValue12 = "tag1-2";
        final String tagValue2 = "tag2";
        final String tagValue31 = "tag3-1";
        final String tagValue32 = "tag3-2";

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tagName1, TagValuesDTO.newBuilder()
                                .addAllValues(Arrays.asList(tagValue11, tagValue12))
                                .build())
                        .putTags(tagName2, TagValuesDTO.newBuilder().addValues(tagValue2).build()))
                .build();
        final GroupDefinition groupDefinition3 = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tagName3, TagValuesDTO.newBuilder()
                                .addAllValues(Arrays.asList(tagValue31, tagValue32))
                                .build()))
                .build();

        groupStore.createGroup(OID2, origin, groupDefinition2, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID3, origin, groupDefinition3, EXPECTED_MEMBERS, true);
        final Map<Long, Map<String, Set<String>>> actualAllTags =
                groupStore.getTags(Collections.emptyList());
        Assert.assertEquals(2, actualAllTags.size());
        final Map<String, Set<String>> group2Tags = actualAllTags.get(OID2);
        final Set<String> group2TagNames = group2Tags.keySet();
        final Set<String> group2TagValues = new HashSet<>();
        group2Tags.values().forEach(group2TagValues::addAll);
        Assert.assertEquals(Sets.newHashSet(tagName1, tagName2), group2TagNames);
        Assert.assertEquals(Sets.newHashSet(tagValue11, tagValue12, tagValue2), group2TagValues);

        final Map<Long, Map<String, Set<String>>> tagsForSingleGroup =
                groupStore.getTags(Collections.singletonList(OID3));
        Assert.assertEquals(1, tagsForSingleGroup.size());
        final Map<String, Set<String>> group3Tags = tagsForSingleGroup.get(OID3);
        final Set<String> group3TagNames = group3Tags.keySet();
        final Set<String> group3TagValues = new HashSet<>();
        group3Tags.values().forEach(group3TagValues::addAll);
        Assert.assertEquals(Sets.newHashSet(tagName3), group3TagNames);
        Assert.assertEquals(Sets.newHashSet(tagValue31, tagValue32), group3TagValues);
    }

    /**
     * Tests retrieval of static groups associated with an entity.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetStaticMembershipInfo() throws Exception {
        final Origin origin = createUserOrigin();
        final GroupDefinition groupDefinition1 = createGroupDefinition();
        final Collection<Long> group1members = groupDefinition1.getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .filter(memberType -> memberType.getType().hasEntity())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<Long> group2members = new HashSet<>(Arrays.asList(100L, 102L));
        group2members.addAll(group1members);
        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(234))
                                .addAllMembers(group2members)
                                .build()))
                .build();
        groupStore.createGroup(OID1, origin, groupDefinition1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, origin, groupDefinition2,
                Collections.singleton(MemberType.newBuilder().setEntity(234).build()), true);
        final Map<Long, Set<Long>> groups100 =
                groupStore.getStaticGroupsForEntities(Collections.singletonList(100L),
                        Collections.emptyList());
        final Map<Long, Set<Long>> groups1 =
                groupStore.getStaticGroupsForEntities(group1members, Collections.emptyList());
        Assert.assertEquals(Collections.singleton(OID2), groups100.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));
        Assert.assertEquals(new HashSet<>(Arrays.asList(OID1, OID2)), groups1.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));
    }

    /**
     * Tests retrieval of static groups of the specified types associated with an entity.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetStaticMembershipInfoSpecificTypes() throws Exception {
        final Origin origin = createUserOrigin();
        final GroupDefinition groupDefinition1 = createGroupDefinition();
        final Collection<Long> group1members = groupDefinition1.getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .filter(memberType -> memberType.getType().hasEntity())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Set<Long> group2members = new HashSet<>(Arrays.asList(100L, 102L));
        group2members.addAll(group1members);
        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(234))
                                .addAllMembers(group2members)
                                .build()))
                .build();
        groupStore.createGroup(OID1, origin, groupDefinition1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, origin, groupDefinition2,
                Collections.singleton(MemberType.newBuilder().setEntity(234).build()), true);
        final Map<Long, Set<Long>> groupsAll =
                groupStore.getStaticGroupsForEntities(group2members, Collections.emptyList());
        Assert.assertEquals(Sets.newHashSet(OID1, OID2), groupsAll.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));

        final Map<Long, Set<Long>> groupsRegular =
                groupStore.getStaticGroupsForEntities(group2members, EnumSet.of(GroupType.REGULAR));
        Assert.assertEquals(Collections.singleton(OID1), groupsRegular.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));
    }

    /**
     * Tests static members retrieval. All the static members (both direct group members and direct
     * entity members) are expected to arrive as a result of the call.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetMembers() throws Exception {
        final Origin origin = createUserOrigin();
        final GroupDefinition child1 = createGroupDefinition();
        final GroupDefinition child2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .build();
        final EntityFilters entityFilters = EntityFilters.newBuilder()
                .addEntityFilter(EntityFilter.newBuilder()
                        .setEntityType(EntityType.STORAGE_VALUE)
                        .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                                .addSearchParameters(SearchParameters.newBuilder()
                                        .setStartingFilter(PropertyFilter.newBuilder()
                                                .setPropertyName("oid")
                                                .setStringFilter(StringFilter.newBuilder()
                                                        .addOptions("awr232421342"))))))
                .build();
        final GroupDefinition child3 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.STORAGE_CLUSTER)
                .setEntityFilters(entityFilters)
                .build();
        final GroupDefinition child4 = GroupDefinition.newBuilder(createGroupDefinition())
                .setGroupFilters(GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.STORAGE_CLUSTER)
                                .build()))
                .build();
        groupStore.createGroup(OID1, origin, child1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, origin, child2, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID3, origin, child3, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID4, origin, child4, EXPECTED_MEMBERS, true);
        final long oidEntity = 123456L;
        final GroupDefinition container = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setGroup(GroupType.REGULAR))
                                .addMembers(OID1)
                                .build())
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER))
                                .addMembers(OID2)
                                .build())
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setGroup(GroupType.REGULAR))
                                .addMembers(OID4)
                                .build())
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(34))
                                .addMembers(oidEntity)
                                .build()))
                .build();
        final Set<MemberType> expectedMemberTypes =
                ImmutableSet.of(MemberType.newBuilder().setGroup(GroupType.REGULAR).build(),
                        MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                        MemberType.newBuilder().setEntity(34).build());
        groupStore.createGroup(OID5, origin, container, expectedMemberTypes, true);
        final GroupMembersPlain members =
                groupStore.getMembers(Collections.singleton(OID5), true);
        final Set<Long> expectedEntities = new HashSet<>();
        expectedEntities.add(oidEntity);
        expectedEntities.addAll(getEntityMembers(child1));
        expectedEntities.addAll(getEntityMembers(child2));
        expectedEntities.addAll(getEntityMembers(child3));
        expectedEntities.addAll(getEntityMembers(child4));
        Assert.assertEquals(Sets.newHashSet(OID1, OID2, OID3, OID4), members.getGroupIds());
        Assert.assertEquals(expectedEntities, members.getEntityIds());
        Assert.assertEquals(Collections.singleton(entityFilters), members.getEntityFilters());
    }

    /**
     * Test get owners of requested groups.
     *
     * @throws StoreOperationException on exceptions occurred.
     */
    @Test
    public void testGetOwnersOfGroups() throws StoreOperationException {
        final Origin origin = createUserOrigin();
        final Long owner1 = 12L;
        final Long owner2 = 21L;

        final GroupDefinition groupDefinition1 =
                GroupDefinition.newBuilder().setOwner(owner1).setType(GroupType.RESOURCE).setDisplayName("resourceGroup").setStaticGroupMembers(
                        StaticMembers.getDefaultInstance()).build();
        final GroupDefinition groupDefinition2 =
                GroupDefinition.newBuilder().setOwner(owner2).setType(GroupType.REGULAR).setDisplayName("regularGroup").setStaticGroupMembers(
                        StaticMembers.getDefaultInstance()).build();
        groupStore.createGroup(OID1, origin, groupDefinition1,
                Collections.singleton(MemberType.getDefaultInstance()), true);
        groupStore.createGroup(OID2, origin, groupDefinition2,
                Collections.singleton(MemberType.getDefaultInstance()), true);

        Assert.assertEquals(Sets.newHashSet(owner1),
                groupStore.getOwnersOfGroups(Collections.singletonList(OID1), null));

        Assert.assertEquals(Sets.newHashSet(owner1, owner2),
                groupStore.getOwnersOfGroups(Arrays.asList(OID1, OID2), null));

        Assert.assertEquals(Sets.newHashSet(owner1),
                groupStore.getOwnersOfGroups(Arrays.asList(OID1, OID2), GroupType.RESOURCE));

        Assert.assertTrue(groupStore.getOwnersOfGroups(Collections.singletonList(OID3), null).isEmpty());
    }

    /**
     * Test that the expected exception type is thrown when we resolve members of a a nested group
     * with a bad regex.
     *
     * @throws StoreOperationException To satisfy compiler.
     */
    @Test
    public void testGetMembersBadRegexException() throws StoreOperationException {
        final GroupDefinition badGroupDef = GroupDefinition.newBuilder()
                .setDisplayName("badGroup")
                .setGroupFilters(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(SearchProtoUtil.nameFilterRegex("*"))))
                .build();
        groupStore.createGroup(OID1, createUserOrigin(), badGroupDef,
                Collections.singleton(MemberType.getDefaultInstance()), true);

        expectedException.expect(BadSqlGrammarException.class);
        groupStore.getMembers(Collections.singleton(OID1), false);
    }

    /**
     * Test that the expected exception type is thrown when we resolve a group filter.
     */
    @Test
    public void testGetGroupIdsBadRegexException() {
        expectedException.expect(BadSqlGrammarException.class);
        groupStore.getGroupIds(GroupFilters.newBuilder()
            .addGroupFilter(GroupFilter.newBuilder()
                    .addPropertyFilters(SearchProtoUtil.nameFilterRegex("*")))
            .build());
    }

    /**
     * Method tests functionality of existing groups retrieval. It is expected that IDs are
     * returned only for groups already added to the group store.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetExistingGroups() throws Exception {
        Assert.assertEquals(Collections.emptySet(),
                groupStore.getExistingGroupIds(Arrays.asList(OID1, OID2, OID3)));
        final Origin origin = createUserOrigin();
        final Set<MemberType> expectedMemberTypes =
                Collections.singleton(MemberType.newBuilder().setEntity(1).build());
        groupStore.createGroup(OID1, origin, createGroupDefinition(), expectedMemberTypes, true);
        Assert.assertEquals(Collections.singleton(OID1),
                groupStore.getExistingGroupIds(Arrays.asList(OID1, OID2)));
        groupStore.createGroup(OID2, origin, createGroupDefinition(), expectedMemberTypes, true);
        Assert.assertEquals(Collections.singleton(OID2),
                groupStore.getExistingGroupIds(Arrays.asList(OID2, OID3)));
    }

    @Nonnull
    private Set<Long> getEntityMembers(@Nonnull GroupDefinition groupDefinition) {
        return groupDefinition.getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .filter(member -> member.getType().hasEntity())
                .map(StaticMembersByType::getMembersList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private void assertGroupsEqual(@Nonnull DiscoveredGroup expectedDiscGroup,
            @Nonnull GroupDTO.Grouping actual) {
        Assert.assertThat(actual.getDefinition(),
                new GroupDefinitionMatcher(expectedDiscGroup.getDefinition()));
        Assert.assertEquals(expectedDiscGroup.getSourceIdentifier(),
                actual.getOrigin().getDiscovered().getSourceIdentifier());
        Assert.assertEquals(new HashSet<>(expectedDiscGroup.getExpectedMembers()),
                new HashSet<>(actual.getExpectedTypesList()));
        Assert.assertEquals(expectedDiscGroup.isReverseLookupSupported(),
                actual.getSupportsMemberReverseLookup());
    }

    @Nonnull
    private DiscoveredGroup createUploadedGroup(@Nonnull String srcId,
            @Nonnull Collection<Long> targetIds) {
        return groupGenerator.createUploadedGroup(srcId, targetIds);
    }

    @Nonnull
    private GroupDefinition createGroupDefinition() {
        return groupGenerator.createGroupDefinition();
    }

    @Nonnull
    private Origin createUserOrigin() {
        return groupGenerator.createUserOrigin();
    }

    @Nonnull
    private Origin createSystemOrigin() {
        return groupGenerator.createSystemOrigin();
    }

    @Nonnull
    private Grouping getGroupFromStore(long oid) {
        final Optional<Grouping> grouping = groupStore.getGroups(GroupFilter.getDefaultInstance())
                .stream()
                .filter(group -> group.getId() == oid)
                .findFirst();
        return grouping.orElseThrow(() -> new AssertionError("Group not found by id " + oid));
    }

    private GroupDefinition createGroupDefinitionWithInvalidId() {
        return GroupDefinition.newBuilder()
            .setType(GroupType.REGULAR)
            .setDisplayName("GroupName")
            .setGroupFilters(GroupFilters.newBuilder().addGroupFilter(
                GroupFilter.newBuilder()
                    .addAllPropertyFilters(Arrays.asList(PropertyFilter.newBuilder().setPropertyName("oid").setStringFilter(StringFilter.newBuilder()
                        .addAllOptions(Arrays.asList("12345abcd"))
                        .setCaseSensitive(true)
                        .setPositiveMatch(true)
                        .build()).build()))
                    .build())
                .build()).build();
    }

    /**
     * Mockito matcher for {@link StoreOperationException}.
     */
    private static class StoreExceptionMatcher extends BaseMatcher<StoreOperationException> {
        private final Status status;
        private final String message;

        StoreExceptionMatcher(@Nonnull Status status, @Nonnull String message) {
            this.status = Objects.requireNonNull(status);
            this.message = Objects.requireNonNull(message);
        }

        StoreExceptionMatcher(@Nonnull Status status) {
            this.status = Objects.requireNonNull(status);
            this.message = null;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof StoreOperationException)) {
                return false;
            }
            final StoreOperationException exception = (StoreOperationException)item;
            if (!exception.getStatus().equals(status)) {
                return false;
            }
            return message == null || exception.getMessage().contains(message);
        }

        @Override
        public void describeTo(Description description) {

        }
    }

    /**
     * A matcher for group definition. It is used instead of equality operator in order to
     * match orderless collections inside.
     */
    private static class GroupDefinitionMatcher extends ProtobufMessageMatcher<GroupDefinition> {

        GroupDefinitionMatcher(@Nonnull GroupDefinition expected) {
            super(expected, Sets.newHashSet("tags.tags.value.values",
                    "static_group_members.members_by_type"));
        }
    }
}
