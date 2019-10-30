package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Unit test to cover {@link GroupDAO} functionality.
 */
public class GroupDaoTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule("group_component");
    private static final Set<MemberType> EXPECTED_MEMBERS =
            ImmutableSet.of(MemberType.newBuilder().setEntity(1).build(),
                    MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                    MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER).build());

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GroupDAO groupStore;

    private PolicyStore policyStore;

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Initialize local variables.
     */
    @Before
    public void setup() {
        dbConfig.clearData(GroupComponent.GROUP_COMPONENT);
        final IdentityProvider identityProvider = new IdentityProvider(0);
        groupStore = new GroupDAO(dbConfig.getDslContext(), identityProvider);
    }

    /**
     * Cleanup resources after the test.
     */
    @After
    public void cleanup() {
        dbConfig.clearData(GroupComponent.GROUP_COMPONENT);
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
        final Origin user = createUserOrigin();
        final GroupDefinition userGroup1Def = createGroupDefinition();
        final GroupDefinition userGroup2Def = createGroupDefinition();
        final long userOid1 =
                groupStore.createGroup(user, userGroup1Def, EXPECTED_MEMBERS, true);
        final long userOid2 =
                groupStore.createGroup(user, userGroup2Def, EXPECTED_MEMBERS, true);
        final String src1 = "discovered-group-1";
        final String src2 = "discovered-group-2";
        final DiscoveredGroup group1 = createUploadedGroup(src1, Arrays.asList(1L, 2L));
        final DiscoveredGroup group2 = createUploadedGroup(src1, Arrays.asList(1L, 2L, 3L));
        final DiscoveredGroup group3 = createUploadedGroup(src2, Arrays.asList(1L, 2L, 3L, 4L));
        final Map<String, Long> createdGroups =
                groupStore.updateDiscoveredGroups(Collections.singleton(group1));
        final String identifyingKey1 = GroupProtoUtil.createIdentifyingKey(GroupType.REGULAR, src1);
        final String identifyingKey2 = GroupProtoUtil.createIdentifyingKey(GroupType.REGULAR, src2);
        Assert.assertEquals(Collections.singleton(identifyingKey1), createdGroups.keySet());
        final GroupDTO.Grouping agroup1 =
                groupStore.getGroup(createdGroups.values().iterator().next()).get();
        assertGroupsEqual(group1, agroup1);
        Assert.assertEquals(Arrays.asList(1L, 2L),
                agroup1.getOrigin().getDiscovered().getDiscoveringTargetIdList());

        final Map<String, Long> createdGroups2 =
                groupStore.updateDiscoveredGroups(Arrays.asList(group2, group3));
        Assert.assertEquals(Sets.newHashSet(identifyingKey1, identifyingKey2), createdGroups2.keySet());
        final GroupDTO.Grouping agroup2 = groupStore.getGroup(createdGroups2.get(identifyingKey1)).get();
        final GroupDTO.Grouping agroup3 = groupStore.getGroup(createdGroups2.get(identifyingKey2)).get();

        assertGroupsEqual(group2, agroup2);
        assertGroupsEqual(group3, agroup3);
        final Grouping userGroup1 = groupStore.getGroup(userOid1).get();
        final Grouping userGroup2 = groupStore.getGroup(userOid2).get();
        Assert.assertEquals(userGroup1Def, userGroup1.getDefinition());
        Assert.assertEquals(userGroup2Def, userGroup2.getDefinition());
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
        final Map<String, Long> createdGroups =
                groupStore.updateDiscoveredGroups(Collections.singleton(group1));
        final long groupId = createdGroups.values().iterator().next();
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
        final long oid1 = groupStore.createGroup(origin, groupDef1, EXPECTED_MEMBERS, false);
        final long oid2 = groupStore.createGroup(origin, groupDef2, EXPECTED_MEMBERS, false);
        expectedException.expect(new StoreExceptionMatcher(Status.ALREADY_EXISTS));
        groupStore.updateGroup(oid1, groupDef3, Collections.emptySet(), false);
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

        final long oid = groupStore.createGroup(origin, groupDefinition, memberTypes, true);
        final GroupDTO.Grouping group1 = groupStore.getGroup(oid).get();
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

        final long oid1 = groupStore.createGroup(origin, groupDefinition, memberTypes, false);
        final GroupDTO.Grouping group1 = groupStore.getGroup(oid1).get();
        Assert.assertEquals(origin, group1.getOrigin());
        Assert.assertEquals(groupDefinition, group1.getDefinition());
        Assert.assertEquals(memberTypes, new HashSet<>(group1.getExpectedTypesList()));
        Assert.assertFalse(group1.getSupportsMemberReverseLookup());

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(
                                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER))
                                .addMembers(oid1)))
                .build();

        final long oid2 = groupStore.createGroup(origin, groupDefinition2, memberTypes, false);
        final GroupDTO.Grouping group2 = groupStore.getGroup(oid2).get();
        Assert.assertEquals(origin, group2.getOrigin());
        Assert.assertEquals(groupDefinition2, group2.getDefinition());
        Assert.assertEquals(memberTypes, new HashSet<>(group2.getExpectedTypesList()));
        Assert.assertFalse(group2.getSupportsMemberReverseLookup());
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
        final long oid1 =
                groupStore.createGroup(origin, groupDefinition, Collections.emptySet(), true);
        final GroupDTO.Grouping group = groupStore.getGroup(oid1).get();
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

        final long oid1 = groupStore.createGroup(origin, groupDefinition, memberTypes, false);

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(
                                        MemberType.newBuilder().setGroup(GroupType.STORAGE_CLUSTER))
                                .addMembers(oid1)))
                .build();

        expectedException.expect(new StoreExceptionMatcher(Status.INVALID_ARGUMENT,
                GroupType.STORAGE_CLUSTER.toString()));
        groupStore.createGroup(origin, groupDefinition2, memberTypes, false);
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
        groupStore.createGroup(origin, groupDefinition, memberTypes, false);
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
        groupStore.createGroup(origin, groupDefinition, Collections.emptySet(), false);
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

        final long oid1 = groupStore.createGroup(origin1, groupDefinition1, memberTypes, false);
        final long oid2 = groupStore.createGroup(origin2, groupDefinition2, memberTypes, false);
        final GroupDTO.Grouping group1 = groupStore.getGroup(oid1).get();
        final GroupDTO.Grouping group2 = groupStore.getGroup(oid2).get();
        Assert.assertEquals(groupDefinition1, group1.getDefinition());
        Assert.assertEquals(groupDefinition2, group2.getDefinition());

        expectedException.expect(new StoreExceptionMatcher(Status.ALREADY_EXISTS));
        groupStore.createGroup(origin1, groupDuplicated, memberTypes, false);
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
        groupStore.createGroup(createUserOrigin(), groupDefinition, Collections.emptySet(), false);
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
        final long oid = groupStore.createGroup(createUserOrigin(), groupDefinition, memberTypes, false);
        final Optional<Grouping> group = groupStore.getGroup(oid);
        Assert.assertEquals(
                new HashSet<>(groupDefinition.getStaticGroupMembers().getMembersByTypeList()),
                new HashSet<>(group.map(Grouping::getDefinition)
                        .map(GroupDefinition::getStaticGroupMembers)
                        .map(StaticMembers::getMembersByTypeList)
                        .orElse(null)));
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
        final Map<String, Long> createdGroups =
                groupStore.updateDiscoveredGroups(Collections.singleton(group1));
        final long groupId = createdGroups.values().iterator().next();
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
        final long oid1 =
                groupStore.createGroup(origin, groupDefinition1, EXPECTED_MEMBERS, false);
        final long oid2 =
                groupStore.createGroup(origin, groupDefinition2, EXPECTED_MEMBERS, false);
        groupStore.deleteGroup(oid1);
        Assert.assertEquals(Optional.empty(), groupStore.getGroup(oid1));
        final GroupDTO.Grouping group = groupStore.getGroup(oid2).get();
        Assert.assertEquals(groupDefinition2, group.getDefinition());
        Assert.assertEquals(origin, group.getOrigin());
    }

    /**
     * Tests how tags are queried from the database. All the tags put on different groups are
     * expected to be returned as one map.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetGroupTags() throws Exception {
        final Origin origin = createUserOrigin();
        final String tag1 = "spell";
        final String tag2 = "potion";
        final String value1 = "against enemies";
        final String value2 = "for friends";
        final String value3 = "just for fun";
        final GroupDefinition groupDefinition1 = createGroupDefinition();

        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tag1, TagValuesDTO.newBuilder().addValues(value1).build())
                        .putTags(tag2, TagValuesDTO.newBuilder().addValues(value2).build()))
                .build();
        final GroupDefinition groupDefinition3 = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tag1, TagValuesDTO.newBuilder().addValues(value2).build())
                        .putTags(tag2, TagValuesDTO.newBuilder().addValues(value2).build())
                        .putTags(tag2, TagValuesDTO.newBuilder().addValues(value3).build()))
                .build();
        groupStore.createGroup(origin, groupDefinition1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(origin, groupDefinition2, EXPECTED_MEMBERS, true);
        groupStore.createGroup(origin, groupDefinition3, EXPECTED_MEMBERS, true);
        final Map<String, Set<String>> actualTags = groupStore.getTags();
        Assert.assertEquals(Sets.newHashSet(value1, value2), actualTags.get(tag1));
        Assert.assertEquals(Sets.newHashSet(value2, value3), actualTags.get(tag2));
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
        final long oid1 =
                groupStore.createGroup(origin, groupDefinition1, EXPECTED_MEMBERS, true);
        final long oid2 = groupStore.createGroup(origin, groupDefinition2,
                Collections.singleton(MemberType.newBuilder().setEntity(234).build()), true);
        final Set<GroupDTO.Grouping> groups100 = groupStore.getStaticGroupsForEntity(100L);
        final Set<GroupDTO.Grouping> groups1 =
                groupStore.getStaticGroupsForEntity(group1members.iterator().next());
        Assert.assertEquals(Collections.singleton(oid2),
                groups100.stream().map(GroupDTO.Grouping::getId).collect(Collectors.toSet()));
        Assert.assertEquals(new HashSet<>(Arrays.asList(oid1, oid2)),
                groups1.stream().map(GroupDTO.Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests static members retrieval. All the static members (both direct group members and direct
     * entity members) are expected to arrive as a result of the call.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetStaticMembers() throws Exception {
        final Origin origin = createUserOrigin();
        final GroupDefinition child1 = createGroupDefinition();
        final GroupDefinition child2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .build();
        final long oid1 = groupStore.createGroup(origin, child1, EXPECTED_MEMBERS, true);
        final long oid2 = groupStore.createGroup(origin, child2, EXPECTED_MEMBERS, true);
        final long oid3 = 123456L;
        final GroupDefinition container = GroupDefinition.newBuilder(createGroupDefinition())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setGroup(GroupType.REGULAR))
                                .addMembers(oid1)
                                .build())
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setGroup(GroupType.COMPUTE_HOST_CLUSTER))
                                .addMembers(oid2)
                                .build())
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(34))
                                .addMembers(oid3)
                                .build()))
                .build();
        final Set<MemberType> expectedMemberTypes =
                ImmutableSet.of(MemberType.newBuilder().setGroup(GroupType.REGULAR).build(),
                        MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER).build(),
                        MemberType.newBuilder().setEntity(34).build());
        final long parentOid =
                groupStore.createGroup(origin, container, expectedMemberTypes, true);
        final Pair<Set<Long>, Set<Long>> members = groupStore.getStaticMembers(parentOid);
        Assert.assertEquals(Pair.create(Collections.singleton(oid3), Sets.newHashSet(oid1, oid2)),
                members);
    }


    /**
     * Tests how the data is restored from diagnostics. All the groups dumped initially should
     * be restored using {@link com.vmturbo.components.common.diagnostics.Diagnosable} interfaces
     * call.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDiags() throws Exception {
        final String src1 = "discovered-group-1";
        final String src2 = "discovered-group-2";
        final DiscoveredGroup group1 = createUploadedGroup(src1, Arrays.asList(1L, 2L));
        final DiscoveredGroup group2 = createUploadedGroup(src2, Arrays.asList(1L, 3L));
        final DiscoveredGroup groupNew = createUploadedGroup(src2, Arrays.asList(1L, 3L));
        groupStore.updateDiscoveredGroups(Arrays.asList(group1, group2));
        final long oid1 = groupStore.createGroup(createUserOrigin(), createGroupDefinition(),
                EXPECTED_MEMBERS, true);
        final long oid2 = groupStore.createGroup(createUserOrigin(), createGroupDefinition(),
                EXPECTED_MEMBERS, true);
        final Collection<GroupDTO.Grouping> allGroups1 =
                groupStore.getGroups(GroupDTO.GroupFilter.newBuilder().build());
        Assert.assertEquals(4, allGroups1.size());
        final List<String> dumpedData = groupStore.collectDiags();

        groupStore.deleteGroup(oid1);
        groupStore.updateDiscoveredGroups(Arrays.asList(groupNew));
        final Collection<GroupDTO.Grouping> allGroups2 =
                groupStore.getGroups(GroupDTO.GroupFilter.newBuilder().build());
        Assert.assertEquals(3, allGroups2.size());

        groupStore.restoreDiags(dumpedData);
        final Collection<GroupDTO.Grouping> restoredGroups =
                groupStore.getGroups(GroupDTO.GroupFilter.newBuilder().build());
        Assert.assertEquals(new HashSet<>(allGroups1), new HashSet<>(restoredGroups));
    }

    private void assertGroupsEqual(@Nonnull DiscoveredGroup expectedDiscGroup,
            @Nonnull GroupDTO.Grouping actual) {
        Assert.assertEquals(expectedDiscGroup.getDefinition(), actual.getDefinition());
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
        final GroupDefinition groupDefinition = createGroupDefinition();
        final List<MemberType> types = Arrays.asList(MemberType.newBuilder().setEntity(1).build(),
                MemberType.newBuilder().setGroup(GroupType.REGULAR).build());
        return new DiscoveredGroup(groupDefinition, srcId, new HashSet<>(targetIds), types, false);
    }

    @Nonnull
    private GroupDefinition createGroupDefinition() {
        return GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("Group-" + counter.getAndIncrement())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(1))
                                .addMembers(counter.getAndIncrement())))
                .setTags(Tags.newBuilder()
                        .putTags("tag", TagValuesDTO.newBuilder()
                                .addValues("tag1")
                                .addValues("tag" + counter.getAndIncrement())
                                .build()))
                .setIsHidden(false)
                .setOptimizationMetadata(OptimizationMetadata.newBuilder()
                        .setIsGlobalScope(false)
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .build())
                .build();
    }

    @Nonnull
    public Origin createUserOrigin() {
        return Origin.newBuilder()
                .setUser(Origin.User.newBuilder().setUsername("user-" + counter.getAndIncrement()))
                .build();
    }

    @Nonnull
    private Origin createSystemOrigin() {
        return Origin.newBuilder()
                .setSystem(Origin.System.newBuilder()
                        .setDescription("system-group-" + counter.getAndIncrement()))
                .build();
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
}
