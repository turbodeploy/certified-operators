package com.vmturbo.group.group;

import static com.vmturbo.group.GroupMockUtil.mockEnvironment;
import static com.vmturbo.group.db.tables.GroupSupplementaryInfo.GROUP_SUPPLEMENTARY_INFO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.GroupOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagValuesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter.StaticOrDynamicFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.PartialGroupingInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.db.TestGroupDBEndpointConfig;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroupId;
import com.vmturbo.group.group.pagination.GroupPaginationParams;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDB;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test to cover {@link GroupDAO} functionality.
 */
@RunWith(Parameterized.class)
public class GroupDaoTest extends MultiDbTestBase {

    /**
     * Provide test parameter values.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameter values.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public GroupDaoTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(GroupComponent.GROUP_COMPONENT, configurableDbDialect, dialect, "group",
                TestGroupDBEndpointConfig::groupEndpoint);
        this.dsl = super.getDslContext();
    }

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
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        this.groupGenerator = new TestGroupGenerator();
        final SQLDialect dialect = dsl.configuration().family();
        groupStore = new GroupDAO(dsl, new GroupPaginationParams(100, 500), MultiDB.of(dialect));
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
            false, Collections.singleton(100L),
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
        expectedException.expect(StoreOperationException.class);
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
     * Check that dynamic groups can still be queried by direct member type.
     *
     * @throws Exception If exception.
     */
    @Test
    public void testDynamicGroupExpectedTypes() throws Exception {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition())
                // We set these filters to indicate that its a dynamic group.
                .setEntityFilters(EntityFilters.getDefaultInstance())
                .build();
        final MemberType memberType = MemberType.newBuilder()
            .setEntity(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .build();
        groupStore.createGroup(OID1, createUserOrigin(), groupDefinition,
                Collections.singleton(memberType), true);

        GetPaginatedGroupsResponse resp =  groupStore.getPaginatedGroups(GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addDirectMemberTypes(memberType))
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setCursor("0")
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_NAME)
                                .build())
                        .setLimit(100)
                        .setAscending(true)
                        .build())
                .build());

        assertThat(resp.getGroupsList().stream()
                .map(Grouping::getId)
                .collect(Collectors.toList()), containsInAnyOrder(OID1));
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
     * Test getting aggregate tag values.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetAggregateTags() throws Exception {
        // ARRANGE
        final Origin origin = createUserOrigin();
        final String tagName1 = "tag1";
        final String tagName2 = "tag2";
        final String tagValue11 = "tag1-1";
        final String tagValue12 = "tag1-2";
        final String tagValue2 = "tag2";

        final GroupDefinition groupDefinition1 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.RESOURCE)
                .setTags(Tags.newBuilder()
                        .putTags(tagName2, TagValuesDTO.newBuilder().addValues(tagValue2).build()))
                .build();
        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.RESOURCE)
                .setTags(Tags.newBuilder()
                        .putTags(tagName1, TagValuesDTO.newBuilder()
                                .addAllValues(Arrays.asList(tagValue11, tagValue12))
                                .build())
                        .putTags(tagName2, TagValuesDTO.newBuilder().addValues(tagValue2).build()))
                .build();
        final GroupDefinition groupDefinition3 = GroupDefinition.newBuilder(createGroupDefinition())
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setTags(Tags.newBuilder()
                        // Same (tag2, tagValue2) tuple
                        .putTags(tagName2, TagValuesDTO.newBuilder().addValues(tagValue2).build())
                        // Different tag1 tuple - missing one value
                        .putTags(tagName1, TagValuesDTO.newBuilder()
                                .addValues(tagValue11)
                                .build()))
                .build();

        groupStore.createGroup(OID1, origin, groupDefinition1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, origin, groupDefinition2, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID3, origin, groupDefinition3, EXPECTED_MEMBERS, true);

        // ACT/ASSERT - We do several act-assert cycles to avoid cleaning and repopulating the DB.

        // Get all tag values
        Map<String, Set<String>> allTags = groupStore.getTagValues(GetTagValuesRequest.newBuilder().build());
        assertThat(allTags, is(ImmutableMap.of(tagName1, Sets.newHashSet(tagValue11, tagValue12), tagName2, Sets.newHashSet(tagValue2))));

        // Get all "compute host" group tags - should be tag 1 with just 1 value, and tag 2
        Map<String, Set<String>> computeHostTags = groupStore.getTagValues(GetTagValuesRequest.newBuilder()
                .addGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                .build());
        assertThat(computeHostTags, is(ImmutableMap.of(tagName1, Sets.newHashSet(tagValue11), tagName2, Sets.newHashSet(tagValue2))));

        // Get all "OID1" group tags - should just be tag 2.
        Map<String, Set<String>> oid1tags = groupStore.getTagValues(GetTagValuesRequest.newBuilder()
                .addGroupId(OID1)
                .build());
        assertThat(oid1tags, is(ImmutableMap.of(tagName2, Sets.newHashSet(tagValue2))));

        // Get all "OID1 and RESOURCE" group tags - to check that AND works correctly.
        Map<String, Set<String>> oid1AndResourcetags = groupStore.getTagValues(GetTagValuesRequest.newBuilder()
                .addGroupId(OID1)
                .addGroupType(GroupType.RESOURCE)
                .build());
        assertThat(oid1AndResourcetags, is(ImmutableMap.of(tagName2, Sets.newHashSet(tagValue2))));
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
     * Tests how tags are inserted.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testInsertCustomTags() throws StoreOperationException {
        final String tagName1 = "tag1";
        final String tagValue11 = "v1";
        final String tagValue12 = "v2";
        final String tagName2 = "tag2";
        final Long groupID = 42L;

        final Tags tags = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue11, tagValue12)).build())
                .putTags(tagName2, TagValuesDTO.newBuilder()
                        .addAllValues(Collections.singletonList(tagValue11)).build()).build();

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupID, origin, groupDefinition, EXPECTED_MEMBERS, true);

        int result = groupStore.insertTags(groupID, tags);
        Assert.assertEquals(result, 3);
    }

    /**
     * Tests how tags are inserted if a tag already exists.
     *
     * @throws StoreOperationException due to duplicate tag insertion.
     */
    @Test(expected = StoreOperationException.class)
    public void testInsertCustomDuplicateTags() throws StoreOperationException {
        final String tagName1 = "tag";
        final String tagValue1 = "v1";
        final String tagValue2 = "v2";
        final Long groupID = 42L;

        final Tags tags1 = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build()).build();
        final Tags tags2 = Tags.newBuilder().putTags(tagName1, TagValuesDTO.newBuilder()
                .addAllValues(Collections.singletonList(tagValue1)).build()).build();

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupID, origin, groupDefinition, EXPECTED_MEMBERS, true);

        groupStore.insertTags(groupID, tags2);
        // Duplicate tag insertion
        groupStore.insertTags(groupID, tags1);
    }

    /**
     * Tests how tags are deleted.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTag() throws StoreOperationException {
        final String tagName1 = "tag1";
        final String tagValue11 = "v1";
        final String tagValue12 = "v2";
        final String tagName2 = "tag2";
        final Long groupID = 42L;

        final Tags tags = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue11, tagValue12)).build())
                .putTags(tagName2, TagValuesDTO.newBuilder()
                        .addAllValues(Collections.singletonList(tagValue11)).build()).build();

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupID, origin, groupDefinition, EXPECTED_MEMBERS, true);

        int result = groupStore.insertTags(groupID, tags);
        Assert.assertEquals(result, 3);

        int affectedRows = groupStore.deleteTag(groupID, tagName1);
        assertThat(affectedRows, is(2));
        Map<String, Set<String>> tagsMap = groupStore.getTags(Arrays.asList(groupID)).get(groupID);
        // We expect two, one discovered, and one we created (tagName2)
        Assert.assertThat(tagsMap.size(), is(2));
        Assert.assertThat(tagsMap.get(tagName1), is(Matchers.nullValue()));
        Assert.assertThat(tagsMap.get(tagName2), is(Matchers.notNullValue()));
    }

    /**
     * Tests how tags are deleted if tag does not exist.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTagNotExist() throws StoreOperationException {
        final Long groupID = 42L;
        final String notExistTag = "randomTag";

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupID, origin, groupDefinition, EXPECTED_MEMBERS, true);

        int affectedRows = groupStore.deleteTag(groupID, notExistTag);
        assertThat(affectedRows, is(0));
    }

    /**
     * Tests how tags are deleted.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTags() throws StoreOperationException {
        final String tagName1 = "tag1";
        final String tagValue11 = "v1";
        final String tagValue12 = "v2";
        final String tagName2 = "tag2";
        final Long groupID = 42L;

        final Tags tags = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue11, tagValue12)).build())
                .putTags(tagName2, TagValuesDTO.newBuilder()
                        .addValues(tagValue11).build()).build();

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupID, origin, groupDefinition, EXPECTED_MEMBERS, true);

        int result = groupStore.insertTags(groupID, tags);
        Assert.assertEquals(result, 3);

        groupStore.deleteTags(groupID);
        Map<String, Set<String>> tagsMap = groupStore.getTags(Arrays.asList(groupID)).get(groupID);
        // Tags map size is 1, because there exists a discovered one, on that group ID
        Assert.assertThat(tagsMap.size(), is(1));
        // Check whether is one of the ones we inserted
        Assert.assertThat(tagsMap.get(tagName1), is(Matchers.nullValue()));
        Assert.assertThat(tagsMap.get(tagName2), is(Matchers.nullValue()));
    }

    /**
     * Test the case of deleting a user defined tag list for a group.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void deleteTagListTest() throws StoreOperationException {
        final String notDeleted = "notDeleted";
        final String tagName1 = "tag1";
        final String tagName2 = "tag2";
        final String tagValue1 = "v1";
        final String tagValue2 = "v2";
        final long groupId = 42L;

        final Tags tags = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build())
                .putTags(tagName2, TagValuesDTO.newBuilder()
                        .addValues(tagValue1).build())
                .putTags(notDeleted, TagValuesDTO.newBuilder()
                        .addValues(tagValue1).build())
                .build();

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupId, origin, groupDefinition, EXPECTED_MEMBERS, true);

        int num = groupStore.insertTags(groupId, tags);
        assertThat(num, is(4));

        int affectedRows = groupStore.deleteTagList(
                groupId,
                Arrays.asList(tagName1, tagName2)
        );
        assertThat(affectedRows, is(3));


        Map<String, Set<String>> tagsMap = groupStore.getTags(Arrays.asList(groupId)).get(groupId);

    assertThat(tagsMap, is(notNullValue()));
    // "tag" is inherited from group definition, as a discovered tag, thus we expect size 2
    assertThat(tagsMap.size(), is(2));

    Set<String> values = tagsMap.get(notDeleted);
    assertThat(values, is(notNullValue()));
    assertThat(values.size(), is(1));
    assertThat(values.toArray()[0], is(tagValue1));
}

    /**
     * Test the case of deleting a tag list for an entity that does not exist.
     *
     * @throws StoreOperationException due to deleting a tag that does not exist.
     */
    @Test(expected = StoreOperationException.class)
    public void deleteTagListNotExistTest() throws StoreOperationException {
        final long groupId = 42L;
        final String tagName = "someNonExistingTag";

        // create group to group by id
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder(createGroupDefinition()).build();
        final Origin origin = createUserOrigin();
        groupStore.createGroup(groupId, origin, groupDefinition, EXPECTED_MEMBERS, true);

        groupStore.deleteTagList(
                groupId,
                Arrays.asList(tagName)
        );
    }

    /**
     * Tests how groups are filtered in queries that contain "non-equals" tag filters.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetGroupIdsFilteredByTagsNotEqualCase() throws StoreOperationException {
        // GIVEN
        final Origin origin = createUserOrigin();
        final String tagName1 = "tag1";
        final String tagName2 = "tag2";
        final String tagName3 = "tag3";
        final String tagValue11 = "tag1-1";
        final String tagValue12 = "tag1-2";
        final String tagValue2 = "tag2";
        final String tagValue31 = "tag3-1";
        final String tagValue32 = "tag3-2";
        final Map<String, Collection<String>> tagNameToTagValues = ImmutableMap.of(
                tagName1, ImmutableSet.of(tagValue11, tagValue12),
                tagName2, Collections.singleton(tagValue2),
                tagName3, ImmutableSet.of(tagValue31, tagValue32));
        final GroupDefinition groupDefinition1 = createGroupDefinitionWithTags(
                ImmutableSet.of(tagName1, tagName2), tagNameToTagValues);
        final GroupDefinition groupDefinition2 =
                createGroupDefinitionWithTags(Collections.singleton(tagName3), tagNameToTagValues);
        groupStore.createGroup(OID1, origin, groupDefinition1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, origin, groupDefinition2, EXPECTED_MEMBERS, true);

        // WHEN (regex case)
        Collection<Long> resultGroupIds = getGroupIdsByMapPropertyFilterNotEquals(
                mpf -> mpf.setRegex(tagName1 + "=" + tagValue11));
        // THEN
        Assert.assertEquals(1, resultGroupIds.size());
        Assert.assertEquals(OID2, resultGroupIds.iterator().next().longValue());

        // WHEN (exact case)
        resultGroupIds = getGroupIdsByMapPropertyFilterNotEquals(
                mpf -> mpf.setKey(tagName1).addValues(tagValue11));
        // THEN
        Assert.assertEquals(1, resultGroupIds.size());
        Assert.assertEquals(OID2, resultGroupIds.iterator().next().longValue());
    }

    @Nonnull
    private GroupDefinition createGroupDefinitionWithTags(Collection<String> tagNames,
            Map<String, Collection<String>> tagNameToTagValues) {
        final Tags.Builder tagsBuilder = Tags.newBuilder();
        tagNames.forEach(tagName -> tagsBuilder.putTags(tagName, TagValuesDTO.newBuilder()
                .addAllValues(tagNameToTagValues.getOrDefault(tagName, Collections.emptySet()))
                .build()));
        return GroupDefinition.newBuilder(createGroupDefinition()).setTags(tagsBuilder.build())
                .build();
    }

    @Nonnull
    private Collection<Long> getGroupIdsByMapPropertyFilterNotEquals(
            Function<MapFilter.Builder, MapFilter.Builder> mapFilterPopulator) {
        final MapFilter mapFilter =
                mapFilterPopulator.apply(MapFilter.newBuilder().setPositiveMatch(false)).build();
        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
                .setPropertyName(StringConstants.TAGS_ATTR)
                .setMapFilter(mapFilter)
                .build();
        return groupStore.getGroupIds(GroupFilters.newBuilder()
                .addGroupFilter(GroupFilter.newBuilder().addPropertyFilters(propertyFilter).build())
                .build());
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
        groupStore.createGroup(OID1, createUserOrigin(), badGroupDef, Collections.singleton(MemberType.getDefaultInstance()), true);
        try {
            groupStore.getMembers(Collections.singleton(OID1), false);
            // first exception is thrown by MariaDB, second one by Postgres
        } catch (BadSqlGrammarException | DataIntegrityViolationException e) {
            return;
        }
        Assert.fail();
    }

    /**
     * Test that the expected exception type is thrown when we resolve a group filter.
     */
    @Test
    public void testGetGroupIdsBadRegexException() {
        try {
            groupStore.getGroupIds(GroupFilters.newBuilder()
                    .addGroupFilter(GroupFilter.newBuilder().addPropertyFilters(SearchProtoUtil.nameFilterRegex("*")))
                    .build());
            //first exception is thrown by MariaDB, second one by Postgres
        } catch (BadSqlGrammarException | DataIntegrityViolationException e) {
        return;
    }
        Assert.fail();
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

    /**
     * Tests that {@link GroupDAO#getGroupType} returns the group's type.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetGroupType() throws StoreOperationException {
        //GIVEN
        prepareGroups("group1", "group2", "group3", "group4");

        //WHEN
        GroupType groupType = groupStore.getGroupType(OID1);

        //THEN
        Assert.assertEquals(GroupType.COMPUTE_HOST_CLUSTER, groupType);
    }

    /**
     * Tests that {@link GroupDAO#getGroupType} returns null when we're passing a uuid that
     * doesn't correspond to an existing group.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetGroupTypeForNonExistentGroup() throws StoreOperationException {
        //GIVEN
        prepareGroups("group1", "group2", "group3", "group4");

        //WHEN
        GroupType groupType = groupStore.getGroupType(100006L);

        //THEN
        Assert.assertNull(groupType);
    }

    /**
     * Utility function to setup some groups for testing.
     * Used in tests for {@link GroupDAO#getPaginatedGroups}.
     * Following groups are created:
     * - group 1: oid = OID1, static, type = pm cluster, contains 1 host, envType = ONPREM,
     *          cloudType = UNKNOWN, severity = MAJOR
     * - group 2: oid = OID2, static, type = pm cluster, contains 1 host, envType = ONPREM,
     *          cloudType = UNKNOWN, severity = CRITICAL
     * - group 3: oid = OID3, dynamic, type = regular vm group, empty, envType = CLOUD,
     *          cloudType = AWS, severity = NORMAL
     * - group 4: oid = OID4, static, type = pm cluster, empty, envType = ONPREM,
     *          cloudType = UNKNOWN, severity = NORMAL
     *
     * @param group1DisplayName 1st group's display name
     * @param group2DisplayName 2nd group's display name
     * @param group3DisplayName 3rd group's display name
     * @param group4DisplayName 4th group's display name
     * @throws StoreOperationException on db error
     */
    private void prepareGroups(final String group1DisplayName,
            final String group2DisplayName,
            final String group3DisplayName,
            final String group4DisplayName) throws StoreOperationException {
        final long entityOid1 = 1L;
        final long entityOid2 = 2L;
        final long entityOid3 = 3L;
        final long entityOid4 = 4L;
        final Origin origin = createUserOrigin();
        final GroupDefinition groupDefinition1 = GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName(group1DisplayName)
                .setIsHidden(false)
                .setIsTemporary(false)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                        .build())
                                .addMembers(entityOid1)
                                .build())
                        .build())
                .build();
        final GroupDefinition groupDefinition2 = GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName(group2DisplayName)
                .setIsHidden(false)
                .setIsTemporary(false)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                        .build())
                                .addMembers(entityOid2)
                                .build())
                        .build())
                .build();
        final GroupDefinition groupDefinition3 = GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName(group3DisplayName)
                .setIsHidden(false)
                .setIsTemporary(false)
                .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setLogicalOperator(LogicalOperator.AND)
                                .setSearchParametersCollection(
                                        SearchParametersCollection.newBuilder()
                                                .addSearchParameters(SearchParameters.newBuilder()
                                                        .addSearchFilter(SearchFilter.newBuilder()
                                                                .setPropertyFilter(
                                                                        PropertyFilter.newBuilder()
                                                                                .setPropertyName("displayName")
                                                                                .setStringFilter(
                                                                                        StringFilter.newBuilder()
                                                                                                .setStringPropertyRegex("^VM.*$")
                                                                                                .setPositiveMatch(true)
                                                                                                .setCaseSensitive(false)
                                                                                                .build())
                                                                                .build())
                                                                .build())
                                                        .build())
                                                .build())
                                .build())
                        .build())
                .build();
        final GroupDefinition groupDefinition4 = GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName(group4DisplayName)
                .setIsHidden(false)
                .setIsTemporary(false)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                        .build())
                                .build())
                        .build())
                .build();
        createCustomGroup(OID1, origin, groupDefinition1,
                ImmutableSet.of(
                        MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE_VALUE).build()),
                false, mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.MAJOR);
        createCustomGroup(OID2, origin, groupDefinition2,
                ImmutableSet.of(
                        MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE_VALUE).build()),
                false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.CRITICAL);
        createCustomGroup(OID3, origin, groupDefinition3,
                ImmutableSet.of(
                        MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build()),
                true,
                mockEnvironment(EnvironmentType.CLOUD, CloudType.AWS),
                Severity.NORMAL);
        createCustomGroup(OID4, origin, groupDefinition4,
                ImmutableSet.of(
                        MemberType.newBuilder().setEntity(EntityType.PHYSICAL_MACHINE_VALUE).build()),
                true,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
    }

    private void createCustomGroup(long oid,
            Origin origin,
            GroupDefinition definition,
            Set<MemberType> expectedMemberTypes,
            boolean isEmpty,
            GroupEnvironment environment,
            Severity severity) throws StoreOperationException {
        groupStore.createGroup(oid,
                origin,
                definition,
                expectedMemberTypes,
                false);
        groupStore.createGroupSupplementaryInfo(oid, isEmpty, environment, severity);
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses for a
     * basic request (no filtering/sorting specified).
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsForDefaultRequest() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "testGroupA";
        final String group3DisplayName = "testGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);

        // expected return order (ordering defaults to name): group2 group4 group1 group3
        // 1st call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setCursor("0")
                        .setAscending(true)
                        .setLimit(1)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertTrue(response.getPaginationResponse().hasNextCursor());
        String cursor = response.getPaginationResponse().getNextCursor();
        Assert.assertEquals("1", cursor);
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName, response.getGroups(0).getDefinition().getDisplayName());

        // 2nd call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(1)
                        .setCursor(cursor)
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertTrue(response.getPaginationResponse().hasNextCursor());
        cursor = response.getPaginationResponse().getNextCursor();
        Assert.assertEquals("2", cursor);
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group4DisplayName, response.getGroups(0).getDefinition().getDisplayName());

        // 3rd call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(1)
                        .setCursor(cursor)
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertTrue(response.getPaginationResponse().hasNextCursor());
        cursor = response.getPaginationResponse().getNextCursor();
        Assert.assertEquals("3", cursor);
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName, response.getGroups(0).getDefinition().getDisplayName());

        // 4th (final) call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(1)
                        .setCursor(cursor)
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group3DisplayName, response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * ordering by name.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsOrderByName() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "testGroupA";
        final String group3DisplayName = "testGroupD";
        final String group4DisplayName = "TestGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);

        // expected return order (we're sorting by name, order is case insensitive):
        // group2 group4 group1 group3
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(OrderBy.GroupOrderBy.GROUP_NAME)
                                .build())
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertEquals("2", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(OrderBy.GroupOrderBy.GROUP_NAME)
                                .build())
                        .setLimit(2)
                        .setCursor("2")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by case insensitive name.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByNameCaseInsensitive() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        PropertyFilter propertyFilter = PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setCaseSensitive(false)
                        .setStringPropertyRegex("^test.*$")
                        .build())
                .build();

        // expected return order (we're filtering by name (case insensitive), sort by name as
        // default):
        // group4 group1 group3
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(propertyFilter)
                        .build())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertEquals("2", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(propertyFilter)
                        .build())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(2)
                        .setCursor("2")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by case sensitive name.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByNameCaseSensitive() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        PropertyFilter propertyFilter = PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setCaseSensitive(true)
                        .setStringPropertyRegex("^test.*$")
                        .build())
                .build();

        // expected return order (we're filtering by name (case sensitive), sort by name as
        // default):
        // group4 group1
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(propertyFilter)
                        .build())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(1)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertEquals("1", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(propertyFilter)
                        .build())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(2)
                        .setCursor("1")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} does not return a cursor when there are no
     * more results to be returned.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsWithoutNextPage() throws StoreOperationException {
        final String group1DisplayName = "testGroup1";
        final String group2DisplayName = "testGroup2";
        final String group3DisplayName = "testGroup3";
        final String group4DisplayName = "testGroup4";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);

        // request with limit = total records count
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(4)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(4, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName, response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group2DisplayName, response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName, response.getGroups(2).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName, response.getGroups(3).getDefinition().getDisplayName());

        // GIVEN
        // request with limit > total records count
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(5)
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(4, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName, response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group2DisplayName, response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName, response.getGroups(2).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName, response.getGroups(3).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering for static groups.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByStatic() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setStaticOrDynamic(StaticOrDynamicFilter.STATIC)
                .build();

        // expected return order (we're filtering by static, sort by name as default):
        // group2 group4 group1
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertEquals("2", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(2)
                        .setCursor("2")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering for dynamic groups.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByDynamic() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setStaticOrDynamic(StaticOrDynamicFilter.DYNAMIC)
                .build();

        // expected return order (we're filtering by dynamic, sort by name as default):
        // group3
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(1, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by groups' direct members.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByDirectMembers() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .addDirectMemberTypes(MemberType.newBuilder()
                        .setEntity(EntityType.PHYSICAL_MACHINE.getNumber())
                        .build())
                .build();

        // expected return order (we're filtering by PM direct member type, sort by name by
        // default):
        // group2 group4 group1
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertEquals("2", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(2)
                        .setCursor("2")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(3, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct response when
     * filtering by multiple groups' direct members.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetGroupsFilterByMultipleDirectMembers() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .addDirectMemberTypes(MemberType.newBuilder()
                        .setEntity(EntityType.PHYSICAL_MACHINE.getNumber())
                        .build())
                .addDirectMemberTypes(MemberType.newBuilder()
                        .setEntity(EntityType.VIRTUAL_MACHINE.getNumber())
                        .build())
                .build();

        // expected to return all 4 groups: 3 with PMs and 1 with VMs
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(10)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(4, response.getGroupsCount());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by groups' indirect members.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByIndirectMembers() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .addIndirectMemberTypes(MemberType.newBuilder()
                        .setEntity(EntityType.VIRTUAL_MACHINE.getNumber())
                        .build())
                .build();

        // (we're filtering by VM indirect member type, sort by name by default)
        // expected return order:
        // group3
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(1, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering out empty groups.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterOutNonEmpty() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setExcludeEmpty(true)
                .build();

        // expected return order (we're filtering out the empty and sort by name by default):
        // group2 group1
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(1)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertEquals("1", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(1)
                        .setCursor("1")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Utility method used by pagination tests related to environment/cloud type.
     * Creates a regular static vm group with 3 entities, HYBRID environment & cloud type and MINOR
     * severity.
     *
     * @param oid the oid for the new group.
     * @param displayName the display name for the new group.
     * @throws StoreOperationException on db error
     */
    private void createVMGroupWithHybridEnvAndCloudTypes(long oid, String displayName)
            throws StoreOperationException {
        final GroupDefinition groupDefinition = GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName(displayName)
                .setIsHidden(false)
                .setIsTemporary(false)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build())
                                .addMembers(20001L)
                                .addMembers(20002L)
                                .addMembers(20003L)
                                .build())
                        .build())
                .build();
        createCustomGroup(oid, createUserOrigin(), groupDefinition,
                ImmutableSet.of(
                        MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build()),
                false,
                mockEnvironment(EnvironmentType.HYBRID, CloudType.HYBRID_CLOUD),
                Severity.MINOR);
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by group environment type (non HYBRID cases; should return HYBRID groups as well).
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByEnvironmentType() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        final String group5DisplayName = "testGroupA";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        createVMGroupWithHybridEnvAndCloudTypes(OID5, group5DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();

        // expected return order (we're filtering by environment type ONPREM, sort by name by
        // default, HYBRID groups are expected to be returned as well):
        // group2 group5 group4 group1
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(3)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(3, response.getGroupsCount());
        Assert.assertEquals("3", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group5DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(2).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(3)
                        .setCursor("3")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(1, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by group environment type HYBRID (special case, should return everything).
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByEnvironmentTypeHybrid()
            throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        final String group5DisplayName = "testGroupA";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        createVMGroupWithHybridEnvAndCloudTypes(OID5, group5DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setEnvironmentType(EnvironmentType.HYBRID)
                .build();

        // expected return order (we're filtering by environment type HYBRID so all groups are
        // expected to be returned, sort by name by default):
        // group2 group5 group4 group1 group3
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(3)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(3, response.getGroupsCount());
        Assert.assertEquals("3", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(5, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group5DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(2).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(3)
                        .setCursor("3")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(5, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by group cloud type (non HYBRID cases; should return HYBRID groups as well).
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByCloudType() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        final String group5DisplayName = "testGroupA";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        createVMGroupWithHybridEnvAndCloudTypes(OID5, group5DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setCloudType(CloudType.AWS)
                .build();

        // (we're filtering by group cloud type, sort by name by default, HYBRID_CLOUD groups are
        // expected to be returned as well)
        // expected return order:
        // group 5 group3
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(3)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group5DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by group cloud type (non HYBRID cases; should return HYBRID groups as well).
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterByCloudTypeHybrid() throws StoreOperationException {
        final String group1DisplayName = "testGroupC";
        final String group2DisplayName = "myGroupA";
        final String group3DisplayName = "TestGroupD";
        final String group4DisplayName = "testGroupB";
        final String group5DisplayName = "testGroupA";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        createVMGroupWithHybridEnvAndCloudTypes(OID5, group5DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setCloudType(CloudType.HYBRID_CLOUD)
                .build();

        // (we're filtering by group cloud type HYBRID_CLOUD so all groups are expected to be
        // returned, sort by name by default)
        // expected return order:
        // group2 group5 group4 group1 group3
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(3)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(3, response.getGroupsCount());
        Assert.assertEquals("3", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(5, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group5DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(2).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setLimit(3)
                        .setCursor("3")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(5, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * filtering by group severity.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsFilterBySeverity() throws StoreOperationException {
        final String group1DisplayName = "testGroupA";
        final String group2DisplayName = "testGroupB";
        final String group3DisplayName = "testGroupD";
        final String group4DisplayName = "testGroupC";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        GroupFilter groupFilter = GroupFilter.newBuilder()
                .setSeverity(Severity.NORMAL)
                .build();

        // (we're filtering by group severity, sort by name by default)
        // expected return order:
        // group4 group3
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(groupFilter)
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(true)
                        .setCursor("0")
                        .setLimit(3)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(2, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getPaginatedGroups} returns correct paginated responses when
     * ordering by severity.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testGetPaginatedGroupsOrderBySeverity() throws StoreOperationException {
        final String group1DisplayName = "testGroupA";
        final String group2DisplayName = "testGroupB";
        final String group3DisplayName = "testGroupC";
        final String group4DisplayName = "TestGroupD";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);

        // expected return order (we're sorting by severity, having emptiness as secondary sorting
        // and id as last sorting for duplicate entries, and descending) :
        // group2 group1 group4 group3
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(false)
                        .setCursor("0")
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_SEVERITY)
                                .build())
                        .setLimit(2)
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertEquals("2", response.getPaginationResponse().getNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());

        // next call
        request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(false)
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_SEVERITY)
                                .build())
                        .setLimit(2)
                        .setCursor("2")
                        .build())
                .build();

        // WHEN
        response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(2, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#getMinimalGroupInfoByIds} returns correct number of
     * Groups and in the form of MinimalGroupingInfos.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testgetMinimalGroupInfoByIds() throws StoreOperationException {
        final String group1DisplayName = "testGroupA";
        final String group2DisplayName = "testGroupB";
        final String group3DisplayName = "testGroupC";
        final String group4DisplayName = "TestGroupD";

        // ARRANGE
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);
        List<Long> groupIds = Arrays.asList(OID1, OID2, OID3, OID4);

        // ACT
        Collection<PartialGroupingInfo> response = groupStore.getMinimalGroupInfoByIds(groupIds);

        // ASSERT
        Assert.assertEquals(4, response.size());

        for (GroupDTO.PartialGroupingInfo partialGroupingInfo : response) {
            Assert.assertTrue(partialGroupingInfo.hasMinimal());
            Assert.assertTrue(partialGroupingInfo.getMinimal().hasDisplayName());
            Assert.assertTrue(partialGroupingInfo.getMinimal().hasOid());
            Assert.assertTrue(partialGroupingInfo.getMinimal().hasType());
        }

        Assert.assertEquals(1, response.stream()
                .filter(x -> x.getMinimal().getOid() == OID1
                        && x.getMinimal().getType().getNumber() == 3 && x.getMinimal()
                        .getDisplayName()
                        .equals(group1DisplayName))
                .collect(Collectors.toList())
                .size());

        Assert.assertEquals(1, response.stream()
                .filter(x -> x.getMinimal().getOid() == OID2
                        && x.getMinimal().getType().getNumber() == 3 && x.getMinimal()
                        .getDisplayName()
                        .equals(group2DisplayName))
                .collect(Collectors.toList())
                .size());

        Assert.assertEquals(1, response.stream()
                .filter(x -> x.getMinimal().getOid() == OID3
                        && x.getMinimal().getType().getNumber() == 0 && x.getMinimal()
                        .getDisplayName()
                        .equals(group3DisplayName))
                .collect(Collectors.toList())
                .size());

        Assert.assertEquals(1, response.stream()
                .filter(x -> x.getMinimal().getOid() == OID4
                        && x.getMinimal().getType().getNumber() == 3 && x.getMinimal()
                        .getDisplayName()
                        .equals(group4DisplayName))
                .collect(Collectors.toList())
                .size());
    }

    /**
     * Tests that results of {@link GroupDAO#getPaginatedGroups} are ordered secondarily by
     * emptiness.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testSecondaryOrderByEmptiness() throws StoreOperationException {
        final String group1DisplayName = "testGroupA";
        final String group2DisplayName = "testGroupB";
        final String group3DisplayName = "testGroupC";
        final String group4DisplayName = "testGroupD";
        // GIVEN
        prepareGroups(group1DisplayName, group2DisplayName, group3DisplayName, group4DisplayName);

        // expected return order (we're sorting by severity, having emptiness as secondary sorting
        // (empty first on ascending and last on descending), and id as last sorting for duplicate
        // entries, and descending) :
        // group2 group1 group4 group3
        // first call:
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.getDefaultInstance())
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setAscending(false)
                        .setCursor("0")
                        .setLimit(10)
                        .setOrderBy(OrderBy.newBuilder()
                                .setGroupSearch(GroupOrderBy.GROUP_SEVERITY)
                                .build())
                        .build())
                .build();

        // WHEN
        GetPaginatedGroupsResponse response = groupStore.getPaginatedGroups(request);

        // THEN
        Assert.assertEquals(4, response.getGroupsCount());
        Assert.assertFalse(response.getPaginationResponse().hasNextCursor());
        Assert.assertEquals(4, response.getPaginationResponse().getTotalRecordCount());
        Assert.assertEquals(group2DisplayName,
                response.getGroups(0).getDefinition().getDisplayName());
        Assert.assertEquals(group1DisplayName,
                response.getGroups(1).getDefinition().getDisplayName());
        Assert.assertEquals(group4DisplayName,
                response.getGroups(2).getDefinition().getDisplayName());
        Assert.assertEquals(group3DisplayName,
                response.getGroups(3).getDefinition().getDisplayName());
    }

    /**
     * Tests that {@link GroupDAO#createGroupSupplementaryInfo} creates a record in the database
     * with the correct values.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testCreateGroupSupplementaryInfo() throws StoreOperationException {
        // GIVEN
        long groupOid = 1L;
        groupStore.createGroup(groupOid,
                createUserOrigin(),
                GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setDisplayName("1")
                        .setIsHidden(false)
                        .setIsTemporary(false)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                                .build())
                                        .build())
                                .build())
                        .build(),
                ImmutableSet.of(MemberType.newBuilder()
                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build()),
                false);
        // WHEN
        groupStore.createGroupSupplementaryInfo(groupOid,
                false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        // THEN
        Assert.assertEquals(Integer.valueOf(1), dsl.selectCount()
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetchOne().value1());
        Assert.assertEquals(Long.valueOf(groupOid), dsl
                .select(GROUP_SUPPLEMENTARY_INFO.GROUP_ID)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1());
        Assert.assertEquals(EnvironmentType.ON_PREM, EnvironmentType.forNumber(
                dsl.select(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE)
                        .from(GROUP_SUPPLEMENTARY_INFO)
                        .fetch().get(0).value1()));
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, CloudType.forNumber(
                dsl.select(GROUP_SUPPLEMENTARY_INFO.CLOUD_TYPE)
                        .from(GROUP_SUPPLEMENTARY_INFO)
                        .fetch().get(0).value1()));
        Assert.assertEquals(false, dsl.select(GROUP_SUPPLEMENTARY_INFO.EMPTY)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1());
        Assert.assertEquals(Severity.NORMAL, Severity.forNumber(dsl
                .select(GROUP_SUPPLEMENTARY_INFO.SEVERITY)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1()));
    }

    /**
     * Tests that {@link GroupDAO#updateSingleGroupSupplementaryInfo} correctly updates a record in
     * the database.
     *
     * @throws StoreOperationException on db error
     */
    @Test
    public void testUpdateGroupSupplementaryInfo() throws StoreOperationException {
        // GIVEN
        long groupOid = 1L;
        groupStore.createGroup(groupOid,
                createUserOrigin(),
                GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setDisplayName("1")
                        .setIsHidden(false)
                        .setIsTemporary(false)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                                                .build())
                                        .build())
                                .build())
                        .build(),
                ImmutableSet.of(MemberType.newBuilder()
                        .setEntity(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build()),
                false);
        groupStore.createGroupSupplementaryInfo(groupOid,
                false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.AWS),
                Severity.NORMAL);
        // WHEN
        groupStore.updateSingleGroupSupplementaryInfo(groupOid,
                true,
                mockEnvironment(EnvironmentType.CLOUD, CloudType.AZURE),
                Severity.MAJOR);
        // THEN
        Assert.assertEquals(Integer.valueOf(1), dsl.selectCount()
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetchOne().value1());
        Assert.assertEquals(Long.valueOf(groupOid), dsl
                .select(GROUP_SUPPLEMENTARY_INFO.GROUP_ID)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1());
        Assert.assertEquals(EnvironmentType.CLOUD, EnvironmentType.forNumber(
                dsl.select(GROUP_SUPPLEMENTARY_INFO.ENVIRONMENT_TYPE)
                        .from(GROUP_SUPPLEMENTARY_INFO)
                        .fetch().get(0).value1()));
        Assert.assertEquals(CloudType.AZURE, CloudType.forNumber(
                dsl.select(GROUP_SUPPLEMENTARY_INFO.CLOUD_TYPE)
                        .from(GROUP_SUPPLEMENTARY_INFO)
                        .fetch().get(0).value1()));
        Assert.assertEquals(true, dsl.select(GROUP_SUPPLEMENTARY_INFO.EMPTY)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1());
        Assert.assertEquals(Severity.MAJOR, Severity.forNumber(dsl
                .select(GROUP_SUPPLEMENTARY_INFO.SEVERITY)
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetch().get(0).value1()));
    }

    /**
     * Tests updating group supplementary info in a bulk.
     * Any groups that are not currently in the database (in main `grouping` table) are expected to
     * be skipped.
     *
     * @throws StoreOperationException to satisfy compiler.
     */
    @Test
    public void testUpdateBulkGroupSupplementaryInfo() throws StoreOperationException {
        // GIVEN
        prepareGroups("group1", "group2", "group3", "group4");
        Map<Long, GroupSupplementaryInfo> groupsToUpdate = new HashMap<>();
        // change emptiness and severity
        groupsToUpdate.put(OID1, new GroupSupplementaryInfo(OID1, true,
                EnvironmentType.ON_PREM.getNumber(),
                CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.MINOR.getNumber()));
        // change env & cloud type and severity
        groupsToUpdate.put(OID2, new GroupSupplementaryInfo(OID2, false,
                EnvironmentType.CLOUD.getNumber(),
                CloudType.GCP.getNumber(),
                Severity.MAJOR.getNumber()));
        // leave same
        groupsToUpdate.put(OID3, new GroupSupplementaryInfo(OID3, true,
                EnvironmentType.CLOUD.getNumber(),
                CloudType.AWS.getNumber(),
                Severity.MAJOR.getNumber()));
        // leave same
        groupsToUpdate.put(OID4, new GroupSupplementaryInfo(OID4, true,
                EnvironmentType.ON_PREM.getNumber(),
                CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.CRITICAL.getNumber()));
        // group that doesn't exist in the database (in grouping table)
        groupsToUpdate.put(OID5, new GroupSupplementaryInfo(OID5, true,
                EnvironmentType.ON_PREM.getNumber(),
                CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.MINOR.getNumber()));

        // WHEN
        groupStore.updateBulkGroupSupplementaryInfo(groupsToUpdate);

        // THEN
        Map<Long, GroupSupplementaryInfo> retrievedGroups = dsl.select()
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetchInto(GroupSupplementaryInfo.class)
                .stream()
                .collect(Collectors.toMap(GroupSupplementaryInfo::getGroupId, Function.identity()));
        Assert.assertEquals(4, retrievedGroups.size());
        Assert.assertThat(retrievedGroups.keySet(),
                Matchers.containsInAnyOrder(OID1, OID2, OID3, OID4));
        // (group with oid 5 should not be present)
        // Verify the contents of the retrieved groups
        assertSupplementaryInfoValues(retrievedGroups.get(OID1), OID1, true,
                EnvironmentType.ON_PREM.getNumber(),
                CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.MINOR.getNumber());
        assertSupplementaryInfoValues(retrievedGroups.get(OID2), OID2, false,
                EnvironmentType.CLOUD.getNumber(),
                CloudType.GCP.getNumber(),
                Severity.MAJOR.getNumber());
        assertSupplementaryInfoValues(retrievedGroups.get(OID3), OID3, true,
                EnvironmentType.CLOUD.getNumber(),
                CloudType.AWS.getNumber(),
                Severity.MAJOR.getNumber());
        assertSupplementaryInfoValues(retrievedGroups.get(OID4), OID4, true,
                EnvironmentType.ON_PREM.getNumber(),
                CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.CRITICAL.getNumber());
    }

    private void assertSupplementaryInfoValues(GroupSupplementaryInfo gsi, long groupId,
            boolean empty, int envType, int cloudType, int severity) {
        Assert.assertEquals(groupId, gsi.getGroupId().longValue());
        Assert.assertEquals(empty, gsi.getEmpty());
        Assert.assertEquals(envType, gsi.getEnvironmentType().intValue());
        Assert.assertEquals(cloudType, gsi.getCloudType().intValue());
        Assert.assertEquals(severity, gsi.getSeverity().intValue());
    }

    /**
     * Tests updating group severities in a bulk.
     * Any groups that are not currently in the database (in main `grouping` table) are expected to
     * be skipped.
     *
     * @throws StoreOperationException to satisfy compiler.
     */
    @Test
    public void testUpdateBulkGroupSeverities() throws StoreOperationException {
        // GIVEN
        prepareGroups("group1", "group2", "group3", "group4");
        Collection<GroupSupplementaryInfo> groupsToUpdate = new ArrayList<>();
        // leave same
        final Severity group1NewSeverity = Severity.MAJOR;
        groupsToUpdate.add(new GroupSupplementaryInfo(OID1, false, 0, 0,
                group1NewSeverity.getNumber()));
        // change
        final Severity group2NewSeverity = Severity.NORMAL;
        groupsToUpdate.add(new GroupSupplementaryInfo(OID2, false, 0, 0,
                group2NewSeverity.getNumber()));
        // change
        final Severity group3NewSeverity = Severity.MINOR;
        groupsToUpdate.add(new GroupSupplementaryInfo(OID3, true, 0, 0,
                group3NewSeverity.getNumber()));
        // leave same
        final Severity group4NewSeverity = Severity.NORMAL;
        groupsToUpdate.add(new GroupSupplementaryInfo(OID4, true, 0, 0,
                group4NewSeverity.getNumber()));
        // group that doesn't exist in the database (in grouping table)
        final Severity group5NewSeverity = Severity.MINOR;
        groupsToUpdate.add(new GroupSupplementaryInfo(OID5, true, 0, 0,
                group5NewSeverity.getNumber()));

        // WHEN
        groupStore.updateBulkGroupsSeverity(groupsToUpdate);

        // THEN
        Map<Long, GroupSupplementaryInfo> retrievedGroups = dsl.select()
                .from(GROUP_SUPPLEMENTARY_INFO)
                .fetchInto(GroupSupplementaryInfo.class)
                .stream()
                .collect(Collectors.toMap(GroupSupplementaryInfo::getGroupId, Function.identity()));
        Assert.assertEquals(4, retrievedGroups.size());
        Assert.assertThat(retrievedGroups.keySet(),
                Matchers.containsInAnyOrder(OID1, OID2, OID3, OID4));
        // (group with oid 5 should not be present)
        // Verify the contents of the retrieved groups
        Assert.assertEquals(group1NewSeverity.getNumber(),
                retrievedGroups.get(OID1).getSeverity().intValue());
        Assert.assertEquals(group2NewSeverity.getNumber(),
                retrievedGroups.get(OID2).getSeverity().intValue());
        Assert.assertEquals(group3NewSeverity.getNumber(),
                retrievedGroups.get(OID3).getSeverity().intValue());
        Assert.assertEquals(group4NewSeverity.getNumber(),
                retrievedGroups.get(OID4).getSeverity().intValue());
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
