package com.vmturbo.group.group;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.group.GroupStore.GroupNotClusterException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.policy.PolicyStore.PolicyDeleteException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class GroupStoreTest {

    private static final long GROUP_ID = 7L;

    private static final long TARGET_ID = 7L;

    private static final GroupInfo GROUP_INFO = GroupInfo.newBuilder()
            .setName("the krew")
            .setEntityType(123)
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L))
            .build();

    private static final GroupInfo UPDATED_GROUP_INFO = GROUP_INFO.toBuilder()
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(2L))
            .build();

    private static final ClusterInfo CLUSTER_INFO = ClusterInfo.newBuilder()
            .setName("the kluster")
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(2L))
            .build();

    private static final ClusterInfo UPDATED_CLUSTER_INFO = CLUSTER_INFO.toBuilder()
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(3L))
            .build();

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private GroupStore groupStore;

    private PolicyStore policyStore = mock(PolicyStore.class);

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private EntityToClusterMapping entityToClusterMapping = mock(EntityToClusterMapping.class);

    @Before
    public void setup() {
        final DSLContext dslContext = dbConfig.prepareDatabase();
        groupStore = new GroupStore(dslContext, policyStore,
                identityProvider, entityToClusterMapping);
    }

    @Test
    public void testNewUserGroupThenGet() throws DuplicateNameException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        final Group group = groupStore.newUserGroup(GROUP_INFO);
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(GROUP_INFO));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getType(), is(Type.GROUP));

        final Group gotGroup = groupStore.get(GROUP_ID).get();
        assertThat(gotGroup, is(group));
    }

    public void testNewUserGroupDuplicateNameOnly() throws DuplicateNameException {
        when(identityProvider.next()).thenReturn(GROUP_ID).thenReturn(GROUP_ID + 1);
        groupStore.newUserGroup(GROUP_INFO);

        // Try to create a group with the same name.
        // This shouldn't throw an exception.
        groupStore.newUserGroup(GroupInfo.newBuilder()
            .setName(GROUP_INFO.getName())
            .setEntityType(GROUP_INFO.getEntityType() + 1)
            .build());
    }

    @Test(expected = DuplicateNameException.class)
    public void testNewUserGroupDuplicateNameAndType() throws DuplicateNameException {
        when(identityProvider.next()).thenReturn(GROUP_ID).thenReturn(GROUP_ID + 1);
        groupStore.newUserGroup(GROUP_INFO);

        // Try to create a group with the same name.
        // This shouldn't throw an exception.
        groupStore.newUserGroup(GroupInfo.newBuilder()
                .setName(GROUP_INFO.getName())
                .setEntityType(GROUP_INFO.getEntityType())
                .build());
    }

    @Test
    public void testEditUserGroup() throws DuplicateNameException, ImmutableGroupUpdateException, GroupNotFoundException {
        when(identityProvider.next()).thenReturn(GROUP_ID);

        groupStore.newUserGroup(GROUP_INFO);
        final Group group = groupStore.updateUserGroup(GROUP_ID, UPDATED_GROUP_INFO);

        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(UPDATED_GROUP_INFO));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getType(), is(Type.GROUP));

        final Group gotGroup = groupStore.get(GROUP_ID).get();
        assertThat(gotGroup, is(group));
    }

    @Test(expected = DuplicateNameException.class)
    public void testEditUserGroupDuplicateName() throws DuplicateNameException, ImmutableGroupUpdateException, GroupNotFoundException {
        final GroupInfo otherGroupInfo = GROUP_INFO.toBuilder()
                .setName("bar")
                .build();
        when(identityProvider.next()).thenReturn(GROUP_ID)
            .thenReturn(GROUP_ID + 1)
            .thenReturn(GROUP_ID + 2);

        final Group group = groupStore.newUserGroup(GROUP_INFO);
        final Group otherGroup = groupStore.newUserGroup(otherGroupInfo);

        groupStore.updateUserGroup(group.getId(), group.getGroup().toBuilder()
                .setName(otherGroup.getGroup().getName())
                .build());
    }

    @Test(expected = GroupNotFoundException.class)
    public void testEditUserGroupNameNotFound() throws GroupNotFoundException, DuplicateNameException, ImmutableGroupUpdateException {
        groupStore.updateUserGroup(1L, GroupInfo.getDefaultInstance());
    }

    @Test(expected = ImmutableGroupUpdateException.class)
    public void testEditUserGroupDiscoveredGroup() throws GroupNotFoundException, DuplicateNameException, ImmutableGroupUpdateException {
        when(identityProvider.next()).thenReturn(GROUP_ID);

        // "Discover" a group
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.singletonList(GROUP_INFO), Collections.emptyList());

        // Try to update that group.
        groupStore.updateUserGroup(GROUP_ID, GroupInfo.getDefaultInstance());
    }

    @Test
    public void testUpdateTargetGroupsDiscoveredNewGroup() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        final Map<String, Long> nameToId = groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO), Collections.emptyList());
        assertThat(nameToId.values(), contains(GROUP_ID));

        final Group group = groupStore.get(GROUP_ID).get();
        assertThat(group.getGroup(), is(GROUP_INFO));
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getType(), is(Type.GROUP));
        assertThat(group.getOrigin(), is(Origin.DISCOVERED));
        assertThat(group.getTargetId(), is(TARGET_ID));
    }

    @Test
    public void testUpdateTargetGroupDiscoveredNewCluster() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        final Map<String, Long> nameToId = groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(), Collections.singletonList(CLUSTER_INFO));
        assertThat(nameToId.values(), contains(GROUP_ID));

        final Group group = groupStore.get(GROUP_ID).get();
        assertThat(group.getCluster(), is(CLUSTER_INFO));
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getType(), is(Type.CLUSTER));
        assertThat(group.getOrigin(), is(Origin.DISCOVERED));
        assertThat(group.getTargetId(), is(TARGET_ID));
    }

    @Test
    public void testUpdateTargetGroupsOldRemoved() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO), Collections.emptyList());

        assertTrue(groupStore.get(GROUP_ID).isPresent());
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.emptyList());
        assertFalse(groupStore.get(GROUP_ID).isPresent());
    }

    @Test
    public void testUpdateTargetGroupsOldClusterRemoved() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(), Collections.singletonList(CLUSTER_INFO));

        assertTrue(groupStore.get(GROUP_ID).isPresent());
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.emptyList());
        assertFalse(groupStore.get(GROUP_ID).isPresent());
    }

    @Test
    public void testUpdateTargetGroupUpdatedExistingGroup() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO), Collections.emptyList());

        assertThat(groupStore.get(GROUP_ID).get().getGroup(), is(GROUP_INFO));

        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.singletonList(UPDATED_GROUP_INFO), Collections.emptyList());

        assertThat(groupStore.get(GROUP_ID).get().getGroup(), is(UPDATED_GROUP_INFO));
    }

    @Test
    public void testUpdateTargetClusterUpdatedUpdatedExistingCluster() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(), Collections.singletonList(CLUSTER_INFO));

        assertThat(groupStore.get(GROUP_ID).get().getCluster(), is(CLUSTER_INFO));

        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.singletonList(UPDATED_CLUSTER_INFO));

        assertThat(groupStore.get(GROUP_ID).get().getCluster(), is(UPDATED_CLUSTER_INFO));
    }

    @Test
    public void testUpdateClusterHeadroomTemplate() throws GroupNotClusterException, GroupNotFoundException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(),
                Collections.singletonList(CLUSTER_INFO));

        assertFalse(groupStore.get(GROUP_ID).get().getCluster().hasClusterHeadroomTemplateId());

        final long headroomTemplateId = 100L;
        groupStore.updateClusterHeadroomTemplate(GROUP_ID, headroomTemplateId);

        assertThat(groupStore.get(GROUP_ID).get().getCluster().getClusterHeadroomTemplateId(), is(headroomTemplateId));
    }

    @Test(expected = GroupNotFoundException.class)
    public void testUpdateClusterHeadroomTemplateClusterNotFound() throws GroupNotFoundException, GroupNotClusterException {
        groupStore.updateClusterHeadroomTemplate(GROUP_ID, 1L);
    }

    @Test(expected = GroupNotClusterException.class)
    public void testUpdateClusterHeadroomTemplateGroupNotCluster() throws DuplicateNameException, GroupNotFoundException, GroupNotClusterException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.newUserGroup(GROUP_INFO);
        groupStore.updateClusterHeadroomTemplate(GROUP_ID, 1L);
    }

    @Test
    public void testDeleteGroupAndAssociatedPolicies()
            throws DuplicateNameException, GroupNotFoundException, ImmutableGroupUpdateException,
               PolicyDeleteException, ImmutablePolicyUpdateException, PolicyNotFoundException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.newUserGroup(GROUP_INFO);

        final Group group = groupStore.deleteUserGroup(GROUP_ID);
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(GROUP_INFO));

        assertFalse(groupStore.get(GROUP_ID).isPresent());

        verify(policyStore).deletePoliciesForGroup(any(), eq(GROUP_ID));
    }

    @Test(expected = GroupNotFoundException.class)
    public void testDeleteGroupNotFound() throws GroupNotFoundException, ImmutableGroupUpdateException, PolicyDeleteException {
        groupStore.deleteUserGroup(1L);
    }

    @Test(expected = ImmutableGroupUpdateException.class)
    public void testDeleteDiscoveredGroup() throws GroupNotFoundException, ImmutableGroupUpdateException, PolicyDeleteException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO),
                Collections.emptyList());
        groupStore.deleteUserGroup(GROUP_ID);
    }

    @Test(expected = PolicyDeleteException.class)
    public void testDeleteGroupImmutableRelatedPolicy() throws DuplicateNameException, ImmutablePolicyUpdateException, PolicyNotFoundException, GroupNotFoundException, ImmutableGroupUpdateException, PolicyDeleteException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.newUserGroup(GROUP_INFO);

        doThrow(PolicyDeleteException.class)
            .when(policyStore).deletePoliciesForGroup(any(), eq(GROUP_ID));

        groupStore.deleteUserGroup(GROUP_ID);
    }

    @Test
    public void testDiagsRoundTrip() throws DuplicateNameException, DiagnosticsException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        final Group originalGroup = groupStore.newUserGroup(GROUP_INFO);

        final List<String> diags = groupStore.collectDiags();

        // Drop everything in groups.
        dbConfig.dsl().deleteFrom(Tables.GROUPING).execute();

        final GroupStore newGroupStore =new GroupStore(dbConfig.dsl(), policyStore,
                identityProvider, entityToClusterMapping);
        newGroupStore.restoreDiags(diags);

        final Group group = newGroupStore.get(GROUP_ID).get();
        assertThat(group, is(originalGroup));
    }
}
