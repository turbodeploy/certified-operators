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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import reactor.test.StepVerifier;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.TagsGroupRecord;
import com.vmturbo.group.group.GroupStore.GroupNotClusterException;
import com.vmturbo.group.group.GroupStore.GroupStoreUpdateEvent.GroupChangeType;
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

    private static final String TAG_KEY_1 = "key1";
    private static final String TAG_KEY_2 = "key2";
    private static final String TAG_VALUE_11 = "value11";
    private static final String TAG_VALUE_12 = "value12";
    private static final String TAG_VALUE_2 = "value2";

    private static final Tags SAMPLE_TAGS =
            Tags.newBuilder()
                .putTags(
                        TAG_KEY_1,
                        TagValuesDTO.newBuilder().addValues(TAG_VALUE_11).addValues(TAG_VALUE_12).build())
                .putTags(
                        TAG_KEY_2,
                        TagValuesDTO.newBuilder().addValues(TAG_VALUE_2).build())
                .build();

    private static final GroupInfo GROUP_INFO = GroupInfo.newBuilder()
            .setName("the krew")
            .setEntityType(123)
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(1L))
            .setTags(SAMPLE_TAGS)
            .build();

    private static final GroupInfo UPDATED_GROUP_INFO = GROUP_INFO.toBuilder()
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(2L))
            .setTags(Tags.newBuilder().build())
            .build();

    private static final ClusterInfo CLUSTER_INFO = ClusterInfo.newBuilder()
            .setName("the kluster")
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(2L))
            .setTags(SAMPLE_TAGS)
            .build();

    private static final ClusterInfo UPDATED_CLUSTER_INFO = CLUSTER_INFO.toBuilder()
            .setMembers(StaticGroupMembers.newBuilder()
                    .addStaticMemberOids(3L))
            .setTags(Tags.newBuilder().build())
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

    /**
     * Release all resources occupied by test.
     */
    @After
    public void tearDown() {
        dbConfig.clean();
    }

    /**
     * This test verifies a few behaviors on new group creation:
     *
     * 1) GroupStore.newUserGroup(...) returns the expected group object.
     * 2) GroupStore.getUpdateEventStream() publishes the expected ADDED event.
     * 3) GroupStore.get(group id) returns the expected group object from the db query.
     *
     * Although we typically don't want to mix so many checks into one test, because this test
     * involves creating a new db schema and writing/reading data to it, it's basically an
     * integration test and is resource-consuming and time-consuming to run since new schemas also
     * require schema migrations to be applied. Might as well reduce the number of schemas created
     * instead of doing the exact same expensive operation in separate tests.
     */
    @Test
    public void testNewUserGroupThenGetAndEvent() {
        when(identityProvider.next()).thenReturn(GROUP_ID);

        final SetOnce<Group> newGroup = new SetOnce<>();
        // verify that the "add" update event is published after creating the new group
        StepVerifier.create(groupStore.getUpdateEventStream())
                .then(() -> {
                    try {
                        newGroup.trySetValue(groupStore.newUserGroup(GROUP_INFO));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(event -> event.getType() == GroupChangeType.ADDED
                        && event.getGroup().getId() == GROUP_ID)
                .thenCancel()
                .verify();

        assertTrue(newGroup.getValue().isPresent());
        final Group group = newGroup.getValue().get();
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(GROUP_INFO));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getType(), is(Type.GROUP));

        final Group gotGroup = groupStore.get(GROUP_ID).get();
        assertThat(gotGroup, is(group));

        assertTrue(tagsAreInDB(GROUP_ID));
    }

    public void testNewUserGroupDuplicateNameOnly() throws DuplicateNameException {
        when(identityProvider.next()).thenReturn(GROUP_ID).thenReturn(GROUP_ID + 1);
        groupStore.newUserGroup(GROUP_INFO);

        // Try to create a group with the same name.
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
        groupStore.newUserGroup(GroupInfo.newBuilder()
                .setName(GROUP_INFO.getName())
                .setEntityType(GROUP_INFO.getEntityType())
                .build());
    }

    /**
     * Tests that the Edit User Group is working with a database round-trip, and also verifies that
     * appropriate update event is sent. We are consolidating these test cases into one test to
     * reduce the number of unique DB schemas needed to run all tests.
     *
     * @throws DuplicateNameException
     * @throws ImmutableGroupUpdateException
     * @throws GroupNotFoundException
     */
    @Test
    public void testEditUserGroupAndEvent() throws DuplicateNameException, ImmutableGroupUpdateException, GroupNotFoundException {
        when(identityProvider.next()).thenReturn(GROUP_ID);

        groupStore.newUserGroup(GROUP_INFO);

        final SetOnce<Group> updatedGroup = new SetOnce<>();
        // verify that the "update" event is published after the group is updated
        StepVerifier.create(groupStore.getUpdateEventStream())
                .then(() -> {
                    try {
                        updatedGroup.trySetValue(groupStore.updateUserGroup(GROUP_ID, UPDATED_GROUP_INFO));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(event -> event.getType() == GroupChangeType.UPDATED
                        && event.getGroup().getId() == GROUP_ID)
                .thenCancel()
                .verify();

        assertTrue(updatedGroup.getValue().isPresent());
        final Group group = updatedGroup.getValue().get();

        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(UPDATED_GROUP_INFO));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getType(), is(Type.GROUP));

        final Group gotGroup = groupStore.get(GROUP_ID).get();
        assertThat(gotGroup, is(group));

        assertFalse(tagsAreInDB(GROUP_ID));
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

        assertTrue(tagsAreInDB(GROUP_ID));
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

        assertTrue(tagsAreInDB(GROUP_ID));
    }

    @Test
    public void testUpdateTargetGroupsOldRemoved() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO), Collections.emptyList());

        assertTrue(groupStore.get(GROUP_ID).isPresent());
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.emptyList());
        assertFalse(groupStore.get(GROUP_ID).isPresent());

        assertFalse(tagsAreInDB(GROUP_ID));
    }

    @Test
    public void testUpdateTargetGroupsOldClusterRemoved() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(), Collections.singletonList(CLUSTER_INFO));

        assertTrue(groupStore.get(GROUP_ID).isPresent());
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.emptyList());
        assertFalse(groupStore.get(GROUP_ID).isPresent());

        assertFalse(tagsAreInDB(GROUP_ID));
    }

    @Test
    public void testUpdateTargetGroupUpdatedExistingGroup() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.singletonList(GROUP_INFO), Collections.emptyList());

        assertThat(groupStore.get(GROUP_ID).get().getGroup(), is(GROUP_INFO));

        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.singletonList(UPDATED_GROUP_INFO), Collections.emptyList());

        assertThat(groupStore.get(GROUP_ID).get().getGroup(), is(UPDATED_GROUP_INFO));

        assertFalse(tagsAreInDB(GROUP_ID));
    }

    @Test
    public void testUpdateTargetClusterUpdatedUpdatedExistingCluster() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID,
                Collections.emptyList(), Collections.singletonList(CLUSTER_INFO));

        assertThat(groupStore.get(GROUP_ID).get().getCluster(), is(CLUSTER_INFO));

        groupStore.updateTargetGroups(dbConfig.dsl(), TARGET_ID, Collections.emptyList(), Collections.singletonList(UPDATED_CLUSTER_INFO));

        assertThat(groupStore.get(GROUP_ID).get().getCluster(), is(UPDATED_CLUSTER_INFO));

        assertFalse(tagsAreInDB(GROUP_ID));
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

        assertTrue(tagsAreInDB(GROUP_ID));
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

    /**
     * Verify that groups can be deleted with associated policies, and that the delete event is
     * published on the event stream.
     *
     * @throws DuplicateNameException
     * @throws GroupNotFoundException
     * @throws ImmutableGroupUpdateException
     * @throws PolicyDeleteException
     * @throws ImmutablePolicyUpdateException
     * @throws PolicyNotFoundException
     */
    @Test
    public void testDeleteGroupAndAssociatedPolicies()
            throws DuplicateNameException, GroupNotFoundException, ImmutableGroupUpdateException,
               PolicyDeleteException, ImmutablePolicyUpdateException, PolicyNotFoundException {
        when(identityProvider.next()).thenReturn(GROUP_ID);
        groupStore.newUserGroup(GROUP_INFO);

        assertTrue(tagsAreInDB(GROUP_ID));

        final SetOnce<Group> deletedGroup = new SetOnce<>();
        // verify that the "delete" event is published after deleting the group
        StepVerifier.create(groupStore.getUpdateEventStream())
                .then(() -> {
                    try {
                        deletedGroup.trySetValue(groupStore.deleteUserGroup(GROUP_ID));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(event -> event.getType() == GroupChangeType.REMOVED
                        && event.getGroup().getId() == GROUP_ID)
                .thenCancel()
                .verify();

        assertTrue(deletedGroup.getValue().isPresent());
        final Group group = deletedGroup.getValue().get();

        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(GROUP_INFO));

        assertFalse(groupStore.get(GROUP_ID).isPresent());
        assertFalse(tagsAreInDB(GROUP_ID));

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

    /**
     * Verify that group store updates are only received after subscribing to them.
     *
     * @throws DuplicateNameException if the group we are creating already exists (shouldnt happen)
     */
    @Test
    public void testHotUpdates() throws DuplicateNameException {
        when(identityProvider.next()).thenReturn(GROUP_ID);

        groupStore.newUserGroup(GROUP_INFO);
        // dummy subscriber
        groupStore.getUpdateEventStream().subscribe();

        final SetOnce<Group> updatedGroup = new SetOnce<>();
        // verify that the "new" update event is published after the group is updated but the "old" udpate event is not
        StepVerifier.create(groupStore.getUpdateEventStream())
                .then(() -> {
                    try {
                        updatedGroup.trySetValue(groupStore.updateUserGroup(GROUP_ID, UPDATED_GROUP_INFO));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(event -> event.getType() == GroupChangeType.UPDATED
                        && event.getGroup().getId() == GROUP_ID)
                .thenCancel()
                .verify();

        assertTrue(updatedGroup.getValue().isPresent());
        final Group group = updatedGroup.getValue().get();

        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getGroup(), is(UPDATED_GROUP_INFO));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getType(), is(Type.GROUP));

        final Group gotGroup = groupStore.get(GROUP_ID).get();
        assertThat(gotGroup, is(group));

        assertFalse(tagsAreInDB(GROUP_ID));
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

    /**
     * Checks if the {@link #SAMPLE_TAGS} are in the database under a specific group.
     *
     * @param id id of the group to check.
     * @return true if all tags are there, false if no tags are there.
     * @throws IllegalStateException should not happen: some tags are there, but they are not correct
     */
    private boolean tagsAreInDB(long id) throws IllegalStateException {
        final Set<TagsGroupRecord> tagsGroupRecords =
                dbConfig.dsl()
                        .selectFrom(Tables.TAGS_GROUP)
                        .where(Tables.TAGS_GROUP.GROUP_ID.eq(id))
                        .fetch()
                        .into(TagsGroupRecord.class)
                        .stream()
                        .collect(Collectors.toSet());
        if (tagsGroupRecords.isEmpty()) {
            return false;
        } else if (sampleTagsInDB(id).equals(tagsGroupRecords)) {
            return true;
        }
        throw new IllegalStateException("boom");
    }

    private Set<TagsGroupRecord> sampleTagsInDB(long id) {
        Assert.assertEquals(SAMPLE_TAGS, groupStore.getTags());
        return
            ImmutableSet.of(
                new TagsGroupRecord(id, TAG_KEY_1, TAG_VALUE_11),
                new TagsGroupRecord(id, TAG_KEY_1, TAG_VALUE_12),
                new TagsGroupRecord(id, TAG_KEY_2, TAG_VALUE_2));
    }
}
