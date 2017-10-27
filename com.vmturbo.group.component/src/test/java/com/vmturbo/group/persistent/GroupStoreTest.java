package com.vmturbo.group.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.ImmutableGroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.ArangoDBFixtures.MockDatabase;
import com.vmturbo.group.persistent.GroupStore.DuplicateGroupException;

@RunWith(MockitoJUnitRunner.class)
public class GroupStoreTest {

    private static String TEST_DATABASE = "test";
    private static String TEST_POLICY_COLLECTION = "test-policy-collection";
    private static String TEST_GROUP_COLLECTION = "test-group-collection";
    private static String TEST_CLUSTER_COLLECTION = "test-cluster-collection";

    @Mock
    private ArangoDriverFactory arangoDriverFactory;

    @Mock
    private ArangoDB arangoDB;

    private GroupStore groupStoreSpy;

    private IdentityProvider identityProviderSpy = spy(new IdentityProvider(0));

    private MockDatabase mockDatabase;

    @Before
    public void setUp() throws Exception {
        final GroupDBDefinition groupDBDefinition = ImmutableGroupDBDefinition.builder()
                .databaseName(TEST_DATABASE)
                .groupCollection(TEST_GROUP_COLLECTION)
                .policyCollection(TEST_POLICY_COLLECTION)
                .clusterCollection(TEST_CLUSTER_COLLECTION)
                .build();
        groupStoreSpy = spy(new GroupStore(arangoDriverFactory,
                groupDBDefinition, identityProviderSpy));

        given(arangoDriverFactory.getDriver()).willReturn(arangoDB);
        mockDatabase = ArangoDBFixtures.mockDatabase(arangoDB, TEST_DATABASE);
    }

    @Test
    public void testGet() throws Exception {
        final Long groupID = 1000L;
        final GroupDTO.Group testGroup = GroupDTO.Group.newBuilder().setId(groupID).build();

        mockDatabase.givenQueryWillReturn(
                GroupStore.ALL_GROUPS_QUERY, GroupDTO.Group.class, testGroup);

        final Optional<GroupDTO.Group> retrieved = groupStoreSpy.get(groupID);

        assertThat(retrieved).isPresent();
        assertThat(retrieved).hasValue(testGroup);
    }

    @Test
    public void testGetAll() throws Exception {
        final Long groupID = 1000L;
        final GroupDTO.Group testGroup = GroupDTO.Group.newBuilder().setId(groupID).build();

        mockDatabase.givenQueryWillReturn(GroupStore.ALL_GROUPS_QUERY,
                GroupDTO.Group.class, testGroup);

        final Collection<Group> retrieved = groupStoreSpy.getAll();

        assertThat(retrieved).containsExactly(testGroup);
    }

    @Test
    public void testGetSome() throws Exception {
        final Long groupID = 1000L;
        final GroupDTO.Group testGroup = GroupDTO.Group.newBuilder().setId(groupID).build();

        mockDatabase.givenQueryWillReturn(GroupStore.ALL_GROUPS_QUERY,
                GroupDTO.Group.class, testGroup);

        final Map<Long, Optional<Group>> retrieved =
                groupStoreSpy.getGroups(Collections.singleton(groupID));

        // It should be found.
        assertEquals(testGroup, retrieved.get(groupID).get());
    }

    @Test
    public void testGetSomeNotFound() throws Exception {
        final Long groupID = 1000L;

        mockDatabase.givenQueryWillReturn(
                GroupStore.ALL_GROUPS_QUERY, GroupDTO.Group.class, Collections.emptyList());

        final Map<Long, Optional<Group>> retrieved =
                groupStoreSpy.getGroups(Collections.singleton(groupID));

        // It should be found.
        assertFalse(retrieved.get(groupID).isPresent());
    }

    @Test
    public void testSaveInsert() throws Exception {
        final Long groupID = 1000L;
        when(identityProviderSpy.next()).thenReturn(groupID);

        mockDatabase.givenKeyDoesNotExist(TEST_GROUP_COLLECTION, groupID.toString());

        final GroupInfo info = GroupInfo.getDefaultInstance();
        doReturn(false).when(groupStoreSpy).checkForDuplicates(eq(groupID.longValue()), eq(info));
        final Group group = groupStoreSpy.newUserGroup(info);

        final GroupDTO.Group expectedGroup = GroupDTO.Group.newBuilder()
                .setId(groupID)
                .setOrigin(Origin.USER)
                .setType(Type.GROUP)
                .setGroup(info)
                .build();

        final ArangoCollection mockCollection = mockDatabase
                .createMockCollection(TEST_GROUP_COLLECTION);
        Mockito.verify(mockCollection).insertDocument(expectedGroup);
        assertEquals(expectedGroup, group);
    }

    @Test(expected = DatabaseException.class)
    public void testSaveInsertException() throws Exception {
        final Long groupID = 1000L;
        when(identityProviderSpy.next()).thenReturn(groupID);

        mockDatabase
            .givenKeyDoesNotExist(TEST_GROUP_COLLECTION, groupID.toString())
            .givenInsertWillThrowException(TEST_GROUP_COLLECTION,
                new ArangoDBException("Mock ArangoDB exception in save"));

        final GroupInfo info = GroupInfo.getDefaultInstance();
        doReturn(false).when(groupStoreSpy).checkForDuplicates(eq(groupID.longValue()), eq(info));
        groupStoreSpy.newUserGroup(info);
    }

    @Test(expected = DuplicateGroupException.class)
    public void testCreateNewDuplicate() throws Exception {
        final Long originalGroupID = 1000L;
        final Long duplicateGroupID = 9999L;
        when(identityProviderSpy.next()).thenReturn(duplicateGroupID);

        final GroupDTO.Group originalGroup = GroupDTO.Group.newBuilder()
            .setId(originalGroupID)
            .setGroup(GroupInfo.newBuilder().setName("foo"))
            .build();
        final GroupInfo duplicateGroup = GroupInfo.newBuilder()
            .setName("foo")
            .build();

        mockDatabase.givenQueryWillReturn(GroupStore.GROUPS_BY_NAME_QUERY, GroupDTO.Group.class,
                originalGroup);

        groupStoreSpy.newUserGroup(duplicateGroup);
    }

    @Test(expected = DuplicateGroupException.class)
    public void testUpdateDuplicate() throws Exception {
        final Long originalGroupID = 1000L;
        final Long duplicateGroupID = 9999L;
        final String originalGroupName = "foo";
        when(identityProviderSpy.next()).thenReturn(duplicateGroupID);

        final GroupDTO.Group originalGroup = GroupDTO.Group.newBuilder()
            .setId(originalGroupID)
            .setGroup(GroupInfo.newBuilder().setName(originalGroupName))
            .build();

        final GroupDTO.Group duplicateGroup = GroupDTO.Group.newBuilder()
            .setId(duplicateGroupID)
            .setGroup(GroupInfo.newBuilder().setName("bar"))
            .build();


        // Make sure the original group with the same name gets returned from the groups query.
        mockDatabase
            .givenQueryWillReturn(GroupStore.GROUPS_BY_NAME_QUERY, GroupDTO.Group.class,
                originalGroup)
            // Pretend the "duplicate" group (the one that's being updated) exists in the database
            // already.
            .givenQueryWillReturn(GroupStore.ALL_GROUPS_QUERY, GroupDTO.Group.class, duplicateGroup);


        groupStoreSpy.updateUserGroup(duplicateGroupID, GroupInfo.newBuilder()
            .setName(originalGroupName)
            .build());
    }

    @Test
    public void testDelete() throws Exception {
        final Long groupID = 1000L;

        final ArangoCollection mockCollection = mockDatabase
                .createMockCollection(TEST_GROUP_COLLECTION);

        doReturn(Optional.of(Group.newBuilder().setId(groupID).build()))
            .when(groupStoreSpy).get(eq(groupID));

        groupStoreSpy.deleteUserGroup(groupID);

        Mockito.verify(mockCollection).deleteDocument(groupID.toString());
    }

    @Test(expected = DatabaseException.class)
    public void testDeleteException() throws Exception {
        final Long groupID = 1000L;

        mockDatabase.givenDeleteWillThrowException(TEST_GROUP_COLLECTION,
                new ArangoDBException("Mock ArangoDB exception in delete"));

        doReturn(Optional.of(Group.newBuilder().setId(groupID).build()))
                .when(groupStoreSpy).get(eq(groupID));

        groupStoreSpy.deleteUserGroup(groupID);
    }
}