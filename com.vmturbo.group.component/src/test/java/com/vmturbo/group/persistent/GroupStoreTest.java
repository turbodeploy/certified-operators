package com.vmturbo.group.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.ImmutableGroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
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
    }

    @Test
    public void testGet() throws Exception {
        final Long groupID = 1000L;
        final GroupDTO.Group testGroup = GroupDTO.Group.newBuilder().setId(groupID).build();

        ArangoDBFixtures.givenGetDocumentWillReturn(arangoDB, TEST_DATABASE, TEST_GROUP_COLLECTION,
                Long.toString(groupID), GroupDTO.Group.class, testGroup);

        final Optional<GroupDTO.Group> retrieved = groupStoreSpy.get(groupID);

        assertThat(retrieved).isPresent();
        assertThat(retrieved).hasValue(testGroup);
    }

    @Test
    public void testSaveInsert() throws Exception {
        final Long groupID = 1000L;
        when(identityProviderSpy.next()).thenReturn(groupID);

        final ArangoCollection mockCollection = ArangoDBFixtures
                .getMockCollection(arangoDB, TEST_DATABASE, TEST_GROUP_COLLECTION);
        ArangoDBFixtures.givenKeyDoesNotExist(mockCollection, groupID.toString());

        final GroupInfo info = GroupInfo.getDefaultInstance();
        doReturn(false).when(groupStoreSpy).isDuplicate(eq(groupID.longValue()), eq(info));
        final Group group = groupStoreSpy.newUserGroup(info);

        final GroupDTO.Group expectedGroup = GroupDTO.Group.newBuilder()
                .setId(groupID)
                .setOrigin(Origin.USER)
                .setInfo(info)
                .build();

        Mockito.verify(mockCollection).insertDocument(expectedGroup);
        assertEquals(expectedGroup, group);
    }

    @Test(expected = DatabaseException.class)
    public void testSaveInsertException() throws Exception {
        final Long groupID = 1000L;
        when(identityProviderSpy.next()).thenReturn(groupID);

        final ArangoCollection mockCollection = ArangoDBFixtures
                .getMockCollection(arangoDB, TEST_DATABASE, TEST_GROUP_COLLECTION);
        ArangoDBFixtures.givenKeyDoesNotExist(mockCollection, groupID.toString());
        ArangoDBFixtures.givenInsertWillThrowException(mockCollection,
                new ArangoDBException("Mock ArangoDB exception in save"));

        final GroupInfo info = GroupInfo.getDefaultInstance();
        doReturn(false).when(groupStoreSpy).isDuplicate(eq(groupID.longValue()), eq(info));
        groupStoreSpy.newUserGroup(info);
    }

    @Test(expected = DuplicateGroupException.class)
    public void testCreateNewDuplicate() throws Exception {
        final Long originalGroupID = 1000L;
        final Long duplicateGroupID = 9999L;
        when(identityProviderSpy.next()).thenReturn(duplicateGroupID);

        final GroupDTO.Group originalGroup = GroupDTO.Group.newBuilder()
            .setId(originalGroupID)
            .setInfo(GroupInfo.newBuilder().setName("foo"))
            .build();
        final GroupInfo duplicateGroup = GroupInfo.newBuilder()
            .setName("foo")
            .build();

        ArangoDBFixtures.givenQueryWillReturn(arangoDB, TEST_DATABASE,
            GroupStore.GROUPS_BY_NAME_QUERY, GroupDTO.Group.class, originalGroup);

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
            .setInfo(GroupInfo.newBuilder().setName(originalGroupName))
            .build();

        final GroupDTO.Group duplicateGroup = GroupDTO.Group.newBuilder()
            .setId(duplicateGroupID)
            .setInfo(GroupInfo.newBuilder().setName("bar"))
            .build();


        final ArangoDatabase mockDatabase = ArangoDBFixtures.givenQueryWillReturn(arangoDB, TEST_DATABASE,
            GroupStore.GROUPS_BY_NAME_QUERY, GroupDTO.Group.class, originalGroup);
        final ArangoCollection mockCollection = Mockito.mock(ArangoCollection.class);

        given(mockDatabase.collection(TEST_GROUP_COLLECTION)).willReturn(mockCollection);
        given(mockCollection.getDocument(duplicateGroupID.toString(), GroupDTO.Group.class)).willReturn(duplicateGroup);

        groupStoreSpy.updateUserGroup(duplicateGroupID, GroupInfo.newBuilder()
            .setName(originalGroupName)
            .build());
    }

    @Test
    public void testDelete() throws Exception {
        final Long groupID = 1000L;

        final ArangoCollection mockCollection = ArangoDBFixtures
                .getMockCollection(arangoDB, TEST_DATABASE, TEST_GROUP_COLLECTION);

        doReturn(Optional.of(Group.newBuilder().setId(groupID).build()))
            .when(groupStoreSpy).get(eq(groupID));

        groupStoreSpy.deleteUserGroup(groupID);

        Mockito.verify(mockCollection).deleteDocument(groupID.toString());
    }

    @Test(expected = DatabaseException.class)
    public void testDeleteException() throws Exception {
        final Long groupID = 1000L;

        final ArangoCollection mockCollection = ArangoDBFixtures
                .getMockCollection(arangoDB, TEST_DATABASE, TEST_GROUP_COLLECTION);
        ArangoDBFixtures.givenDeleteWillThrowException(mockCollection,
                new ArangoDBException("Mock ArangoDB exception in delete"));

        doReturn(Optional.of(Group.newBuilder().setId(groupID).build()))
                .when(groupStoreSpy).get(eq(groupID));

        groupStoreSpy.deleteUserGroup(groupID);
    }
}