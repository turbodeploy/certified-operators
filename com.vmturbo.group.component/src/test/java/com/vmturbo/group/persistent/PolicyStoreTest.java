package com.vmturbo.group.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

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

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.ImmutableGroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.ArangoDBFixtures.MockDatabase;
import com.vmturbo.group.persistent.PolicyStore.PolicyDeleteException;

@RunWith(MockitoJUnitRunner.class)
public class PolicyStoreTest {

    private static String TEST_DATABASE = "test";
    private static String TEST_POLICY_COLLECTION = "test-policy-collection";
    private static String TEST_GROUP_COLLECTION = "test-group-collection";
    private static String TEST_CLUSTER_COLLECTION = "test-cluster-collection";

    @Mock
    private ArangoDriverFactory arangoDriverFactory;

    @Mock
    private ArangoDB arangoDB;

    @Mock
    private IdentityProvider identityProvider;

    private PolicyStore policyStore;

    private MockDatabase mockDatabase;

    @Before
    public void setUp() throws Exception {
        final GroupDBDefinition groupDBDefinition = ImmutableGroupDBDefinition.builder()
                .databaseName(TEST_DATABASE)
                .groupCollection(TEST_GROUP_COLLECTION)
                .policyCollection(TEST_POLICY_COLLECTION)
                .clusterCollection(TEST_CLUSTER_COLLECTION)
                .build();
        policyStore = new PolicyStore(arangoDriverFactory, groupDBDefinition, identityProvider);

        given(arangoDriverFactory.getDriver()).willReturn(arangoDB);
        mockDatabase = ArangoDBFixtures.mockDatabase(arangoDB, TEST_DATABASE);
    }

    @Test
    public void testGet() {
        final Long policyID = 1000L;
        final PolicyDTO.InputPolicy testPolicy = PolicyDTO.InputPolicy.newBuilder().setId(policyID).build();

        mockDatabase.givenGetDocumentWillReturn(TEST_POLICY_COLLECTION,
                Long.toString(policyID), PolicyDTO.InputPolicy.class, testPolicy);

        final Optional<PolicyDTO.InputPolicy> retrieved = policyStore.get(policyID);

        assertThat(retrieved).isPresent();
        assertThat(retrieved).hasValue(testPolicy);
    }

    @Test
    public void testSaveReplace() {
        final Long policyID = 1000L;
        final PolicyDTO.InputPolicy testPolicy = PolicyDTO.InputPolicy.newBuilder().setId(policyID).build();

        mockDatabase.givenKeyExists(TEST_POLICY_COLLECTION, policyID.toString());

        final boolean result = policyStore.save(policyID, testPolicy);

        final ArangoCollection mockCollection =
                mockDatabase.createMockCollection(TEST_POLICY_COLLECTION);
        Mockito.verify(mockCollection).replaceDocument(policyID.toString(), testPolicy);
        assertThat(result).isTrue();
    }

    @Test
    public void testSaveInsert() {
        final Long policyID = 1000L;
        final PolicyDTO.InputPolicy testPolicy = PolicyDTO.InputPolicy.newBuilder().setId(policyID).build();

        mockDatabase.givenKeyDoesNotExist(TEST_POLICY_COLLECTION, policyID.toString());

        final boolean result = policyStore.save(policyID, testPolicy);

        final ArangoCollection mockCollection =
                mockDatabase.createMockCollection(TEST_POLICY_COLLECTION);
        Mockito.verify(mockCollection).insertDocument(testPolicy);
        assertThat(result).isTrue();
    }

    @Test
    public void testSaveInsertException() {
        final Long policyID = 1000L;
        final PolicyDTO.InputPolicy testPolicy = PolicyDTO.InputPolicy.newBuilder().setId(policyID).build();

        mockDatabase
            .givenKeyDoesNotExist(TEST_POLICY_COLLECTION, policyID.toString())
            .givenInsertWillThrowException(TEST_POLICY_COLLECTION,
                new ArangoDBException("Mock ArangoDB exception in save"));

        final boolean result = policyStore.save(policyID, testPolicy);

        assertThat(result).isFalse();
    }

    @Test
    public void testDelete() {
        final Long policyID = 1000L;

        final ArangoCollection mockCollection =
                mockDatabase.createMockCollection(TEST_POLICY_COLLECTION);

        final boolean result = policyStore.delete(policyID);

        Mockito.verify(mockCollection).deleteDocument(policyID.toString());
        assertThat(result).isTrue();
    }

    @Test
    public void testDeletePolicies() throws PolicyDeleteException {
        final Long policyID = 1000L;

        final ArangoCollection mockCollection = mockDatabase
            .createMockCollection(TEST_POLICY_COLLECTION);

        policyStore.deletePolicies(Lists.newArrayList(policyID));

        Mockito.verify(mockCollection).deleteDocuments(Lists.newArrayList(String.valueOf(policyID)), null,
            null);
    }

    @Test
    public void testDeleteException() {
        final Long policyID = 1000L;

        mockDatabase.givenDeleteWillThrowException(TEST_POLICY_COLLECTION,
                new ArangoDBException("Mock ArangoDB exception in delete"));

        final boolean result = policyStore.delete(policyID);

        assertThat(result).isFalse();
    }
}