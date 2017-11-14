package com.vmturbo.repository.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;


@RunWith(MockitoJUnitRunner.class)
public class GraphTopologyServiceTest {

    private GraphTopologyService graphTopologyServiceTest;

    private GraphDefinition graphDefinition = new GraphDefinition.Builder()
            .setGraphName("seGraph")
            .setServiceEntityVertex("seVertexCollection")
            .setProviderRelationship("seProviderEdgeCollection")
            .setTopologyProtoCollection("topology_proto")
            .createGraphDefinition();

    @Mock
    private ArangoDB arangoDB;

    @Mock
    private ArangoDatabase arangoDatabase;

    @Mock
    private ArangoDatabaseFactory arangoDatabaseFactory;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() {
        graphTopologyServiceTest = new GraphTopologyService(arangoDatabaseFactory,
                                                            graphDefinition);

        given(arangoDatabaseFactory.getArangoDriver()).willReturn(arangoDB);
    }

    @Test
    public void testDeleteTopology() {
        final String topologyId = "12345";
        final String matchingDatabaseName = "topology-98798-12345";
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        given(arangoDB.getDatabases()).willReturn(Arrays.asList(matchingDatabaseName, "topology-8921-12383"));
        given(arangoDB.db(matchingDatabaseName)).willReturn(mockDatabase);

        graphTopologyServiceTest.deleteTopology(topologyId);

        verify(mockDatabase).drop();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteTopologyNoMatch() {
        final String topologyId = "12345";
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        given(arangoDB.getDatabases()).willReturn(Arrays.asList("topology-89374-1209238", "topology-8921-12383"));
        given(arangoDB.db(anyString())).willReturn(mockDatabase);

        graphTopologyServiceTest.deleteTopology(topologyId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteTopologyMultipleMatches() {
        final String topologyId = "12345";
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        given(arangoDB.getDatabases()).willReturn(Arrays.asList("topology-89374-12345", "topology-107324-12345"));
        given(arangoDB.db(anyString())).willReturn(mockDatabase);

        graphTopologyServiceTest.deleteTopology(topologyId);
    }

    @Test
    public void topologyDatabaseNameFilter() {
        Boolean result = false;
        final String topologyId = "123456789";

        final String invalidName = "group-policy";
        result = GraphTopologyService.topologyDatabaseFilter(invalidName, topologyId);
        assertFalse(result);

        final String invalidTopoName = "topology-77777-";
        result = GraphTopologyService.topologyDatabaseFilter(invalidTopoName, topologyId);
        assertFalse(result);

        final String mismatchTopologyName = "topology-77777-98798";
        result = GraphTopologyService.topologyDatabaseFilter(mismatchTopologyName, topologyId);
        assertFalse(result);

        final String zeroString = "0";
        result = GraphTopologyService.topologyDatabaseFilter(zeroString, topologyId);
        assertFalse(result);

        final String containStringTest = "topology-7777-1234567890";
        result = GraphTopologyService.topologyDatabaseFilter(containStringTest, topologyId);
        assertFalse(result);

        final String exactMatchDatabaseName = "topology-7777-123456789";
        result = GraphTopologyService.topologyDatabaseFilter(exactMatchDatabaseName, topologyId);
        assertTrue(result);
    }

    @Test
    public void testRetrieve() {
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final ArangoCollection mockCollection = Mockito.mock(ArangoCollection.class);
        final long topologyID = 1L;

        final TopologyDTO.Topology testTopo = TopologyDTO.Topology.newBuilder()
                .setTopologyId(topologyID)
                .setData(TopologyDTO.Topology.Data.newBuilder().build())
                .build();

        given(arangoDB.db()).willReturn(mockDatabase);
        given(mockDatabase.collection(anyString())).willReturn(mockCollection);
        given(mockCollection.getDocument(eq(Long.toString(topologyID)), eq(TopologyDTO.Topology.class)))
                .willReturn(testTopo);

        final TopologyDTO.Topology retrieved = graphTopologyServiceTest.retrieveTopology(topologyID);

        assertEquals(testTopo, retrieved);
    }

    @Test
    public void testRetrieveNotFound() {
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final ArangoCollection mockCollection = Mockito.mock(ArangoCollection.class);

        given(arangoDB.db()).willReturn(mockDatabase);
        given(mockDatabase.collection(anyString())).willReturn(mockCollection);
        given(mockCollection.getDocument(anyString(), eq(TopologyDTO.Topology.class)))
                .willReturn(null);

        final TopologyDTO.Topology retrieved = graphTopologyServiceTest.retrieveTopology(1L);

        assertNull(retrieved);
    }
}
