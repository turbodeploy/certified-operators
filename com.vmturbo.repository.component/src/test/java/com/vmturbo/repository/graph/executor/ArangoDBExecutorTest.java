package com.vmturbo.repository.graph.executor;

import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.dc;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.host;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.nodeMapFor;
import static com.vmturbo.repository.graph.result.SubgraphResultUtilities.vm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javaslang.control.Try;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsFixture;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;
import com.vmturbo.repository.topology.GlobalSupplyChainRelationships;

@RunWith(MockitoJUnitRunner.class)
public class ArangoDBExecutorTest {

    private ArangoDBExecutor arangoDBExecutor;

    private GraphCmd graphCmd;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Try<SupplyChainSubgraph> supplyChainSubgraph;

    private Try<Collection<ServiceEntityRepoDTO>> searchServiceEntityResults;

    @Mock
    private ArangoDB arangoDriver;

    @Mock
    private ArangoDatabaseFactory databaseFactory;

    @Before
    public void setUp() {
        arangoDBExecutor = new ArangoDBExecutor(databaseFactory, "db");

        given(databaseFactory.getArangoDriver()).willReturn(arangoDriver);
    }

    @Test
    public void testExecuteSupplyChainCmdWithException() throws Exception {
        givenASupplyChainCmd("start", "graph", Optional.empty(), "vertex");
        givenArangoDriverWillThrowException();

        whenExecuteSupplyChainCmd();

        thenSupplyChainExecutorResultIsEmpty();
    }

    @Test
    public void testExecuteSupplyChainCmdConsumerResults() throws Exception {

        ResultVertex host1a = host("1", "", "2");
        ResultVertex host1b = host("1", "", "3");
        ResultVertex host1c = host("1", "", "4");
        ResultVertex host1d = host("1", "", "5");

        ResultVertex vm1 = vm("2", "1", "");
        ResultVertex vm2 = vm("3", "1", "");
        ResultVertex vm3 = vm("4", "1", "");
        ResultVertex vm4 = vm("5", "1", "");

        List<ResultVertex> vertices =
                ImmutableList.of(host1a, host1b, host1c, host1d,
                        vm1, vm2, vm3, vm4);

        givenASupplyChainCmd("start", "graph", Optional.empty(), "vertex");
        givenArangoDriverWillThrowException();
        givenSupplyChainSubgraphResults(vertices, vertices);
        whenExecuteSupplyChainCmd();
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(supplyChainSubgraph.get());
        assertEquals(1, RepositoryDTOUtil.getMemberCount(supplyChainNodes.get(UIEntityType.PHYSICAL_MACHINE.apiStr())));
        assertEquals(4, RepositoryDTOUtil.getMemberCount(supplyChainNodes.get(UIEntityType.VIRTUAL_MACHINE.apiStr())));

        org.hamcrest.MatcherAssert.assertThat(
            supplyChainNodes.get(UIEntityType.PHYSICAL_MACHINE.apiStr()).getConnectedConsumerTypesList(),
            contains(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        org.hamcrest.MatcherAssert.assertThat(
            supplyChainNodes.get(UIEntityType.PHYSICAL_MACHINE.apiStr()).getConnectedProviderTypesList(),
            is(empty()));
        org.hamcrest.MatcherAssert.assertThat(
            supplyChainNodes.get(UIEntityType.VIRTUAL_MACHINE.apiStr()).getConnectedProviderTypesList(),
            contains(UIEntityType.PHYSICAL_MACHINE.apiStr()));
    }

    @Test
    public void testExecuteSupplyChainCmd() throws Exception {

        ResultVertex host1a = host("1", "20", "2");
        ResultVertex host1b = host("1", "20", "3");
        ResultVertex host1c = host("1", "20", "4");
        ResultVertex host1d = host("1", "20", "5");
        // This edge won't be traversed
        ResultVertex host2a = host("10", "20", "50");

        ResultVertex vm1 = vm("2", "1", "");
        ResultVertex vm2 = vm("3", "1", "");
        ResultVertex vm3 = vm("4", "1", "");
        ResultVertex vm4 = vm("5", "1", "");
        // This edge won't be traversed because it is not outward from host 1
        ResultVertex vm5 = vm("50", "10", "");

        ResultVertex dc1a = dc("20", "", "1");
        ResultVertex dc1b = dc("20", "", "10");

        List<ResultVertex> vertices =
                ImmutableList.of(host1a, host1b, host1c, host1d, host2a,
                        vm1, vm2, vm3, vm4, vm5, dc1a, dc1b);

        givenASupplyChainCmd("start", "graph", Optional.empty(), "vertex");
        givenArangoDriverWillThrowException();
        givenSupplyChainSubgraphResults(vertices, vertices);
        whenExecuteSupplyChainCmd();
        final Map<String, SupplyChainNode> supplyChainNodes = nodeMapFor(supplyChainSubgraph.get());
        final SupplyChainNode pmNode = supplyChainNodes.get(UIEntityType.PHYSICAL_MACHINE.apiStr());
        assertEquals(1, RepositoryDTOUtil.getMemberCount(pmNode));
        assertEquals(4, RepositoryDTOUtil.getMemberCount(supplyChainNodes.get(UIEntityType.VIRTUAL_MACHINE.apiStr())));
        assertEquals(1, RepositoryDTOUtil.getMemberCount(supplyChainNodes.get(UIEntityType.DATACENTER.apiStr())));

        org.hamcrest.MatcherAssert.assertThat(pmNode.getConnectedConsumerTypesList(),
            contains(UIEntityType.VIRTUAL_MACHINE.apiStr()));
        org.hamcrest.MatcherAssert.assertThat(pmNode.getConnectedProviderTypesList(),
            contains(UIEntityType.DATACENTER.apiStr()));
        org.hamcrest.MatcherAssert.assertThat(
            supplyChainNodes.get(UIEntityType.VIRTUAL_MACHINE.apiStr()).getConnectedProviderTypesList(),
            contains(UIEntityType.PHYSICAL_MACHINE.apiStr()));
    }

    @Test
    public void testSearchServiceEntityException() throws Exception {
        givenASearchEntityCmd("collection", "field", "query", GraphCmd.SearchType.FULLTEXT);
        givenArangoDriverWillThrowException();

        whenSearchServiceEntity();

        thenSearchServiceEntityResultIsEmpty();
    }

    @Test
    public void testSearchServiceEntity() throws Exception {
        final List<ServiceEntityRepoDTO> mockResults = fill(5, ResultsFixture.VM_TYPE);

        givenASearchEntityCmd("collection", "field", "query", GraphCmd.SearchType.FULLTEXT);
        givenSearchServiceEntityResults(mockResults);

        whenSearchServiceEntity();

        thenSearchServiceEntityResults(mockResults);
    }

    @Test
    public void getSupplyChainRels() throws Exception {
        final ArangoCursor<BaseDocument> searchResultCursor = Mockito.mock(ArangoCursor.class);
        final String key1 = "Virtual Machine";
        final List<String> key2 = Arrays.asList("Storage");
        final BaseDocument document = new BaseDocument();
        document.addAttribute(key1, key2);
        final List<BaseDocument> documents = new ArrayList<>(Arrays.asList(document));
        final Multimap<String, String> providerRels = HashMultimap.create();
        providerRels.putAll(key1,key2);
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        GlobalSupplyChainRelationships mockResult = new GlobalSupplyChainRelationships(documents);

        when(mockDatabase.collection(anyString())).thenReturn(mock(ArangoCollection.class));
        when(arangoDriver.db(anyString())).thenReturn(mockDatabase);
        when(mockDatabase.query(ArangoDBQueries.GET_SUPPLY_CHAIN_RELS, null, null,
            BaseDocument.class))
            .thenReturn(searchResultCursor);
        when(searchResultCursor.asListRemaining()).thenReturn(documents);
        mockDatabase.collection(arangoDBExecutor.GLOBAL_SUPPLY_CHAIN_RELS_COLLECTION).insertDocument(document);

        assertThat(arangoDBExecutor.getSupplyChainRels()).isEqualToComparingFieldByField(mockResult);
    }

    @Test
    public void testSearchServiceEntityNonFullText() throws Exception {
        final List<ServiceEntityRepoDTO> mockResults = fill(5, ResultsFixture.VM_TYPE);

        givenASearchEntityCmd("collection", "uuid", "10", GraphCmd.SearchType.STRING);
        givenSearchServiceEntityResults(mockResults);

        whenSearchServiceEntity();

        thenSearchServiceEntityResults(mockResults);
    }

    private void givenASearchEntityCmd(final String collection,
                                       final String field,
                                       final String query,
                                       final GraphCmd.SearchType searchType) {
        graphCmd = new GraphCmd.SearchServiceEntity(collection, field, query, searchType);
    }

    private void givenSearchServiceEntityResults(final List<ServiceEntityRepoDTO> searchResults) throws ArangoDBException {
        final ArangoCursor<ServiceEntityRepoDTO> searchResultCursor = Mockito.mock(ArangoCursor.class);
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final String query = ArangoDBExecutor.searchServiceEntitytQuery((GraphCmd.SearchServiceEntity) graphCmd);

        given(searchResultCursor.asListRemaining()).willReturn(searchResults);

        when(arangoDriver.db(anyString())).thenReturn(mockDatabase);

        when(mockDatabase.query(query, null, null, ServiceEntityRepoDTO.class))
                .thenReturn(searchResultCursor);
    }

    private void givenASupplyChainCmd(final String starting,
                                      final String graphName,
                                      final Optional<UIEnvironmentType> environmentType,
                                      final String vertexColl) {
        graphCmd = new GraphCmd.GetSupplyChain(starting, environmentType,
            graphName, vertexColl, Optional.empty(),
                Collections.emptySet(), Collections.emptySet());
    }

    @SuppressWarnings("unchecked")
    private void givenSupplyChainSubgraphResults(final List<ResultVertex> providerResults,
                                                 final List<ResultVertex> consumerResults) throws Exception {

        final ArangoCursor<ResultVertex> providerCursor = Mockito.mock(ArangoCursor.class);
        final ArangoCursor<ResultVertex> consumerCursor = Mockito.mock(ArangoCursor.class);

        given(providerCursor.asListRemaining()).willReturn(providerResults);
        given(consumerCursor.asListRemaining()).willReturn(consumerResults);

        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        when(mockDatabase.query(anyString(), anyMap(), any(AqlQueryOptions.class),
                                          Matchers.<Class<ResultVertex>>any()))
                .thenReturn(providerCursor)
                .thenReturn(consumerCursor);

        when(arangoDriver.db(anyString())).thenReturn(mockDatabase);
    }

    @SuppressWarnings("unchecked")
    private void givenArangoDriverWillThrowException() throws Exception {
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        given(mockDatabase.query(anyString(),
                anyMap(),
                any(AqlQueryOptions.class),
                anyObject())).willThrow(new ArangoDBException("Mock exception"));

        given(arangoDriver.db(anyString())).willReturn(mockDatabase);
    }

    private void whenExecuteSupplyChainCmd() {
        supplyChainSubgraph = arangoDBExecutor.executeSupplyChainCmd((GraphCmd.GetSupplyChain) graphCmd);
    }

    private void thenSupplyChainExecutorResultIsEmpty() {
        assertThat(supplyChainSubgraph).isEmpty();
    }

    private void whenSearchServiceEntity() {
        searchServiceEntityResults = arangoDBExecutor.executeSearchServiceEntityCmd((GraphCmd.SearchServiceEntity) graphCmd);
    }

    private void thenSearchServiceEntityResultIsEmpty() {
        assertThat(searchServiceEntityResults).isEmpty();
    }

    private void thenSearchServiceEntityResults(final List<ServiceEntityRepoDTO> expectedResults) {
        assertThat(searchServiceEntityResults.get()).hasSameElementsAs(expectedResults);
    }
}
