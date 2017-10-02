package com.vmturbo.repository.graph.executor;

import static com.vmturbo.repository.graph.result.ResultsFixture.DC_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fill;
import static com.vmturbo.repository.graph.result.ResultsFixture.supplyChainQueryResultFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlQueryOptions;
import com.google.common.collect.Lists;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainExecutorResult;
import com.vmturbo.repository.graph.result.SupplyChainQueryResult;
import com.vmturbo.repository.topology.TopologyDatabase;

import javaslang.control.Try;

@RunWith(MockitoJUnitRunner.class)
public class ArangoDBExecutorTest {

    private ArangoDBExecutor arangoDBExecutor;

    private GraphCmd graphCmd;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Try<SupplyChainExecutorResult> supplyChainExecutorResult;

    private Try<Collection<ServiceEntityRepoDTO>> searchServiceEntityResults;

    @Mock
    private ArangoDB arangoDriver;

    @Mock
    private ArangoDatabaseFactory databaseFactory;

    @Before
    public void setUp() {
        arangoDBExecutor = new ArangoDBExecutor(databaseFactory);

        given(databaseFactory.getArangoDriver()).willReturn(arangoDriver);
    }

    @Test
    public void testExecuteSupplyChainCmdWithException() throws Exception {
        givenASupplyChainCmd("db","start", "graph", "vertex");
        givenArangoDriverWillThrowException();

        whenExecuteSupplyChainCmd();

        thenSupplyChainExecutorResultIsEmpty();
    }

    @Test
    public void testExecuteSupplyChainCmdConsumerResults() throws Exception {
        final List<ServiceEntityRepoDTO> pmInstances = fill(5, PM_TYPE);
        List<SupplyChainQueryResult> consumerQueryResults = Collections.singletonList(
                supplyChainQueryResultFor(PM_TYPE, pmInstances, Collections.emptyList()));

        givenASupplyChainCmd("db","start", "graph", "vertex");
        givenArangoDriverWillThrowException();
        givenSupplyChainQueryResults(Collections.emptyList(), consumerQueryResults);
        whenExecuteSupplyChainCmd();
        thenSupplyChainExecutorResults(Collections.emptyList(), consumerQueryResults);
    }

    @Test
    public void testExecuteSupplyChainCmd() throws Exception {
        final List<ServiceEntityRepoDTO> pmInstances = fill(5, PM_TYPE);
        final List<ServiceEntityRepoDTO> vmInstances = fill(5, VM_TYPE);
        final List<ServiceEntityRepoDTO> dcInstances = fill(2, DC_TYPE);

        List<SupplyChainQueryResult> consumerQueryResults = Lists.newArrayList(
            supplyChainQueryResultFor(PM_TYPE, pmInstances, vmInstances)
        );

        List<SupplyChainQueryResult> providerQueryResults = Lists.newArrayList(
            supplyChainQueryResultFor(PM_TYPE, pmInstances, dcInstances)
        );

        givenASupplyChainCmd("db","start", "graph", "vertex");
        givenSupplyChainQueryResults(providerQueryResults, consumerQueryResults);

        whenExecuteSupplyChainCmd();

        thenSupplyChainExecutorResults(providerQueryResults, consumerQueryResults);
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
        final List<ServiceEntityRepoDTO> mockResults = fill(5, VM_TYPE);

        givenASearchEntityCmd("collection", "field", "query", GraphCmd.SearchType.FULLTEXT);
        givenSearchServiceEntityResults(mockResults);

        whenSearchServiceEntity();

        thenSearchServiceEntityResults(mockResults);
    }

    @Test
    public void testSearchServiceEntityNonFullText() throws Exception {
        final List<ServiceEntityRepoDTO> mockResults = fill(5, VM_TYPE);

        givenASearchEntityCmd("collection", "uuid", "10", GraphCmd.SearchType.STRING);
        givenSearchServiceEntityResults(mockResults);

        whenSearchServiceEntity();

        thenSearchServiceEntityResults(mockResults);
    }

    private void givenASearchEntityCmd(final String collection,
                                       final String field,
                                       final String query,
                                       final GraphCmd.SearchType searchType) {
        final TopologyDatabase topologyDatabase = TopologyDatabase.from("db-foo");
        graphCmd = new GraphCmd.SearchServiceEntity(collection, field, query, searchType,
                                                    topologyDatabase);
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

    private void givenASupplyChainCmd(final String databaseName,
                                      final String starting,
                                      final String graphName,
                                      final String vertexColl) {
        graphCmd = new GraphCmd.GetSupplyChain(starting, TopologyDatabase.from(databaseName),graphName, vertexColl);
    }

    @SuppressWarnings("unchecked")
    private void givenSupplyChainQueryResults(final List<SupplyChainQueryResult> providerResults,
                                              final List<SupplyChainQueryResult> consumerResults) throws Exception {

        final ArangoCursor<SupplyChainQueryResult> providerCursor = Mockito.mock(ArangoCursor.class);
        final ArangoCursor<SupplyChainQueryResult> consumerCursor = Mockito.mock(ArangoCursor.class);

        given(providerCursor.asListRemaining()).willReturn(providerResults);
        given(consumerCursor.asListRemaining()).willReturn(consumerResults);

        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);

        when(mockDatabase.query(anyString(), anyMap(), any(AqlQueryOptions.class),
                                          Matchers.<Class<SupplyChainQueryResult>>any()))
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
        supplyChainExecutorResult = arangoDBExecutor.executeSupplyChainCmd((GraphCmd.GetSupplyChain) graphCmd);
    }

    private void thenSupplyChainExecutorResultIsEmpty() {
        assertThat(supplyChainExecutorResult).isEmpty();
    }

    private void thenSupplyChainExecutorResults(final List<SupplyChainQueryResult> expectedProviderResults,
                                                final List<SupplyChainQueryResult> expectedConsumerResults) {
        assertThat(supplyChainExecutorResult).isNotEmpty();
        assertThat(supplyChainExecutorResult.get().getConsumers()).hasSameElementsAs(expectedConsumerResults);
        assertThat(supplyChainExecutorResult.get().getProviders()).hasSameElementsAs(expectedProviderResults);
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