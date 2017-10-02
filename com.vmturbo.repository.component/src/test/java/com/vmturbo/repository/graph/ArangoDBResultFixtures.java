package com.vmturbo.repository.graph;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlQueryOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyDatabases;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

public class ArangoDBResultFixtures {
    public static final String TEST_DATABASE_NAME = "test-database";
    public static final String TEST_COLLECTION = "test-collection";
    public static final String TEST_GRAPH = "test-graph";
    public static final TopologyDatabase TEST_DATABASE = TopologyDatabase.from(TEST_DATABASE_NAME);

    /**
     * Stub the calls to {@link ArangoDatabase#query(String, Map, AqlQueryOptions, Class)} with results.
     *
     * @param mockFactory The mock instance of {@link ArangoDatabaseFactory}.
     * @param topologyDatabase The database we are going to query.
     * @param cursorTestData A collection test data.
     * @param objectMapper An {@link ObjectMapper}.
     * @param <DATA> The type of the test data.
     */
    @SuppressWarnings("unchecked")
    public static <DATA> void givenArangoDBQueryWillReturnResults(
            final ArangoDatabaseFactory mockFactory,
            final TopologyDatabase topologyDatabase,
            final Collection<ArangoDBCursorTestData<DATA>> cursorTestData,
            final ObjectMapper objectMapper) {

        final ArangoDB mockArangoDB = Mockito.mock(ArangoDB.class);
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final String databaseName = TopologyDatabases.getDbName(topologyDatabase);

        given(mockFactory.getArangoDriver()).willReturn(mockArangoDB);
        given(mockArangoDB.db(databaseName)).willReturn(mockDatabase);

        cursorTestData.forEach(testData -> {
            try {
                final ArangoCursor<String> mockCursor = (ArangoCursor<String>) Mockito.mock(ArangoCursor.class);
                given(mockDatabase.query(anyString(), Mockito.anyMapOf(String.class, Object.class) , any(), eq(String.class)))
                        .willReturn(mockCursor);
                testData.jsonData(objectMapper).foldLeft(given(mockCursor.next()), BDDMockito.BDDMyOngoingStubbing::willReturn);
                testData.data().foldLeft(given(mockCursor.hasNext()), (stub, e) -> stub.willReturn(true)).willReturn(false);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Stub the calls to {@link ArangoDatabase#query(String, Map, AqlQueryOptions, Class)} to throw an exception.
     *
     * @param arangoDB An mock {@link ArangoDB}.
     * @param exception The exception we want to throw.
     */
    public static void givenArangoDBWillThrowException(final ArangoDB arangoDB, final Throwable exception) {
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final ArangoCursor mockCursor = Mockito.mock(ArangoCursor.class);

        given(arangoDB.db(TEST_DATABASE_NAME)).willReturn(mockDatabase);
        given(mockDatabase.query(anyString(), Mockito.anyMapOf(String.class, Object.class), any(), any())).willReturn(mockCursor);

        given(mockCursor.hasNext()).willThrow(exception);
    }

    /**
     * Stub supply chain query calls with test data.
     * <p>
     * A single-source supply chain is comprised of three queries, provider, consumer, and origin.
     *
     * @param arangoDatabaseFactory The mock {@link ArangoDB}.
     * @param topologyDatabase      The database we want to mock.
     * @param providerTestData      The test data for the provider query.
     * @param consumerTestData      The test data for the consumer query.
     * @param originTestData        The test data for the starting point.
     * @param objectMapper          An instance of {@link ObjectMapper}.
     * @param <ORIGIN>              The type of the origin query.
     * @param <SUPPLYCHAIN>         The type of the provider and consumer queries.
     * @throws JsonProcessingException
     */
    @SuppressWarnings("unchecked")
    public static <ORIGIN, SUPPLYCHAIN> void givenSupplyCahinQueryWillReturnResults(
            final ArangoDatabaseFactory arangoDatabaseFactory,
            final TopologyDatabase topologyDatabase,
            final ArangoDBCursorTestData<SUPPLYCHAIN> providerTestData,
            final ArangoDBCursorTestData<SUPPLYCHAIN> consumerTestData,
            final ArangoDBCursorTestData<ORIGIN> originTestData,
            final ObjectMapper objectMapper) throws JsonProcessingException {

        final ArangoDB mockArangoDB = Mockito.mock(ArangoDB.class);
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        final ArangoCursor<String> providerCursor = (ArangoCursor<String>) Mockito.mock(ArangoCursor.class);
        final ArangoCursor<String> consumerCursor = (ArangoCursor<String>) Mockito.mock(ArangoCursor.class);
        final ArangoCursor<String> originCursor = (ArangoCursor<String>) Mockito.mock(ArangoCursor.class);
        final String databaseName = TopologyDatabases.getDbName(topologyDatabase);

        given(arangoDatabaseFactory.getArangoDriver()).willReturn(mockArangoDB);
        given(mockArangoDB.db(databaseName)).willReturn(mockDatabase);
        given(mockDatabase.query(anyString(), Mockito.anyMapOf(String.class, Object.class), any(), eq(String.class)))
                .willReturn(providerCursor).willReturn(consumerCursor).willReturn(originCursor);

        providerTestData.jsonData(objectMapper).foldLeft(given(providerCursor.next()), BDDMockito.BDDMyOngoingStubbing::willReturn);
        providerTestData.data().foldLeft(given(providerCursor.hasNext()), (stubbing, e) -> stubbing.willReturn(true)).willReturn(false);

        consumerTestData.jsonData(objectMapper).foldLeft(given(consumerCursor.next()), BDDMockito.BDDMyOngoingStubbing::willReturn);
        consumerTestData.data().foldLeft(given(consumerCursor.hasNext()), (stubbing, e) -> stubbing.willReturn(true)).willReturn(false);

        originTestData.jsonData(objectMapper).foldLeft(given(originCursor.next()), BDDMockito.BDDMyOngoingStubbing::willReturn);
        originTestData.data().foldLeft(given(originCursor.hasNext()), (stubbing, e) -> stubbing.willReturn(true)).willReturn(false);
    }
}
