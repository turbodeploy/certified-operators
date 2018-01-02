package com.vmturbo.repository.graph.executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static javaslang.API.Case;
import static javaslang.API.Match;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javaslang.collection.Seq;
import javaslang.control.Try;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.parameter.GraphCmd.ServiceEntityMultiGet;
import com.vmturbo.repository.graph.parameter.GraphCmd.SupplyChainDirection;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.SubgraphResult;
import com.vmturbo.repository.topology.TopologyDatabases;

public class ArangoDBExecutor implements GraphDBExecutor {

    // TODO: Temporary place holder for topology database name.
    public static final String DEFAULT_PLACEHOLDER_DATABASE = "";
    private final Logger logger = LoggerFactory.getLogger(ArangoDBExecutor.class);

    private final ArangoDatabaseFactory arangoDatabaseFactory;

    private static final DataMetricSummary SINGLE_SOURCE_SUPPLY_CHAIN_QUERY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_single_source_supply_chain_query_duration_seconds")
        .withHelp("Duration in seconds it takes repository to execute a single source supply chain query.")
        .build()
        .register();
    private static final DataMetricSummary GLOBAL_SUPPLY_CHAIN_QUERY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_global_supply_chain_query_duration_seconds")
        .withHelp("Duration in seconds it takes repository to execute a global supply chain query.")
        .build()
        .register();
    private static final DataMetricSummary SEARCH_QUERY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_search_query_duration_seconds")
        .withHelp("Duration in seconds it takes repository to execute a search query.")
        .build()
        .register();

    public ArangoDBExecutor(final ArangoDatabaseFactory arangoDatabaseFactoryArg) {
        arangoDatabaseFactory = checkNotNull(arangoDatabaseFactoryArg);
    }

    /**
     * Derive a supply chain query based on the input parameters.
     *
     * @param direction Either a CONSUMER or PROVIDER. This value determines whether the query will
     *                  traverse inbound edges or outbound edges of the starting vertex.
     * @param startingVertex The origin of the supply chain. The value is the ID of the service entity.
     * @param graphName The name of graph with the service entity graph inside ArangoDB.
     * @param vertexCollection The collection where all the service entity documents reside.
     * @return An AQL query.
     */
    private static String getSupplyChainQuery(final SupplyChainDirection direction,
                                              final String startingVertex,
                                              final String graphName,
                                              final String vertexCollection) {

        final String startingId = Joiner.on("/").join(vertexCollection, startingVertex);

        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                                                              .put("edgeCollection", graphName)
                                                              .put("startingId", startingId)
                                                              .put("vertexCollection", vertexCollection)
                                                              .build();

        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        return Match(direction).of(
                Case(SupplyChainDirection.PROVIDER,
                    substitutor.replace(ArangoDBQueries.SUPPLY_CHAIN_PROVIDER_QUERY_STRING)),
                Case(SupplyChainDirection.CONSUMER,
                    substitutor.replace(ArangoDBQueries.SUPPLY_CHAIN_CONSUMER_QUERY_STRING))
        );
    }

    private static String staticGlobalSupplyChainQuery(final GraphCmd.GetGlobalSupplyChain cmd) {
        final Set<String> types = new HashSet<>();
        types.addAll(cmd.getProviderStructure().values());
        types.addAll(cmd.getProviderStructure().keySet());

        final Map<String, String> valueMap = new ImmutableMap.Builder<String, String>()
                .put("types", types.stream().map(t -> "\"" + t + "\"").collect(Collectors.joining(", ")))
                .put("vertexCollection", cmd.getVertexCollection())
                .build();
        final StrSubstitutor substitutor = new StrSubstitutor(valueMap);

        return substitutor.replace(ArangoDBQueries.INSTANCES_OF_TYPES_QUERY_STRING);
    }

    private static String getSupplyChainQuery(final SupplyChainDirection direction,
                                      final GraphCmd.GetSupplyChain supplyChainCmd) {
        return getSupplyChainQuery(direction,
                                   supplyChainCmd.getStartingVertex(),
                                   supplyChainCmd.getGraphName(),
                                   supplyChainCmd.getVertexCollection());
    }

    static String searchServiceEntitytQuery(final GraphCmd.SearchServiceEntity searchCmd) {

        String matchValue = "";

        switch (searchCmd.getSearchType()) {
            case FULLTEXT:
                matchValue = searchCmd.getQuery();
                break;
            case STRING:
                matchValue = String.format("'%s'", searchCmd.getQuery());
                break;
            case NUMERIC:
                matchValue = searchCmd.getQuery();
                break;
        }

        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                                                              .put("collection", searchCmd.getCollection())
                                                              .put("field", searchCmd.getField())
                                                              .put("query", matchValue)
                                                              .put("value", matchValue)
                                                              .build();

        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        if (searchCmd.getSearchType() == GraphCmd.SearchType.FULLTEXT) {
            return substitutor.replace(ArangoDBQueries.SEARCH_SERVICE_ENTITY_FULLTEXT_QUERY_STRING);
        } else {
            return substitutor.replace(ArangoDBQueries.SEARCH_SERVICE_ENTITY_QUERY_STRING);
        }
    }

    @Override
    public Try<SupplyChainSubgraph> executeSupplyChainCmd(final GraphCmd.GetSupplyChain supplyChainCmd) {
        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        final String providerQuery = getSupplyChainQuery(SupplyChainDirection.PROVIDER, supplyChainCmd);
        final String consumerQuery = getSupplyChainQuery(SupplyChainDirection.CONSUMER, supplyChainCmd);

        final String databaseName = TopologyDatabases.getDbName(supplyChainCmd.getTopologyDatabase());
        final DataMetricTimer timer = SINGLE_SOURCE_SUPPLY_CHAIN_QUERY_DURATION_SUMMARY.startTimer();

        logger.debug("Supply chain provider query {}", providerQuery);
        logger.debug("Supply chain consumer query {}", consumerQuery);

        final Try<Seq<ArangoCursor<SubgraphResult>>> combinedResults = Try.sequence(ImmutableList.of(
            Try.of(() -> driver.db(databaseName).query(providerQuery, null, null, SubgraphResult.class))
            , Try.of(() -> driver.db(databaseName).query(consumerQuery, null, null, SubgraphResult.class))));
        timer.observe();

        combinedResults.onFailure(logAQLException(Joiner.on("\n").join(providerQuery, consumerQuery)));

        return combinedResults.flatMap(results -> {
            final List<SubgraphResult> providerResults = results.get(0).asListRemaining();
            final List<SubgraphResult> consumerResults = results.get(1).asListRemaining();

            return providerResults.size() == 1 && consumerResults.size() == 1
                ? Try.success(new SupplyChainSubgraph(providerResults.get(0), consumerResults.get(0)))
                : Try.failure(new NoSuchElementException("Entity " + supplyChainCmd.getStartingVertex() + " not found."));
        });
    }

    @Override
    public Try<Collection<ServiceEntityRepoDTO>> executeSearchServiceEntityCmd(final GraphCmd.SearchServiceEntity searchServiceEntity) {
        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        final String searchQuery = searchServiceEntitytQuery(searchServiceEntity);
        final String databaseName = TopologyDatabases.getDbName(searchServiceEntity.getTopologyDatabase());
        final DataMetricTimer timer = SEARCH_QUERY_DURATION_SUMMARY.startTimer();

        logger.debug("Service entity search query {}", searchQuery);

        final Try<ArangoCursor<ServiceEntityRepoDTO>> results =
            Try.of(() -> driver.db(databaseName).query(searchQuery, null, null, ServiceEntityRepoDTO.class));
        timer.observe();

        results.onFailure(logAQLException(searchQuery));

        return results.map(ArangoCursor::asListRemaining);
    }

    @Override
    @Nonnull
    public Try<Collection<ServiceEntityRepoDTO>> executeServiceEntityMultiGetCmd(
            @Nonnull final ServiceEntityMultiGet serviceEntityMultiGet) {

        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        final StrSubstitutor querySubstitutor = new StrSubstitutor(ImmutableMap.of(
                "collection",
                serviceEntityMultiGet.getCollection(),
                "commaSepLongs",
                serviceEntityMultiGet.getEntityIds().stream()
                        .map(id -> Long.toString(id))
                        .collect(Collectors.joining(","))));

        final String searchQuery = querySubstitutor.replace(ArangoDBQueries.GET_ENTITIES_BY_OID);
        final String databaseName = TopologyDatabases.getDbName(serviceEntityMultiGet.getTopologyDatabase());
        final DataMetricTimer timer = SEARCH_QUERY_DURATION_SUMMARY.startTimer();

        logger.debug("Service entity multi-get query {} for database {}", searchQuery, databaseName);

        final Try<ArangoCursor<ServiceEntityRepoDTO>> results =
            Try.of(() -> driver.db(databaseName).query(searchQuery,null, null, ServiceEntityRepoDTO.class));
        timer.observe();

        results.onFailure(logAQLException(searchQuery));

        return results.map(ArangoCursor::asListRemaining);
    }


    private Consumer<? super Throwable> logAQLException(final String query) {
        return exc -> logger.error("Exception encountered while executing AQL query: " + query, exc);
    }
}
