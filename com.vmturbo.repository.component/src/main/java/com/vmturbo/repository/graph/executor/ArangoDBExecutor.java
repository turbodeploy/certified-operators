package com.vmturbo.repository.graph.executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static javaslang.API.Case;
import static javaslang.API.Match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javaslang.collection.Seq;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.TagValuesDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.constant.RepoObjectType;
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
    private static final Logger logger = LoggerFactory.getLogger(ArangoDBExecutor.class);

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

            final List<SubgraphResult> providerResults =
                fetchAllResults(results.get(0));
            final List<SubgraphResult> consumerResults =
                fetchAllResults(results.get(1));

            return hasOneNonnullResult(providerResults) && hasOneNonnullResult(consumerResults)
                ? Try.success(new SupplyChainSubgraph(providerResults.get(0), consumerResults.get(0)))
                : Try.failure(new NoSuchElementException("Entity " + supplyChainCmd.getStartingVertex() + " not found."));
        });
    }

    private boolean hasOneNonnullResult(@Nonnull final List<SubgraphResult> resultList) {
        return (resultList.size() == 1) && (resultList.get(0).getOrigin().getId() != null);
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

        return results.map(this::fetchAllResults);
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

        return results.map(this::fetchAllResults);
    }


    private Consumer<? super Throwable> logAQLException(final String query) {
        return exc -> logger.error("Exception encountered while executing AQL query: " + query, exc);
    }

    /**
     *  Fetch all the results from ArangoDB.
     *
     *  @param cursor ArangoDB cursor object.
     *  @return The list of results.
     */
    private <T> List<T> fetchAllResults(@Nonnull ArangoCursor<T> cursor) {
        // We don't explicitly close the cursor because the server auto-closes it once the
        // cursor is out of results.
        return cursor.asListRemaining();
    }

    /**
     * Fetches tags from the database.  Currently, no pagination is supported, for simplicity.
     *
     * @param databaseName name of the current live database.
     * @param request the RPC request.
     * @throws ArangoDBException for any problem communicating with the database.
     */
    @Override
    @Nonnull
    public Map<String, TagValuesDTO> executeTagCommand(
            @Nonnull String databaseName,
            @Nonnull SearchTagsRequest request) throws ArangoDBException {
        final Collection<Long> entityOids = request.getEntitiesList();
        final EnvironmentTypeEnum.EnvironmentType environmentType =
                request.hasEnvironmentType() ? request.getEnvironmentType() : null;

        // construct AQL query
        final StringBuilder queryBuilder =
                new StringBuilder("FOR service_entity IN seVertexCollection\n");
        if (request.hasEntityType()) {
            queryBuilder
                    .append("FILTER service_entity.entityType == \"")
                    .append(RepoObjectType.mapEntityType(request.getEntityType()))
                    .append("\"\n");
        }
        if (entityOids != null && !entityOids.isEmpty()) {
            queryBuilder.append("FILTER service_entity.oid IN [");
            queryBuilder.append(
                    entityOids
                            .stream()
                            .map(x -> "\"" + x.toString() + "\"")
                            .collect(Collectors.joining(", ")));
            queryBuilder.append("]\n");
        }
        if (environmentType != null) {
            // TODO: add filter for environment type (ONPREM, CLOUD, HYBRID) when it is implemented
        }
        queryBuilder.append("FILTER LENGTH(ATTRIBUTES(service_entity.tags)) > 0\n");
        queryBuilder.append("RETURN service_entity.tags");
        final String query = queryBuilder.toString();
        logger.info("AQL query constructed:\n {}\n", query);

        // execute query
        final ArangoCursor<BaseDocument> resultCursor =
                arangoDatabaseFactory.getArangoDriver().db(databaseName).query(
                    query, null, new AqlQueryOptions(), BaseDocument.class);

        // convert results to a map from strings (key) to sets of strings (values)
        // using sets here prevents duplicate key/value pairs
        final Map<String, Set<String>> resultWithSetsOfValues = new HashMap<>();
        while (resultCursor.hasNext()) {
            final BaseDocument tags = resultCursor.next();
            for (Entry<String, Object> e : tags.getProperties().entrySet()) {
                final String key = e.getKey();
                final List<String> values = (List<String>)e.getValue();
                resultWithSetsOfValues.merge(
                        key,
                        new HashSet<>(values),
                        (vs1, vs2) -> {
                            vs1.addAll(vs2);
                            return vs1;
                        });
            }
        }

        // convert the result to a map from strings (key) to a TagValuesDTO object (values)
        final Map<String, TagValuesDTO> result = new HashMap<>();
        for (Entry<String, Set<String>> e : resultWithSetsOfValues.entrySet()) {
            result.put(e.getKey(), TagValuesDTO.newBuilder().addAllValues(e.getValue()).build());
        }

        return result;
    }
}
