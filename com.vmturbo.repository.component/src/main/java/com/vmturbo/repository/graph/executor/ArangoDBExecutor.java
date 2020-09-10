package com.vmturbo.repository.graph.executor;

import static com.google.common.base.Preconditions.checkNotNull;

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

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import javaslang.collection.Seq;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.OptScope;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.parameter.GraphCmd.ServiceEntityMultiGet;
import com.vmturbo.repository.graph.parameter.GraphCmd.SupplyChainDirection;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;
import com.vmturbo.repository.topology.GlobalSupplyChainRelationships;

public class ArangoDBExecutor implements GraphDBExecutor {

    /**
     * Collection name of global supply chain provider relationships.
     */
    public static final String GLOBAL_SUPPLY_CHAIN_RELS_COLLECTION = "globalSCProviderRels";

    /**
     * Collection name of global supply chain entities info.
     */
    public static final String GLOBAL_SUPPLY_CHAIN_ENTITIES_COLLECTION = "globalSCEntitiesInfo";

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBExecutor.class);

    private final ArangoDatabaseFactory arangoDatabaseFactory;

    private final String arangoDatabaseName;

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

    /**
     * Constructor of ArangoDBExecutor.
     *
     * @param arangoDatabaseFactoryArg Given {@link ArangoDatabaseFactory} where we can get the driver
     *                                 to connection to db.
     * @param arangoDatabaseName       Given Arango database name.
     */
    public ArangoDBExecutor(final ArangoDatabaseFactory arangoDatabaseFactoryArg,
                            final String arangoDatabaseName) {
        arangoDatabaseFactory = checkNotNull(arangoDatabaseFactoryArg);
        this.arangoDatabaseName = checkNotNull(arangoDatabaseName);
    }

    /**
     * Derive a supply chain query based on the input parameters.
     *
     * @param direction Either a CONSUMER or PROVIDER. This value determines whether the query will
     *                  traverse inbound edges or outbound edges of the starting vertex.
     * @return An AQL query.
     */
    private static String getSupplyChainQuery(final SupplyChainDirection direction,
                                              final GraphCmd.GetSupplyChain supplyChainCmd) {

        final String vertexCollection = supplyChainCmd.getVertexCollection();
        final String startingId = Joiner.on("/").join(vertexCollection, supplyChainCmd.getStartingVertex());

        final ST template;
        if (direction == SupplyChainDirection.CONSUMER) {
            template = new ST(ArangoDBQueries.SUPPLY_CHAIN_CONSUMER_QUERY_TEMPLATE);
        } else if (direction == SupplyChainDirection.PROVIDER) {
            template = new ST(ArangoDBQueries.SUPPLY_CHAIN_PROVIDER_QUERY_TEMPLATE);
        } else {
            throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        template.add("edgeCollection", supplyChainCmd.getGraphName())
                .add("startingId", startingId)
                .add("vertexCollection", vertexCollection)
                .add("edgeType", direction.getEdgeType())
                .add("hasEnvType", supplyChainCmd.getEnvironmentType().isPresent());
        supplyChainCmd.getEnvironmentType().ifPresent(envType ->
                template.add("envType", EnvironmentTypeUtil.toApiString(envType)));

        supplyChainCmd.getEntityAccessScope().ifPresent(entityAccessScope -> {
            // add an accessible oids list if the access scope is restricted
            if (entityAccessScope.hasRestrictions()) {
                template.add("allowedOidList", entityAccessScope.accessibleOids().iterator());
            }
        });

        // set the "hasAllowedOidList" attribute based on if we populated a list or not.
        template.add("hasAllowedOidList", template.getAttribute("allowedOidList") != null);

        // filter the path by the inclusion entity types, which means it will traverse the path
        // if and only if the entity types in this path contains the provided set
        final Set<Integer> inclusionEntityTypes = supplyChainCmd.getInclusionEntityTypes();
        if (!inclusionEntityTypes.isEmpty()) {
            template.add("hasInclusionEntityTypes", true);
            template.add("inclusionEntityTypes", entityTypesListToAQL(inclusionEntityTypes));
        } else {
            template.add("hasInclusionEntityTypes", false);
        }

        // filter the path by the exclusion entity types, which means it will not traverse the
        // path if the path contains entities of any type within the provided set
        final Set<Integer> exclusionEntityTypes = supplyChainCmd.getExclusionEntityTypes();
        if (!exclusionEntityTypes.isEmpty()) {
            template.add("hasExclusionEntityTypes", true);
            template.add("exclusionEntityTypes", entityTypesListToAQL(exclusionEntityTypes));
        } else {
            template.add("hasExclusionEntityTypes", false);
        }

        return template.render();
    }

    /**
     * Convert the given set of integers into the AQL string list representation.
     *
     * @param entityTypes the set of entity types to convert
     * @return AQL string list representation of given set
     */
    private static String entityTypesListToAQL(@Nonnull Set<Integer> entityTypes) {
        return "[" + entityTypes.stream()
            .map(ApiEntityType::fromSdkTypeToEntityTypeString)
            .map(entityType -> "\"" + entityType + "\"")
            .collect(Collectors.joining(",")) + "]";
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
        try (OptScope scope = Tracing.childOfActiveSpan("executeSupplyChainCmd")) {
            final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
            final String providerQuery = getSupplyChainQuery(SupplyChainDirection.PROVIDER, supplyChainCmd);
            final String consumerQuery = getSupplyChainQuery(SupplyChainDirection.CONSUMER, supplyChainCmd);

            final DataMetricTimer timer = SINGLE_SOURCE_SUPPLY_CHAIN_QUERY_DURATION_SUMMARY.startTimer();

            logger.debug("Supply chain provider query {}", providerQuery);
            logger.debug("Supply chain consumer query {}", consumerQuery);

            final Try<Seq<ArangoCursor<ResultVertex>>> combinedResults = Try.sequence(ImmutableList.of(
                Try.of(() -> driver.db(arangoDatabaseName).query(providerQuery, null, null, ResultVertex.class)),
                Try.of(() -> driver.db(arangoDatabaseName).query(consumerQuery, null, null, ResultVertex.class))));
            timer.observe();

            combinedResults.onFailure(logAQLException(Joiner.on("\n").join(providerQuery, consumerQuery)));

            return combinedResults.flatMap(results -> {

                final List<ResultVertex> providerResults =
                    fetchAllResults(results.get(0));
                final List<ResultVertex> consumerResults =
                    fetchAllResults(results.get(1));

                return (!providerResults.isEmpty() && !consumerResults.isEmpty())
                    ? Try.success(new SupplyChainSubgraph(providerResults, consumerResults))
                    : Try.failure(new NoSuchElementException("Entity " + supplyChainCmd.getStartingVertex() + " not found."));
            });
        }
    }

    public  void insertNewDocument(final @Nonnull BaseDocument newDocument,
                                   String collection,
                                   DocumentCreateOptions documentCreateOptions)
            throws ArangoDBException {

        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        driver.db(arangoDatabaseName).collection(collection).insertDocument(newDocument, documentCreateOptions);
    }

    public <T> void insertNewDocument(final T doc,
                                      String collection,
                                      DocumentCreateOptions documentCreateOptions)
            throws ArangoDBException {

        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        driver.db(arangoDatabaseName).collection(collection).insertDocument(doc,
                documentCreateOptions);
    }

    public BaseDocument getDocument(final String key,
                                    String collection)
            throws ArangoDBException {

        final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
        return driver.db(arangoDatabaseName).collection(collection).getDocument(key, BaseDocument.class);
    }

    @Override
    public GlobalSupplyChainRelationships getSupplyChainRels() {
        logger.debug("Get supply chain relationship query {} for database {}", ArangoDBQueries.GET_SUPPLY_CHAIN_RELS,
            arangoDatabaseName);
        final List<BaseDocument> results =
            arangoDatabaseFactory.getArangoDriver().db(arangoDatabaseName).query(ArangoDBQueries.GET_SUPPLY_CHAIN_RELS, null, null,
                BaseDocument.class).asListRemaining();
        return new GlobalSupplyChainRelationships(results);
    }

    @Override
    public Try<Collection<ServiceEntityRepoDTO>> executeSearchServiceEntityCmd(final GraphCmd.SearchServiceEntity searchServiceEntity) {
        try (OptScope scope = Tracing.childOfActiveSpan("executeSearchServiceEntityCmd")) {
            final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
            final String searchQuery = searchServiceEntitytQuery(searchServiceEntity);
            final DataMetricTimer timer = SEARCH_QUERY_DURATION_SUMMARY.startTimer();

            logger.debug("Service entity search query {}", searchQuery);

            final Try<ArangoCursor<ServiceEntityRepoDTO>> results =
                Try.of(() -> driver.db(arangoDatabaseName).query(searchQuery, null, null, ServiceEntityRepoDTO.class));
            timer.observe();

            results.onFailure(logAQLException(searchQuery));

            return results.map(this::fetchAllResults);
        }
    }

    @Override
    @Nonnull
    public Try<Collection<ServiceEntityRepoDTO>> executeServiceEntityMultiGetCmd(
            @Nonnull final ServiceEntityMultiGet serviceEntityMultiGet) {
        try (final OptScope scope = Tracing.childOfActiveSpan("executeServiceEntityMultiGetCmd")) {
            final ArangoDB driver = arangoDatabaseFactory.getArangoDriver();
            final StrSubstitutor querySubstitutor;
            final String searchQuery;

            // if entityIDs is empty, return all entities.
            if (serviceEntityMultiGet.getEntityIds().size() == 0) {
                querySubstitutor = new StrSubstitutor(ImmutableMap.of(
                    "collection",
                    "`" + serviceEntityMultiGet.getCollection() + "`"));
                searchQuery = querySubstitutor.replace(ArangoDBQueries.GET_ALL_ENTITIES);
            } else {
                // replace the "commaSepLongs" token with the list of entity ids, e.g. "1","2","3"
                querySubstitutor = new StrSubstitutor(ImmutableMap.of(
                    "collection",
                    "`" + serviceEntityMultiGet.getCollection() + "`",
                    "commaSepLongs",
                    serviceEntityMultiGet.getEntityIds().stream()
                                .map(id -> "\"" + Long.toString(id) + "\"")
                                .collect(Collectors.joining(","))));
                searchQuery = querySubstitutor.replace(ArangoDBQueries.GET_ENTITIES_BY_OID);
            }
            final DataMetricTimer timer = SEARCH_QUERY_DURATION_SUMMARY.startTimer();

            logger.debug("Service entity multi-get query {} for database {}", searchQuery, arangoDatabaseName);

            final Try<ArangoCursor<ServiceEntityRepoDTO>> results =
                Try.of(() -> driver.db(arangoDatabaseName).query(searchQuery, null, null, ServiceEntityRepoDTO.class));
            timer.observe();

            results.onFailure(logAQLException(searchQuery));

            return results.map(this::fetchAllResults);
        }
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
            @Nonnull String vertexCollection,
            @Nonnull SearchTagsRequest request) throws ArangoDBException {
        try (OptScope scope = Tracing.childOfActiveSpan("executeTagCommand")) {
            final Collection<Long> entityOids = request.getEntitiesList();
            final EnvironmentTypeEnum.EnvironmentType environmentType =
                request.hasEnvironmentType() ? request.getEnvironmentType() : null;

            // construct AQL query
            final StringBuilder queryBuilder =
                    new StringBuilder(String.format("FOR service_entity IN %s\n", vertexCollection));
            if (request.hasEntityType()) {
                queryBuilder
                    .append("FILTER service_entity.entityType == \"")
                    .append(ApiEntityType.fromType(request.getEntityType()))
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
                    final List<String> values = (List<String>) e.getValue();
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

    /**
     * Get Arango database name where the query command will be executed.
     *
     * @return Arango database name where the query command will be executed.
     */
    public String getArangoDatabaseName() {
        return arangoDatabaseName;
    }

    /**
     * Get full collection name based on given collection name and collection name suffix.
     *
     * @param collection Given collection name.
     * @param collectionNameSuffix Given collection name suffix from TopologyID.
     * @return Constructed full collection name.
     */
    public String fullCollectionName(String collection, String collectionNameSuffix) {
        return collection + collectionNameSuffix;
    }
}
