package com.vmturbo.repository.search;

import static com.vmturbo.repository.search.SearchTestUtil.makeRegexStringFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Ignore;
import org.junit.Test;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javaslang.collection.List;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.operator.GraphCreatorFixture;

/**
 * This class <b>MUST NOT</b> be run during normal build.
 */
public class SearchIntegration {

    private final ExecutorService executorService = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("search-aql-%d")
                    .build());

    private final DataMetricSummary SEARCH_INTEGRATION_SUMMARY = DataMetricSummary.builder()
        .withName("search_computation_duration_seconds")
        .withHelp("summary for search integration timing")
        .build()
        .register();

    private final String SERVICE_ENTITY_VERTEX = "seVertexCollection";

    @Test
    @Ignore
    public void overallFlow() {
        final AQLRepr repr1 = new AQLRepr(List.of(Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeRegexStringFilter("DataCenter", true))
                        .build())));
        final AQLRepr repr2 = new AQLRepr(List.of(
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1)));
        final AQLRepr repr3 = new AQLRepr(List.of(Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeRegexStringFilter("20", false))
                        .build())));
        final SearchStage<SearchComputationContext, Collection<String>, Collection<String>> s1 =
                () -> new ArangoDBSearchComputation(repr1.toAQL());
        final SearchStage<SearchComputationContext, Collection<String>, Collection<String>> s2 =
                () -> new ArangoDBSearchComputation(repr2.toAQL());
        final SearchStage<SearchComputationContext, Collection<String>, Collection<String>> s3 =
                () -> new ArangoDBSearchComputation(repr3.toAQL());

        final SearchStage<SearchComputationContext, Collection<String>, Collection<String>> combined = s1.andThen(s2).andThen(s3);
        final SearchPipeline<SearchComputationContext> pipeline =
                new SearchPipeline<>(Arrays.asList(s1, s2, s3), SERVICE_ENTITY_VERTEX);

        final SearchComputationContext context = ImmutableSearchComputationContext.builder()
                .summary(SEARCH_INTEGRATION_SUMMARY)
                .graphName("seGraph")
                .arangoDB(new ArangoDB.Builder()
                        .port(8600)
                        .user("root")
                        .password("root")
                        .build())
                .databaseName("topology-777777-71776130873552")
                .entityCollectionName("seVertexCollection")
                .allKeyword("ALL")
                .executorService(executorService)
                .build();

        // Run directly.
        final Collection<String> results = combined.run(context, Collections.singleton("ALL"));
        System.out.println("====> Num results: " + results.size());
        System.out.println("====> Results: " + results);

        // Run via Pipeline
        final Either<Throwable, java.util.List<String>> pipelineResults =
                pipeline.run(context, Collections.emptyList());
        System.out.println("====> Pipeline Results: " + pipelineResults);

        // Run via Pipeline with a function.
        final Either<Throwable, java.util.List<ServiceEntityRepoDTO>> pipelineWithEntities =
                pipeline.run(context, ArangoDBSearchComputation.toEntities, Collections.emptyList());
        System.out.println("====> Pipeline with entities: " + pipelineWithEntities);
    }

    @Test
    @Ignore
    public void testOverallSearchParameters() throws Throwable {
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter("VirtualMachine"))
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(
                        displayNameFilter("5")).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(
                        traversalCondFilter(Search.TraversalFilter.TraversalDirection.CONSUMES,
                                            entityTypeFilter("Storage"))).build())
                .build();

        // SearchParameters to AQLRepr's
        final java.util.List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);

        // Fuser
        final java.util.List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs, Optional.empty());

        // AQLs
        final Stream<AQL> aqlStream = fusedAQLReprs.stream().map(AQLRepr::toAQL);

        // Stages
        final Stream<SearchStage<SearchComputationContext, Collection<String>, Collection<String>>> stages =
                aqlStream.map(aql -> () -> new ArangoDBSearchComputation(aql));

        final SearchPipeline<SearchComputationContext> pipeline =
                new SearchPipeline<>(stages.collect(Collectors.toList()), SERVICE_ENTITY_VERTEX);

        final SearchComputationContext context = ImmutableSearchComputationContext.builder()
                .summary(SEARCH_INTEGRATION_SUMMARY)
                .graphName("seGraph")
                .arangoDB(new ArangoDB.Builder()
                        .port(8600)
                        .user("root")
                        .password("root")
                        .build())
                .databaseName("topology-777777-71778796370224")
                .entityCollectionName("seVertexCollection")
                .allKeyword("ALL")
                .executorService(executorService)
                .build();

        final Either<Throwable, java.util.List<ServiceEntityRepoDTO>> resultsEntities =
                pipeline.run(context, ArangoDBSearchComputation.toEntities, Collections.emptyList());
        System.out.println(resultsEntities.map(entities -> {
            System.out.println("Entities: " + entities.size());
            return entities;
        }));
    }

    @Test
    @Ignore
    public void testMaxInput() {
        final ArangoDB arangoDB = new ArangoDB.Builder()
                .port(8600)
                .user("root")
                .password("root")
                .build();

        final String query = "FOR se IN @@seColl FILTER se._id IN @inputs RETURN se.displayName";
        final List<String> inputs = List.range(0, 50000)
                .map(s -> String.format("seVertexCollection/%s", s))
                .append("seVertexCollection/71774668367475")
                .append("seVertexCollection/71774668367232");

        System.out.println("Inputs size: " + inputs.size());

        final Map<String, Object> bindVars = ImmutableMap.of(
                "@seColl", "seVertexCollection",
                "inputs", inputs.toJavaList());

        final ArangoCursor<String> cursor = arangoDB.db("topology-777777-71774697249200")
                .query(query, bindVars, null, String.class);

        System.out.println(cursor.asListRemaining());
    }

    @Test
    @Ignore
    public void testSearchHandler() {
        final GraphCreatorFixture graphFixture = new GraphCreatorFixture();
        final GraphDefinition graphDefinition = graphFixture.getGraphDefinition();
        final String db = "topology-777777-71780223970528";

        ArangoDatabaseFactory arangoDatabaseFactory = () -> {
                    return new ArangoDB.Builder()
                                    .host("192.168.99.100")
                                    .port(8529)
                                    .user("root")
                                    .password("root")
                                    .build();
                };

        final ArangoDBExecutor arangoDBExecutor = new ArangoDBExecutor(arangoDatabaseFactory, db);

        final AQLRepr repr1 = new AQLRepr(List.of(Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setStringFilter(makeRegexStringFilter("PhysicalMachine", true))
                        .build())));
        final AQLRepr repr2 = new AQLRepr(List.of(
                Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1)));
        final AQLRepr repr3 = new AQLRepr(List.of(Filter.propertyFilter(
                PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(makeRegexStringFilter("#", false))
                        .build())));

        SearchHandler searchHandler = new SearchHandler(graphDefinition,
                                                        arangoDatabaseFactory,
                                                        arangoDBExecutor);

        Either<Throwable, java.util.List<String>> result = searchHandler.searchEntityOids(
                Arrays.asList(repr1, repr2, repr3), db, Optional.empty(), Collections.emptyList());
        System.out.println("====> Search OID Results: " + result);

        Either<Throwable, java.util.List<ServiceEntityRepoDTO>> result2 = searchHandler.searchEntities(
                Arrays.asList(repr1, repr2, repr3), db, Optional.empty(), Collections.emptyList());
        System.out.println("====> Search entity Results: " + result2);
    }

    private Search.PropertyFilter entityTypeFilter(final String entityType) {
        return Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                        .setStringPropertyRegex(entityType)
                        .build())
                .build();
    }

    private Search.PropertyFilter displayNameFilter(final String nameRegex) {
        return Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                        .setStringPropertyRegex(nameRegex)
                        .build())
                .build();
    }

    private Search.TraversalFilter traversalCondFilter(final Search.TraversalFilter.TraversalDirection direction,
                                                                    final Search.PropertyFilter condition) {
        return Search.TraversalFilter.newBuilder()
                .setTraversalDirection(direction)
                .setStoppingCondition(Search.TraversalFilter.StoppingCondition.newBuilder()
                        .setStoppingPropertyFilter(condition)
                        .build())
                .build();
    }
}
