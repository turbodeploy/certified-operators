package com.vmturbo.repository.search;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Failure;
import static javaslang.Patterns.Success;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javaslang.control.Either;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.topology.TopologyID;

/**
 * Handler for the search requests.
 */
public class SearchHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchHandler.class);

    private static final String ALL_ENTITIES = "ALL";

    private final GraphDefinition graphDefinition;

    private final ArangoDatabaseFactory arangoDatabaseFactory;

    private final GraphDBExecutor executor;

    private static final DataMetricSummary SEARCH_STAGE_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_search_stage_duration_seconds")
        .withHelp("Duration in seconds it takes repository to execute a search stage.")
        .build()
        .register();

    private static final DataMetricSummary SEARCH_PIPELINE_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_search_pipeline_duration_seconds")
        .withHelp("Duration in seconds it takes repository to execute search pipeline.")
        .build()
        .register();

    /**
     * The instance of ExecutorService that will be used for all {@link SearchComputationContext}
     * created for search requests.
     */
    private final ExecutorService executorService;

    public SearchHandler(@Nonnull final GraphDefinition graphDefinition,
                         @Nonnull final ArangoDatabaseFactory arangoDatabaseFactory,
                         @Nonnull final GraphDBExecutor graphDBExecutor) {
        this.graphDefinition = Objects.requireNonNull(graphDefinition);
        this.arangoDatabaseFactory = Objects.requireNonNull(arangoDatabaseFactory);
        this.executor = Objects.requireNonNull(graphDBExecutor);

        executorService = Tracing.traceAwareExecutor(Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("search-handler-%d").build()));
    }

    /**
     * Search tags based on a {@link SearchTagsRequest} message.
     *
     * @param dbName the name of the live database.
     * @param request the criteria for the tags returned.
     * @return the tags returned in form of a map from keys to {@link TagValuesDTO} objects.
     *         one {@link TagValuesDTO} object represents a list of values.
     * @throws ArangoDBException database access failed.
     */
    public Map<String, TagValuesDTO> searchTags(String dbName, SearchTagsRequest request)
            throws ArangoDBException {
        return executor.executeTagCommand(dbName, graphDefinition.getServiceEntityVertex(),
                request);
    }

    /**
     * Given a list of {@link AQLRepr}, query the database.
     *
     * @param aqlReprs The list of {@link AQLRepr}.
     * @param database The database to search for entities.
     * @return The result of OID list from database or an exception.
     */
    public Either<Throwable, List<String>> searchEntityOids(
                          @Nonnull final List<AQLRepr> aqlReprs,
                          @Nonnull final String database,
                          @Nonnull final Optional<PaginationParameters> paginationParams,
                          @Nonnull final List<String> oids) {
        final List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs, paginationParams);

        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();

        final List<String> oidsWithPrefix = generateOidsWithPrefix(oids);

        try (DataMetricTimer timer = SEARCH_PIPELINE_DURATION_SUMMARY.startTimer()){
            return pipeline(fusedAQLReprs).run(context(arangoDB, database, false), oidsWithPrefix);
        }
    }

    /**
     * Given a list of {@link AQLRepr}, query the database.
     *
     * @param aqlReprs The list of {@link AQLRepr}.
     * @param database The database to search for entities.
     * @return The result of {@link ServiceEntityRepoDTO} list from database or an exception.
     */
    public Either<Throwable, List<ServiceEntityRepoDTO>> searchEntities(
            @Nonnull final List<AQLRepr> aqlReprs,
            @Nonnull final String database,
            @Nonnull final Optional<PaginationParameters> paginationParams,
            @Nonnull final List<String> oids) {
        final List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs, paginationParams);

        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();
        final List<String> oidsWithPrefix = generateOidsWithPrefix(oids);

        try (DataMetricTimer timer = SEARCH_PIPELINE_DURATION_SUMMARY.startTimer()) {
            return pipeline(fusedAQLReprs).run(context(arangoDB, database, false),
                                   ArangoDBSearchComputation.toEntities, oidsWithPrefix);
        }
    }

    /**
     * Given a list of {@link AQLRepr}, query the database. And return full ServiceEntityRepoDTOs
     * with all fields populated.
     *
     * @param aqlReprs The list of {@link AQLRepr}.
     * @param database The database to search for entities.
     * @return The result of {@link ServiceEntityRepoDTO} list from database or an exception.
     */
    public Either<Throwable, List<ServiceEntityRepoDTO>> searchEntitiesFull(
            @Nonnull final List<AQLRepr> aqlReprs,
            @Nonnull final String database,
            @Nonnull final Optional<PaginationParameters> paginationParams,
            @Nonnull final List<String> oids) {
        final List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs, paginationParams);

        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();
        final List<String> oidsWithPrefix = generateOidsWithPrefix(oids);

        try (DataMetricTimer timer = SEARCH_PIPELINE_DURATION_SUMMARY.startTimer()) {
            return pipeline(fusedAQLReprs).run(context(arangoDB, database, true),
                    ArangoDBSearchComputation.toEntities, oidsWithPrefix);
        }
    }

    /**
     * Get a list of {@link ServiceEntityRepoDTO} based on input a list of entity oids.
     *
     * @param entityOids a list of entity oids.
     * @param topologyID {@link TopologyID} contains which database to query.
     * @return Either of a list of {@link ServiceEntityRepoDTO} or {@link Throwable}.
     */
    public Either<Throwable, Collection<ServiceEntityRepoDTO>> getEntitiesByOids(
            @Nonnull final Set<Long> entityOids,
            @Nonnull final TopologyID topologyID) {
        final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
                graphDefinition.getSEVertexCollection(topologyID),
                entityOids);
        final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeServiceEntityMultiGetCmd(cmd);

        return Match(seResults).of(
                Case(Success($()), Either::right),
                Case(Failure($()), Either::left));
    }

    @PreDestroy
    private void springPreDestroy() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private SearchPipeline<SearchComputationContext> pipeline(final List<AQLRepr> aqlReprs) {
        // AQLs
        final Stream<AQL> aqlStream = aqlReprs.stream().map(AQLRepr::toAQL);

        // Stages
        final Stream<SearchStage<SearchComputationContext, Collection<String>, Collection<String>>>
                stages = aqlStream.map(aql -> () -> new ArangoDBSearchComputation(aql));

        return new SearchPipeline<>(stages.collect(Collectors.toList()), graphDefinition.getServiceEntityVertex());
    }

    private SearchComputationContext context(final ArangoDB arangoDB, final String db,
            final boolean fullEntity) {
        final SearchComputationContext context = ImmutableSearchComputationContext.builder()
                .summary(SEARCH_STAGE_DURATION_SUMMARY)
                .graphName(graphDefinition.getGraphName())
                .arangoDB(arangoDB)
                .databaseName(db)
                .entityCollectionName(graphDefinition.getServiceEntityVertex())
                .allKeyword(ALL_ENTITIES)
                .executorService(executorService)
                .fullEntity(fullEntity)
                .build();

        return context;
    }

    private List<String> generateOidsWithPrefix(@Nonnull final List<String> oids) {
        return oids.stream()
                .map(id -> graphDefinition.getServiceEntityVertex() + "/" + id)
                .collect(Collectors.toList());
    }
}
