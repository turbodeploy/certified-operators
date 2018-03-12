package com.vmturbo.repository.search;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javaslang.control.Either;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.executor.AQL;

/**
 * Handler for the search requests.
 */
public class SearchHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchHandler.class);

    private static final String ALL_ENTITIES = "ALL";

    private final GraphDefinition graphDefinition;

    private final ArangoDatabaseFactory arangoDatabaseFactory;

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
                         @Nonnull final ArangoDatabaseFactory arangoDatabaseFactory) {
        this.graphDefinition = Objects.requireNonNull(graphDefinition);
        this.arangoDatabaseFactory = Objects.requireNonNull(arangoDatabaseFactory);

        executorService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("search-handler-%d").build());
    }

    /**
     * Given a list of {@link AQLRepr}, query the database.
     *
     * @param aqlReprs The list of {@link AQLRepr}.
     * @param database The database to search for entities.
     * @return The result of OID list from database or an exception.
     */
    public Either<Throwable, Collection<String>> searchEntityOids(
                          @Nonnull final List<AQLRepr> aqlReprs,
                          @Nonnull final String database) {
        final List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs);

        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();

        try (DataMetricTimer timer = SEARCH_PIPELINE_DURATION_SUMMARY.startTimer()){
            return pipeline(fusedAQLReprs).run(context(arangoDB, database));
        }
    }

    /**
     * Given a list of {@link AQLRepr}, query the database.
     *
     * @param aqlReprs The list of {@link AQLRepr}.
     * @param database The database to search for entities.
     * @return The result of {@link ServiceEntityRepoDTO} list from database or an exception.
     */
    public Either<Throwable, Collection<ServiceEntityRepoDTO>> searchEntities(
                          @Nonnull final List<AQLRepr> aqlReprs,
                          @Nonnull final String database) {
        final List<AQLRepr> fusedAQLReprs = AQLReprFuser.fuse(aqlReprs);

        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();

        try(DataMetricTimer timer = SEARCH_PIPELINE_DURATION_SUMMARY.startTimer()) {
            final Either<Throwable, ArangoCursor<ServiceEntityRepoDTO>> cursorResult =
                            pipeline(fusedAQLReprs).run(context(arangoDB, database),
                                                   ArangoDBSearchComputation.toEntities);
            // TODO: We may want to reify the ArangoCursor later to reduce memory pressure.
            return cursorResult.map(cursor -> {
                    try {
                        return cursor.asListRemaining();
                    } finally {
                        try {
                            if (cursor!=null) {
                                cursor.close();
                            }
                        } catch (IOException ioe) {
                            logger.error("Error closing arangodb cursor", ioe);
                        }
                    }
            });
        }
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

        return new SearchPipeline<>(stages.collect(Collectors.toList()));
    }

    private SearchComputationContext context(final ArangoDB arangoDB, final String db) {
        final SearchComputationContext context = ImmutableSearchComputationContext.builder()
                .summary(SEARCH_STAGE_DURATION_SUMMARY)
                .graphName(graphDefinition.getGraphName())
                .arangoDB(arangoDB)
                .databaseName(db)
                .entityCollectionName(graphDefinition.getServiceEntityVertex())
                .allKeyword(ALL_ENTITIES)
                .executorService(executorService)
                .build();

        return context;
    }
}
