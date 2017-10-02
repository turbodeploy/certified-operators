package com.vmturbo.repository.search;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.immutables.value.Value;

import com.arangodb.ArangoDB;

import com.vmturbo.proactivesupport.DataMetricSummary;

@Value.Immutable
public abstract class SearchComputationContext {

    /**
     * An ID for a pipeline.
     *
     * This is for tracing purposes.
     *
     * @return A ID that is unique to the flow of computation.
     */
    @Value.Default
    public String traceID() {
        return UUID.randomUUID().toString();
    }

    /**
     * The ArangoDB driver needed by the search computation.
     *
     * @return {@link ArangoDB}.
     */
    public abstract ArangoDB arangoDB();

    /**
     * The database the computation needs query.
     *
     * @return The database name.
     */
    public abstract String databaseName();

    /**
     * The graph within the database the computation needs to traverse.
     *
     * @return The graph name.
     */
    public abstract String graphName();

    /**
     * The name of the service entity collection.
     *
     * @return The name.
     */
    public abstract String entityCollectionName();

    /**
     * The keyword used in the query to signify we want to search for all entities.
     *
     * @return The all keyword.
     */
    public abstract String allKeyword();

    /**
     * The {@link ExecutorService} for running search queries.
     *
     * @return An {@link ExecutorService}.
     */
    public abstract ExecutorService executorService();

    /**
     * The summary for collecting metrics.
     *
     * @return Collect metrics for each {@link SearchStage}.
     */
    public abstract DataMetricSummary summary();
}
