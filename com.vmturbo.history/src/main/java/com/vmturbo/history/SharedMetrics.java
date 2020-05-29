package com.vmturbo.history;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * Contains metric instrumentation code that is shared across different locations
 * in the history component.
 */
public class SharedMetrics {
    /**
     * Don't allow instantiation.
     */
    private SharedMetrics() {
    }

    /**
     * Used for labelling source-topology related metrics. Came from topology-processor.
     */
    public static final String SOURCE_TOPOLOGY_TYPE_LABEL = "source";

    /**
     * Used for labelling projected topology-related metrics. Came from market.
     */
    public static final String PROJECTED_TOPOLOGY_TYPE_LABEL = "projected";

    /**
     * Indicates a metric related to a plan topology.
     */
    public static final String PLAN_CONTEXT_TYPE_LABEL = "plan";

    /**
     * Indicates a metric related to a live topology.
     */
    public static final String LIVE_CONTEXT_TYPE_LABEL = "live";

    /**
     * Possible values for batched insert dispositions (use name() for label values).
     */
    public enum BatchInsertDisposition {
        /**
         * The batch was inserted without error.
         */
        success,
        /**
         * The batch failed on first attempt but succeeded on retry.
         */
        retry,
        /**
         * The batch failed on all attempts and was abandoned.
         */
        failure
    }

    /**
     * Count of entities processed from various topology types and contexts.
     */
    public static final DataMetricHistogram TOPOLOGY_ENTITY_COUNT_HISTOGRAM = DataMetricHistogram.builder()
            .withName("history_topology_entity_count")
            .withHelp("Size of topology received by the history component.")
            .withLabelNames("topology_type", "context_type")
            .withBuckets(1_000, 5_000, 10_000, 30_000, 50_000, 75_000, 100_000, 150_000, 200_000)
            .build()
            .register();

    /**
     * Duration of topology processing.
     */
    public static final DataMetricSummary UPDATE_TOPOLOGY_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("history_update_topology_duration_seconds")
            .withHelp("Duration in seconds it takes the history component to update the status for a topology.")
            .withLabelNames("topology_type", "context_type")
            .build()
            .register();

    /**
     * Duration of price index update for a topology.
     */
    public static final DataMetricSummary UPDATE_PRICE_INDEX_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("history_update_price_index_duration_seconds")
            .withHelp("Duration in seconds it takes the history component to update the price index for a (source) topology.")
            .withLabelNames("context_type")
            .build()
            .register();

    /**
     * Count of records successfully written to various tables.
     */
    public static final DataMetricCounter RECORDS_WRITTEN_BY_TABLE = DataMetricCounter.builder()
            .withName("records_written_by_table")
            .withHelp("Number of records written to database across individual tables")
            .withLabelNames("table")
            .build()
            .register();

    /**
     * Count of record batches written to various tables, including batches that failed or required
     * retries.
     */
    public static final DataMetricCounter BATCHED_INSERTS = DataMetricCounter.builder()
            .withName("batched_inserts")
            .withHelp("Number of batched insert executions.\n" +
            "RETRY may happen multiple times per batch, and that batch will also be counted " +
            "as a SUCCESS or as a FAILURE.")
        .withLabelNames("table", "disposition")
        .build()
        .register();

    /**
     * Prometheus metric to measure amount of time spent in percentile snapshot reading by history
     * component.
     */
    public static final DataMetricSummary PERCENTILE_READING = DataMetricSummary.builder()
            .withName("percentile_reading_seconds")
            .withHelp("Duration in seconds it takes the history component to read percentile snapshot from database.")
            .build()
            .register();

    /**
     * Prometheus metric to measure amount of time spent in percentile snapshot writing by history
     * component.
     */
    public static final DataMetricSummary PERCENTILE_WRITING = DataMetricSummary.builder()
            .withName("percentile_writing_seconds")
            .withHelp("Duration in seconds it takes the history component to store percentile snapshot in database.")
            .build()
            .register();
}
