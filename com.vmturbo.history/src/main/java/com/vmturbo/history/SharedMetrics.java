package com.vmturbo.history;

import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

/**
 * Contains metric instrumentation code that is shared across different locations
 * in the history component.
 */
public class SharedMetrics {
    /**
     * Used for labelling source-topology related metrics. Came from topology-processor.
     */
    public static final String SOURCE_TOPOLOGY_TYPE_LABEL = "source";

    /**
     * Used for labelling projected topology-related metrics. Came from market.
     */
    public static final String PROJECTED_TOPOLOGY_TYPE_LABEL = "projected";

    /**
     * Indicates a plan topology belonged to a plan topology
     */
    public static final String PLAN_CONTEXT_TYPE_LABEL = "plan";
    public static final String LIVE_CONTEXT_TYPE_LABEL = "live";

    public static final Histogram TOPOLOGY_ENTITY_COUNT_HISTOGRAM = Histogram.build()
        .name("history_topology_entity_count")
        .help("Size of topology received by the history component.")
        .labelNames("topology_type", "context_type")
        .buckets(1_000, 5_000, 10_000, 30_000, 50_000, 75_000, 100_000, 150_000, 200_000)
        .register();

    public static final Summary UPDATE_TOPOLOGY_DURATION_SUMMARY = Summary.build()
        .name("history_update_topology_duration_seconds")
        .help("Duration in seconds it takes the history component to update the status for a topology.")
        .labelNames("topology_type", "context_type")
        .register();

    public static final Summary UPDATE_PRICE_INDEX_DURATION_SUMMARY = Summary.build()
        .name("history_update_price_index_duration_seconds")
        .help("Duration in seconds it takes the history component to update the price index for a (source) topology.")
        .labelNames("context_type")
        .register();
}
