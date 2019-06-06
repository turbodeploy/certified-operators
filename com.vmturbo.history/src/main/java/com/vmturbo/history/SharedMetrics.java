package com.vmturbo.history;

import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;

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

    public static final DataMetricHistogram TOPOLOGY_ENTITY_COUNT_HISTOGRAM = DataMetricHistogram.builder()
        .withName("history_topology_entity_count")
        .withHelp("Size of topology received by the history component.")
        .withLabelNames("topology_type", "context_type")
        .withBuckets(1_000, 5_000, 10_000, 30_000, 50_000, 75_000, 100_000, 150_000, 200_000)
        .build()
        .register();

    public static final DataMetricSummary UPDATE_TOPOLOGY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("history_update_topology_duration_seconds")
        .withHelp("Duration in seconds it takes the history component to update the status for a topology.")
        .withLabelNames("topology_type", "context_type")
        .build()
        .register();

    public static final DataMetricSummary UPDATE_PRICE_INDEX_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("history_update_price_index_duration_seconds")
        .withHelp("Duration in seconds it takes the history component to update the price index for a (source) topology.")
        .withLabelNames("context_type")
        .build()
        .register();

    public static final String ALL_AGGREGATORS_LABEL = "all_aggregators";
    public static final DataMetricSummary STATISTICS_AGGREGATION_SUMMARY = DataMetricSummary.builder()
                    .withName("statistics_aggregation_duration_seconds")
                    .withHelp("Duration in seconds it takes the history component to aggregate and write statistics for a topology snapshot.")
                    .withLabelNames(ALL_AGGREGATORS_LABEL)
                    .build()
                    .register();
}
