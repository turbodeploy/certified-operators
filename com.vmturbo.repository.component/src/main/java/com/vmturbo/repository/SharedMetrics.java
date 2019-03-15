package com.vmturbo.repository;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * Contains metrics for receiving and updating topologies in the repository.
 */
public class SharedMetrics {
    /**
     * Used for labelling source-topology related metrics.
     */
    public static final String SOURCE_LABEL = "source";

    /**
     * Used for labelling projected topology-related metrics.
     */
    public static final String PROJECTED_LABEL = "projected";

    /**
     * Used for labelling single-source supply chain related metrics.
     */
    public static final String SINGLE_SOURCE_LABEL = "single_source";

    /**
     * Used for labelling global supply chain related metrics.
     */
    public static final String GLOBAL_LABEL = "global";

    public static final String PROCESSED_LABEL = "processed";
    public static final String FAILED_LABEL = "failed";
    public static final String SKIPPED_LABEL = "skipped";

    public static final DataMetricGauge TOPOLOGY_ENTITY_COUNT_GAUGE = DataMetricGauge.builder()
        .withName("repo_topology_entity_count")
        .withHelp("Size of topology received by repository.")
        .withLabelNames("topology_type")
        .build()
        .register();

    public static final DataMetricSummary TOPOLOGY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_update_topology_duration_seconds")
        .withHelp("Duration in seconds it takes repository to update a topology. May be source or projected. Skipped topologies are not included.")
        .withLabelNames("topology_type")
        .build()
        .register();

    public static final DataMetricCounter TOPOLOGY_COUNTER = DataMetricCounter.builder()
            .withName("repo_topology_count")
            .withHelp("Number of topologies received by repository, separated into 'processed', 'failed' and 'skipped' statuses.")
            .withLabelNames("topology_type", "status")
            .build()
            .register();

}
