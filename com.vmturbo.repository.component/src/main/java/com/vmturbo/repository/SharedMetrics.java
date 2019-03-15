package com.vmturbo.repository;

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

    public static final DataMetricGauge TOPOLOGY_ENTITY_COUNT_GAUGE = DataMetricGauge.builder()
        .withName("repo_topology_entity_count")
        .withHelp("Size of topology received by repository.")
        .withLabelNames("topology_type")
        .build()
        .register();

    public static final DataMetricSummary TOPOLOGY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("repo_update_topology_duration_seconds")
        .withHelp("Duration in seconds it takes repository to update a topology. May be source or projected")
        .withLabelNames("topology_type")
        .build()
        .register();
}
