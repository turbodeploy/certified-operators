package com.vmturbo.integrations.intersight;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.health.PollingHealthMonitor;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * IntersightMonitor watches both the Intersight instance the topology processor before declaring
 * itself healthy.
 */
public class IntersightMonitor extends PollingHealthMonitor {
    /**
     * The connection to access the Intersight instance.
     */
    private final IntersightConnection intersightConnection;

    /**
     * The entry point to access topology processor APIs.
     */
    private final TopologyProcessor topologyProcessor;

    /**
     * Create an {@link IntersightMonitor} object that will check on the specified Intersight
     * instance at the specified polling interval and perform tasks corresponding to various
     * integration points.
     *
     * @param pollingIntervalSecs how often (in seconds) to check the Intersight instance
     * @param intersightConnection provides connection to the Intersight instance
     * @param topologyProcessor the entry point to access topology processor APIs
     */
    public IntersightMonitor(double pollingIntervalSecs,
                             @Nonnull IntersightConnection intersightConnection,
                             @Nonnull TopologyProcessor topologyProcessor) {
        super("IntersightIntegration", pollingIntervalSecs);
        this.intersightConnection = Objects.requireNonNull(intersightConnection);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
    }

    @Override
    public void updateHealthStatus() {
        try {
            // enable to acquire the auth token and get the api client
            intersightConnection.getApiClient();
            reportHealthy();
        } catch (Throwable t) {
            // Treat all errors on this check as a sign of unhealthiness -- we'll be checking
            // again later.
            reportUnhealthy("Error: " + t.toString());
        }
    }
}
