package com.vmturbo.topology.processor.operation.discovery;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An discovery operation on a target.
 */
public class Discovery extends Operation {

    /**
     * The timer used for timing the duration of discoveries.
     * Mark transient to avoid serialization of this field.
     */
    private transient final DataMetricTimer durationTimer;

    private static final DataMetricSummary DISCOVERY_DURATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_discovery_duration_seconds")
        .withHelp("Duration of a discovery in the Topology Processor.")
        .build()
        .register();

    private static final DataMetricCounter DISCOVERY_STATUS_COUNTER = DataMetricCounter.builder()
        .withName("tp_discovery_status_total")
        .withHelp("Status of all completed discoveries.")
        .withLabelNames("status")
        .build()
        .register();

    public Discovery(final long probeId,
                     final long targetId,
                     @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider);
        durationTimer = DISCOVERY_DURATION_SUMMARY.startTimer();
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Discovery ")
                .append(super.toString()).toString();
    }

    @Nonnull
    @Override
    protected DataMetricTimer getMetricsTimer() {
        return durationTimer;
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return DISCOVERY_STATUS_COUNTER;
    }

    @Nonnull
    @Override
    public String getErrorString() {
        return "Discovery failed: " + String.join(", ", getErrors());
    }
}
