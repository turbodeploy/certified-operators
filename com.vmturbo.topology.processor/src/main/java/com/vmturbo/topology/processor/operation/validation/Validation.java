package com.vmturbo.topology.processor.operation.validation;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An validation operation on a target.
 */
public class Validation extends Operation {

    /**
     * The timer used for timing the duration of validations.
     * Mark transient to avoid serialization of this field.
     */
    private transient final DataMetricTimer durationTimer;

    private static final DataMetricSummary VALIDATION_DURATION_SECONDS = DataMetricSummary.builder()
        .withName("tp_validation_duration_seconds")
        .withHelp("Duration of a validation in the Topology Processor.")
        .build()
        .register();

    private static final DataMetricCounter VALIDATION_STATUS_COUNTER = DataMetricCounter.builder()
        .withName("tp_validation_status_total")
        .withHelp("Status of all completed validations.")
        .withLabelNames("status")
        .build()
        .register();

    public Validation(final long probeId,
                      final long targetId,
                      @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider);
        durationTimer = VALIDATION_DURATION_SECONDS.startTimer();
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Validation ")
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
        return VALIDATION_STATUS_COUNTER;
    }
}
