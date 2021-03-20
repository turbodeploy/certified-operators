package com.vmturbo.topology.processor.operation.validation;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiModel;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An validation operation on a target.
 */
@ApiModel("Validation")
public class Validation extends Operation {

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

    /**
     * Constructs validation operation.
     *
     * @param probeId probe OID
     * @param targetId target OID
     * @param identityProvider identity provider to assign unique operation OID
     */
    public Validation(final long probeId,
                      final long targetId,
                      @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, VALIDATION_DURATION_SECONDS);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Validation ")
                .append(super.toString()).toString();
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return VALIDATION_STATUS_COUNTER;
    }

    @Nonnull
    @Override
    public String getErrorString() {
        return "Validation failed: " + String.join(", ", getErrors());
    }
}
