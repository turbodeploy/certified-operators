package com.vmturbo.topology.processor.operation.actionapproval;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * Operation to request approval for action execution from the external action approval backend.
 */
public class ActionApproval extends Operation {

    private static final DataMetricSummary ACTION_APPROVAL_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("tp_action_approval_duration_seconds")
            .withHelp("Duration of an action in the Topology Processor.")
            .build()
            .register();

    private static final DataMetricCounter ACTION_APPROVAL_STATUS_COUNTER = DataMetricCounter.builder()
            .withName("tp_action_approval_status_total")
            .withHelp("Status of all completed actions.")
            .withLabelNames("status")
            .build()
            .register();

    /**
     * Constructs action approval operation.
     *
     * @param probeId probe to operate with
     * @param targetId target to send approval requests to
     * @param identityProvider identity provider
     */
    public ActionApproval(long probeId, long targetId, @Nonnull IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, ACTION_APPROVAL_DURATION_SUMMARY);
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return ACTION_APPROVAL_STATUS_COUNTER;
    }
}
