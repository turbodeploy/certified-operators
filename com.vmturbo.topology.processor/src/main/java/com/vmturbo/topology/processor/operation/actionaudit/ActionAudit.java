package com.vmturbo.topology.processor.operation.actionaudit;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * Action audit operation sends action events to external action audit backend.
 */
public class ActionAudit extends Operation {
    private static final DataMetricSummary ACTION_AUDIT_SUMMARY =
            DataMetricSummary.builder()
                    .withName("tp_action_audit_duration_seconds")
                    .withHelp("Duration of action audit request to external audit backend.")
                    .build()
                    .register();

    private static final DataMetricCounter ACTION_AUDIT_COUNTER =
            DataMetricCounter.builder()
                    .withName("tp_action_audit_status_total")
                    .withHelp(
                            "Status of all action audit requests to extenal action audit backend.")
                    .withLabelNames("status")
                    .build()
                    .register();

    /**
     * Constructs action audit operation.
     *
     * @param probeId probe to operate with
     * @param targetId target to send action audit events to
     * @param identityProvider identity provider
     */
    public ActionAudit(long probeId, long targetId, @Nonnull IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, ACTION_AUDIT_SUMMARY);
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return ACTION_AUDIT_COUNTER;
    }
}
