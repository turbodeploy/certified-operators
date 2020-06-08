package com.vmturbo.topology.processor.operation.actionapproval;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * Operation to retrieve action states from the external action approval backend. This operation
 * is used to track if the action has been approved or rejected at the external approval backend.
 */
public class GetActionState extends Operation {

    private static final DataMetricSummary GET_ACTION_STATE_DURATION_SUMMARY =
            DataMetricSummary.builder().withName("tp_get_action_state_duration_seconds").withHelp(
                    "Duration of request of remote action state"
                            + " in external action approval backend.").build().register();

    private static final DataMetricCounter GET_ACTION_STATE_STATUS_COUNTER =
            DataMetricCounter.builder()
                    .withName("tp_get_action_state_status_total")
                    .withHelp("Status of all action state updates to action approval backend.")
                    .withLabelNames("status")
                    .build()
                    .register();

    /**
     * Constructs get action states operation.
     *
     * @param probeId probe to operate with
     * @param targetId target to get actopm states frp,m
     * @param identityProvider identity provider
     */
    public GetActionState(long probeId, long targetId, @Nonnull IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, GET_ACTION_STATE_DURATION_SUMMARY);
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return GET_ACTION_STATE_STATUS_COUNTER;
    }
}
