package com.vmturbo.topology.processor.operation.actionapproval;

import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * Actions state updates (execution progress, transitions to IN_PROGRESS, SUCCESS, FAILURE) to an
 * external action approval backend.
 */
public class ActionUpdateState extends Operation {

    private static final DataMetricSummary ACTION_UPDATE_STATE_DURATION_SUMMARY =
            DataMetricSummary.builder()
                    .withName("tp_action_update_state_duration_seconds")
                    .withHelp(
                            "Duration of updating action state in external action approval backend.")
                    .build()
                    .register();

    private static final DataMetricCounter ACTION_UPDATE_STATE_STATUS_COUNTER =
            DataMetricCounter.builder()
                    .withName("tp_action_update_state_status_total")
                    .withHelp("Status of all action state updates to action approval backend.")
                    .withLabelNames("status")
                    .build()
                    .register();

    /**
     * Constructs action update state operation.
     *
     * @param probeId probe to operate with
     * @param targetId target to send action states to
     * @param identityProvider identity provider
     */
    public ActionUpdateState(long probeId, long targetId,
            @Nonnull IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, ACTION_UPDATE_STATE_DURATION_SUMMARY);
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return ACTION_UPDATE_STATE_STATUS_COUNTER;
    }
}
