package com.vmturbo.mediation.actionstream.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * Tests the ActionStreamKafkaProbe.
 */
public class ActionStreamKafkaProbeTest {

    /**
     * Discovery should return the ON_GENERATION workflow.
     */
    @Test
    public void testDiscovery() {
        ActionStreamKafkaProbe actionStreamKafkaProbe = spy(new ActionStreamKafkaProbe());
        // return succeeding validation
        when(actionStreamKafkaProbe.validateTarget(any())).thenReturn(ValidationResponse.newBuilder()
            .build());

        final ActionStreamKafkaProbeAccount account = new ActionStreamKafkaProbeAccount();
        DiscoveryResponse response = actionStreamKafkaProbe.discoverTarget(account);

        Assert.assertEquals(1, response.getWorkflowList().size());
        Assert.assertEquals(ActionScriptPhase.ON_GENERATION,
            response.getWorkflowList().get(0).getPhase());
    }

    /**
     * If validation fails, then discovery should also fail.
     */
    @Test
    public void testFailingValidationFailsDiscovery() {
        ActionStreamKafkaProbe actionStreamKafkaProbe = spy(new ActionStreamKafkaProbe());
        // return succeeding validation
        when(actionStreamKafkaProbe.validateTarget(any())).thenReturn(ValidationResponse.newBuilder()
            .addErrorDTO(ErrorDTO.newBuilder()
                .setDescription("something bad happened")
                .setSeverity(ErrorSeverity.CRITICAL)
                .build())
            .build());

        final ActionStreamKafkaProbeAccount account = new ActionStreamKafkaProbeAccount();
        DiscoveryResponse response = actionStreamKafkaProbe.discoverTarget(account);

        Assert.assertEquals(0, response.getWorkflowList().size());
        Assert.assertEquals(1, response.getErrorDTOList().size());
    }
}
