package com.vmturbo.mediation.webhook;

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
 * Tests the WebhookProbeTest.
 */
public class WebhookProbeTest {

    /**
     * Discovery should return the ON_GENERATION workflow.
     */
    @Test
    public void testDiscovery() {
        WebhookProbe webhookProbe = spy(new WebhookProbe());
        // return succeeding validation
        when(webhookProbe.validateTarget(any())).thenReturn(ValidationResponse.newBuilder()
                .build());

        final WebhookProbeAccount account = new WebhookProbeAccount();
        DiscoveryResponse response = webhookProbe.discoverTarget(account);

        Assert.assertEquals(1, response.getWorkflowList().size());
        Assert.assertEquals(ActionScriptPhase.ON_GENERATION,
                response.getWorkflowList().get(0).getPhase());
    }

    /**
     * If validation fails, then discovery should also fail.
     */
    @Test
    public void testFailingValidationFailsDiscovery() {
        WebhookProbe webhookProbe = spy(new WebhookProbe());
        // return succeeding validation
        when(webhookProbe.validateTarget(any())).thenReturn(ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setDescription("something bad happened")
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .build())
                .build());

        final WebhookProbeAccount account = new WebhookProbeAccount();
        DiscoveryResponse response = webhookProbe.discoverTarget(account);

        Assert.assertEquals(0, response.getWorkflowList().size());
        Assert.assertEquals(1, response.getErrorDTOList().size());
    }
}
