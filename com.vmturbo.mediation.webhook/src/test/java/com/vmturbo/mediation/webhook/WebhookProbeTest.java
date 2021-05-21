package com.vmturbo.mediation.webhook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests the WebhookProbeTest.
 */
public class WebhookProbeTest {

    private static final ActionExecutionDTO NO_TEMPLATE_ACTION = ActionExecutionDTO.newBuilder()
        .setActionType(ActionType.RESIZE)
        .setWorkflow(Workflow.newBuilder()
            .setId("Webhook")
            .build())
        .build();

    /**
     * Discovery should return the ON_GENERATION workflow.
     */
    @Test
    public void testDiscovery() {
        WebhookProbe webhookProbe = spy(new WebhookProbe());
        // return succeeding validation
        when(webhookProbe.validateTarget(any())).thenReturn(ValidationResponse.newBuilder()
                .build());

        final WebhookAccount account = new WebhookAccount();
        DiscoveryResponse response = webhookProbe.discoverTarget(account);

        Assert.assertEquals(3, response.getWorkflowList().size());
        Assert.assertEquals(ActionScriptPhase.PRE,
            response.getWorkflowList().get(0).getPhase());
        Assert.assertEquals(ActionScriptPhase.REPLACE,
            response.getWorkflowList().get(1).getPhase());
        Assert.assertEquals(ActionScriptPhase.POST,
            response.getWorkflowList().get(2).getPhase());
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

        final WebhookAccount account = new WebhookAccount();
        DiscoveryResponse response = webhookProbe.discoverTarget(account);

        Assert.assertEquals(0, response.getWorkflowList().size());
        Assert.assertEquals(1, response.getErrorDTOList().size());
    }

    /**
     * When we don't receive workflow property TEMPLATED_ACTION_BODY, we should fail execution.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testTemplateNotProvided() throws Exception {
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        IProgressTracker progressTracker = mock(IProgressTracker.class);
        // this method should throw an exception
        ActionResult result = probe.executeAction(NO_TEMPLATE_ACTION,
            new WebhookAccount(),
            new HashMap<>(),
            progressTracker);
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
    }
}
