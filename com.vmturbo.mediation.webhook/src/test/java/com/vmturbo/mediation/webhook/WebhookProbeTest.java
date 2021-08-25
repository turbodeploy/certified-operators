package com.vmturbo.mediation.webhook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests the WebhookProbeTest.
 */
public class WebhookProbeTest {
    /**
     * If validation fails, then discovery should also fail.
     */
    @Test
    public void testFailingValidationFailsDiscovery() {
        WebhookProbe webhookProbe = Mockito.spy(new WebhookProbe());
        // return succeeding validation
        when(webhookProbe.validateTarget(any())).thenReturn(
                ValidationResponse.newBuilder()
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
     * When the template that referring to a property that does not exist.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testTemplateWithUnknownProperty() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = createActionExecutionDTO("http://google.com",
                "Test $action.description1", "GET");

        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        IProgressTracker progressTracker = mock(IProgressTracker.class);

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO,
            new WebhookAccount(),
            new HashMap<>(),
            progressTracker);

        // ASSERT
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
        Assert.assertTrue(result.getDescription()
                .contains("does not contain property 'description1' at <unknown template>[line 1, column 14]"));

    }

    /**
     * Makes sure the probe supports version two action types.
     */
    @Test
    public void testSupportsVersion2ActionTypes() {
        WebhookProbe probe = new WebhookProbe();
        Assert.assertTrue(probe.supportsVersion2ActionTypes());
    }

    /**
     * When the template is not a valid, the action should fail.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testTemplateWithParseException() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = createActionExecutionDTO("http://google.com",
                "#set ($test=]())", "GET");

        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        IProgressTracker progressTracker = mock(IProgressTracker.class);

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO,
                new WebhookAccount(),
                new HashMap<>(),
                progressTracker);

        // ASSERT
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
        Assert.assertTrue(result.getDescription()
                .contains("Encountered \"]\" at <unknown template>[line 1, column 13]"));
    }

    /**
     * When we don't receive required webhook data using for sending request to workflow server
     * (e.g. URL_PROPERTY, HTTP_METHOD_PROPERTY), we should fail execution.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testWebhookDataNotProvided() throws Exception {

        final IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        final IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        final WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        final IProgressTracker progressTracker = mock(IProgressTracker.class);

        ActionExecutionDTO actionExecutionDTO = createActionExecutionDTO(null, "test", null);

        // this method should throw an exception
        final ActionResult result = probe.executeAction(actionExecutionDTO,
                new WebhookAccount(),
                new HashMap<>(),
                progressTracker);
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
    }

    private ActionExecutionDTO createActionExecutionDTO(String url, String template, String method) {
        return ActionExecutionDTO.newBuilder().setActionType(ActionType.RESIZE)
            .addActionItem(ActionExecution.ActionItemDTO.newBuilder()
                    .setActionType(ActionType.RESIZE)
                    .setUuid("1363")
                    .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                            .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                            .setId("4521")
                            .build())
                    .build())
            .setWorkflow(createWorkflow(url, template, method, null, null, null)).build();
    }

    /**
     * Creates a workflow with input parameters.
     *
     * @param url the url that we are making call to.
     * @param template the template for the body of call.
     * @param method the method of the http call e.g., GET, POST,...
     * @param authenticationMethod the method to use to do authentication.
     * @param username the username to use for authentication
     * @param password the password to use for authentication
     * @return the workflow object created.
     */
    public static Workflow createWorkflow(String url, String template, String method,
                              AuthenticationMethod authenticationMethod, String username, String password) {
        Workflow.Builder workflow = Workflow.newBuilder()
                .setId("Webhook");

        if (url != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.URL)
                    .setValue(url)
                    .build());
        }

        if (template != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.TEMPLATED_ACTION_BODY)
                    .setValue(template)
                    .build());
        }

        if (method != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.HTTP_METHOD)
                    .setValue(method)
                    .build());
        }

        if (authenticationMethod != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.AUTHENTICATION_METHOD)
                    .setValue(authenticationMethod.name())
                    .build());
        }

        if (username != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.USER_NAME)
                    .setValue(username)
                    .build());
        }

        if (password != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.PASSWORD)
                    .setValue(password)
                    .build());
        }
        return workflow.build();
    }
}
