package com.vmturbo.action.orchestrator.workflow.webhook;

import java.io.IOException;
import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator.ActionTemplateApplicationException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.api.ApiMessageMoles.ApiMessageServiceMole;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowProperty;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;

/**
 * Test for {@link ActionTemplateApplicator}.
 */
public class ActionTemplateApplicatorTest {

    @Mock
    private ApiMessageServiceMole apiMessageServiceMole;

    private GrpcTestServer grpcTestServer;
    private ActionTemplateApplicator actionTemplateApplicator;

    /**
     * Rule to track the expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Set up test environment.
     *
     * @throws IOException if failed to start grpc server
     */
    @Before
    public void init() throws IOException {
        MockitoAnnotations.initMocks(this);
        grpcTestServer = GrpcTestServer.newServer(apiMessageServiceMole);
        grpcTestServer.start();
        actionTemplateApplicator = new ActionTemplateApplicator(
                ApiMessageServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    /**
     * Clean up test environment.
     */
    @After
    public void clean() {
        grpcTestServer.close();
    }

    /**
     * Tests successful applying of template on a action.
     *
     * @throws ActionTemplateApplicationException if exception while applying template on
     *         the action happened
     */
    @Test
    public void testSuccessfulTemplateApplying() throws ActionTemplateApplicationException {
        // ARRANGE
        final String webhookURL = "www.webhook_server.test";
        final String action = ActionOrchestratorTestUtils.readSampleActionApiDto();
        final Workflow webhook = Workflow.newBuilder().setId(123L).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.WEBHOOK)
                        .setDisplayName("A unique name for the webhook")
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                                .setUrl(webhookURL)
                                .setTemplate(
                                        "{\"id\": \"$action.uuid\", \"type\": \"$action.actionType\", \"commodity\":"
                                                + " \"$action.risk.reasonCommodities.toArray()[0]\", \"to\": \"$action.newValue\"}")
                                .build())).build();

        // ACT
        final Workflow workflow = actionTemplateApplicator.addTemplateInformation(action, webhook);

        // ASSERT
        final Optional<WorkflowProperty> templatedBody =
                ActionOrchestratorTestUtils.getWorkflowProperty(
                        workflow.getWorkflowInfo().getWorkflowPropertyList(),
                        WebhookConstants.TEMPLATED_ACTION_BODY);
        final Optional<WorkflowProperty> url =
                ActionOrchestratorTestUtils.getWorkflowProperty(
                        workflow.getWorkflowInfo().getWorkflowPropertyList(),
                        WebhookConstants.URL);
        Assert.assertTrue(templatedBody.isPresent());
        Assert.assertTrue(url.isPresent());
        Assert.assertEquals(webhookURL, url.get().getValue());
        Assert.assertEquals("{\"id\": \"637078747168364\", \"type\": \"RESIZE\", \"commodity\": \"VMem\", \"to\": \"59768832.0\"}", templatedBody.get().getValue());
    }

    /**
     * When the template that referring to a property that does not exist.
     *
     * @throws ActionTemplateApplicationException if exception while applying template on
     *         the action happened
     */
    @Test
    public void testTemplateWithUnknownProperty() throws ActionTemplateApplicationException {
        // ARRANGE
        expectedException.expect(ActionTemplateApplicationException.class);
        expectedException.expectMessage("Exception while applying template:");
        final String action = ActionOrchestratorTestUtils.readSampleActionApiDto();
        final Workflow webhook = Workflow.newBuilder()
                .setId(123L)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.WEBHOOK)
                        .setDisplayName("A unique name for the webhook")
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                                .setTemplate(
                                        "{\"id\": \"$action.uuid12\"")))
                .build();

        // ACT
        actionTemplateApplicator.addTemplateInformation(action, webhook);
    }

    /**
     * Tests failed to  de-serialize ActionApiDTO.
     *
     * @throws ActionTemplateApplicationException if exception while applying template on
     *         the action happened
     */
    @Test
    public void testFailedToDeserializeAction() throws ActionTemplateApplicationException {
        // ARRANGE
        expectedException.expect(ActionTemplateApplicationException.class);
        expectedException.expectMessage("Failed to de-serialize ActionApiDTO.");
        final Workflow webhook = Workflow.newBuilder().setWorkflowInfo(WorkflowInfo.newBuilder()
                .setType(OrchestratorType.WEBHOOK)
                .setWebhookInfo(WebhookInfo.getDefaultInstance())).build();
        // ACT
        actionTemplateApplicator.addTemplateInformation("WrongSerializedActions", webhook);
    }

    /**
     * Returns workflow without changes if it is not webhook workflow.
     *
     * @throws ActionTemplateApplicationException if exception while applying template on
     *         the action happened
     */
    @Test
    public void testNotWebhookWorkflow() throws ActionTemplateApplicationException {
        // ARRANGE
        final Workflow servicenowWorkflow = Workflow.newBuilder().setId(1L).setWorkflowInfo(
                WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.SERVICENOW)
                        .setActionPhase(ActionPhase.ON_GENERATION)
                        .setTargetId(2L)).build();

        // ACT
        final Workflow resultWorkflow = actionTemplateApplicator.addTemplateInformation(
                "SerializedAction", servicenowWorkflow);

        // ASSERT
        Assert.assertEquals(servicenowWorkflow, resultWorkflow);
    }

    /**
     * Tests when the template is invalid.
     *
     * @throws ActionTemplateApplicationException if exception while applying template on
     *         the action happened
     */
    @Test
    public void testInvalidTemplate() throws ActionTemplateApplicationException {
        // ARRANGE
        expectedException.expect(ActionTemplateApplicationException.class);
        expectedException.expectMessage("Exception while applying template: Encountered \"]\" at <unknown template>");
        final String action = ActionOrchestratorTestUtils.readSampleActionApiDto();
        final Workflow webhook = Workflow.newBuilder()
                .setId(123L)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.WEBHOOK)
                        .setDisplayName("A unique name for the webhook")
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                                .setTemplate("#set ($test=]())")))
                .build();

        // ACT
        actionTemplateApplicator.addTemplateInformation(action, webhook);
    }
}
