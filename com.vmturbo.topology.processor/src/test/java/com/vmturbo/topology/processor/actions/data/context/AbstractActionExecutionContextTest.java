package com.vmturbo.topology.processor.actions.data.context;

import static com.vmturbo.common.protobuf.utils.StringConstants.WEBHOOK_PASSWORD_SUBJECT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests {@link AbstractActionExecutionContext}.
 */
public class AbstractActionExecutionContextTest {

    private static final long TARGET_ID = 7L;
    private static final long WORKFLOW_ID = 8L;
    private static final long ACTION_ID = 100L;

    private static final String URL = "http://www.turbonomic.com/test";
    private static final String METHOD = "PUT";
    private static final String TEMPLATE_BODY = "{json:$actionId}";
    private static final String USER_NAME = "testUser";
    private static final String PASSWORD = "testPassword";

    private static final ActionDTO.ActionSpec testAction = ActionDTO.ActionSpec.newBuilder()
            .setRecommendation(
                    ActionDTO.Action.newBuilder().setId(ACTION_ID).setDeprecatedImportance(1)
                            .setExplanation(ActionDTO.Explanation.newBuilder().build())
                            .setInfo(ActionDTO.ActionInfo.newBuilder()
                                    .setMove(ActionDTO.Move.newBuilder()
                                            .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                    .setId(3000L)
                                                    .setType(10)
                                                    .build())
                                            .addChanges(ActionDTO.ChangeProvider.newBuilder()
                                                    .setSource(ActionDTO.ActionEntity.newBuilder()
                                                            .setId(2000)
                                                            .setType(14)
                                                            .build())
                                                    .setDestination(ActionDTO.ActionEntity.newBuilder()
                                                            .setId(2001)
                                                            .setType(14)
                                                            .build())
                                                    .build()))
                                    .build())
                            .setSupportingLevel(ActionDTO.Action.SupportLevel.SUPPORTED)
                            .build())
            .build();

    @Mock
    private TargetStore targetStore;

    @Mock
    private ProbeStore probeStore;

    @Mock
    private EntityStore entityStore;

    @Mock
    private EntityRetriever entityRetriever;

    @Mock
    private ActionDataManager actionDataManager;

    @Mock
    private GroupAndPolicyRetriever groupAndPolicyRetriever;

    @Mock
    private SecureStorageClient secureStorageClientMock;

    private static ExecuteActionRequest createExecutionRequest(Workflow workflow) {
        return ExecuteActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setActionSpec(testAction)
                .setActionType(ActionDTO.ActionType.MOVE)
                .setTargetId(TARGET_ID)
                .setWorkflow(workflow)
                .build();
    }

    /**
     * Initializes the mocks.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * When an action has a workflow with no properties, the converted workflow should not have any properties.
     *
     * @throws ContextCreationException if failed to create action context.
     */
    @Test
    public void testWorkflowWithNoProperties() throws ContextCreationException {
        // ARRANGE
        ExecuteActionRequest request = createExecutionRequest(Workflow.newBuilder()
                .setId(WORKFLOW_ID)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setName("TestWf")
                        .build())
                .build());

        // ACT
        AbstractActionExecutionContext context = createContext(request);

        // ASSERT
        assertTrue(context.getWorkflow().isPresent());
        assertThat(context.getWorkflow().get().getParamCount(), equalTo(0));
    }

    /**
     * When an action has a workflow that has a property that property should be present in the converted workflow.
     *
     * @throws ContextCreationException if failed to create action context.
     */
    @Test
    public void testWorkflowWithUnrelatedProperty() throws ContextCreationException {
        // ARRANGE
        ExecuteActionRequest request = createExecutionRequest(Workflow.newBuilder()
                .setId(WORKFLOW_ID)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setName("Testwf")
                        .addWorkflowProperty(WorkflowDTO.WorkflowProperty.newBuilder()
                                .setName("UNRELATED_PROP")
                                .setValue("SomeVal")
                                .build())
                        .build())
                .build());

        // ACT
        AbstractActionExecutionContext context = createContext(request);

        // ASSERT
        assertTrue(context.getWorkflow().isPresent());
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(1));
        assertThat(context.getWorkflow().get().getProperty(0).getName(), equalTo("UNRELATED_PROP"));
        assertThat(context.getWorkflow().get().getProperty(0).getValue(), equalTo("SomeVal"));
    }

    /**
     * Test converting a webhook workflow.
     *
     * @throws ContextCreationException if failed to create action context.
     */
    @Test
    public void testConvertWebhookWorkflow() throws ContextCreationException {
        ExecuteActionRequest request = createExecutionRequest(Workflow.newBuilder()
                .setId(WORKFLOW_ID)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setName("Testwf")
                        .addWorkflowProperty(WorkflowDTO.WorkflowProperty.newBuilder()
                                .setName("UNRELATED_PROP")
                                .setValue("SomeVal")
                                .build())
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                                .setHttpMethod(WorkflowInfo.WebhookInfo.HttpMethod.PUT)
                                .setUrl(URL)
                                .setTemplate(TEMPLATE_BODY)
                                .build())
                        .build())
                .build());

        // ACT
        AbstractActionExecutionContext context = createContext(request);

        // ASSERT
        assertTrue(context.getWorkflow().isPresent());
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(4));
        assertThat(context.getWorkflow().get().getProperty(0).getName(), equalTo("UNRELATED_PROP"));
        assertThat(context.getWorkflow().get().getProperty(0).getValue(), equalTo("SomeVal"));
        assertThat(context.getWorkflow().get().getProperty(1).getName(), equalTo(WebhookConstants.HTTP_METHOD));
        assertThat(context.getWorkflow().get().getProperty(1).getValue(), equalTo(METHOD));
        assertThat(context.getWorkflow().get().getProperty(2).getName(), equalTo(WebhookConstants.URL));
        assertThat(context.getWorkflow().get().getProperty(2).getValue(), equalTo(URL));
        assertThat(context.getWorkflow().get().getProperty(3).getName(), equalTo(WebhookConstants.TEMPLATED_ACTION_BODY));
        assertThat(context.getWorkflow().get().getProperty(3).getValue(), equalTo(TEMPLATE_BODY));
    }

    /**
     * Test converting a webhook workflow with basic auth.
     *
     * @throws ContextCreationException if failed to create action context.
     * @throws CommunicationException   if something goes wrong.
     */
    @Test
    public void testConvertWebhookWorkflowWithBasicAuth() throws ContextCreationException, CommunicationException {
        ExecuteActionRequest request = createExecutionRequest(Workflow.newBuilder()
                .setId(WORKFLOW_ID)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setName("Testwf")
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                                .setHttpMethod(WorkflowInfo.WebhookInfo.HttpMethod.PUT)
                                .setUrl(URL)
                                .setTemplate(TEMPLATE_BODY)
                                .setAuthenticationMethod(WorkflowInfo.WebhookInfo.AuthenticationMethod.BASIC)
                                .setUsername(USER_NAME)
                                .build())
                        .build())
                .build());

        when(secureStorageClientMock.getValue(WEBHOOK_PASSWORD_SUBJECT, "8")).thenReturn(Optional.of(PASSWORD));

        // ACT
        AbstractActionExecutionContext context = createContext(request);

        // ASSERT
        assertTrue(context.getWorkflow().isPresent());
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(6));
        assertThat(context.getWorkflow().get().getProperty(0).getName(), equalTo(WebhookConstants.HTTP_METHOD));
        assertThat(context.getWorkflow().get().getProperty(0).getValue(), equalTo(METHOD));
        assertThat(context.getWorkflow().get().getProperty(1).getName(), equalTo(WebhookConstants.URL));
        assertThat(context.getWorkflow().get().getProperty(1).getValue(), equalTo(URL));
        assertThat(context.getWorkflow().get().getProperty(2).getName(), equalTo(WebhookConstants.TEMPLATED_ACTION_BODY));
        assertThat(context.getWorkflow().get().getProperty(2).getValue(), equalTo(TEMPLATE_BODY));
        assertThat(context.getWorkflow().get().getProperty(3).getName(), equalTo(WebhookConstants.AUTHENTICATION_METHOD));
        assertThat(context.getWorkflow().get().getProperty(3).getValue(), equalTo("BASIC"));
        assertThat(context.getWorkflow().get().getProperty(4).getName(), equalTo(WebhookConstants.USER_NAME));
        assertThat(context.getWorkflow().get().getProperty(4).getValue(), equalTo(USER_NAME));
        assertThat(context.getWorkflow().get().getProperty(5).getName(), equalTo(WebhookConstants.PASSWORD));
        assertThat(context.getWorkflow().get().getProperty(5).getValue(), equalTo(PASSWORD));
    }

    private AbstractActionExecutionContext createContext(ExecuteActionRequest request) throws ContextCreationException {
        return new AbstractActionExecutionContext(request, actionDataManager, entityStore,
                entityRetriever, targetStore, probeStore, groupAndPolicyRetriever, secureStorageClientMock) {
            @Override
            protected long getPrimaryEntityId() {
                return 0;
            }
        };
    }

}