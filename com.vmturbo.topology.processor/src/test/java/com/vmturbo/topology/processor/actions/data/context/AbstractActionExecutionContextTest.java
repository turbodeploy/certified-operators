package com.vmturbo.topology.processor.actions.data.context;

import static com.vmturbo.common.protobuf.utils.StringConstants.WEBHOOK_PASSWORD_SUBJECT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Assert;
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
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
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
    private static final String AUTH_METHOD = "BASIC";
    private static final String USER_NAME = "testUser";
    private static final String PASSWORD = "testPassword";
    private static final boolean TRUST_SELF_SIGNED_CERT = false;

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
    private static final String WEBHOOK_UNRELATED_PROP = "UNRELATED_PROP";
    private static final String WEBHOOK_UNRELATED_PROP_VALUE = "SomeVal";

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
                                .setName(WEBHOOK_UNRELATED_PROP)
                                .setValue(WEBHOOK_UNRELATED_PROP_VALUE)
                                .build())
                        .build())
                .build());

        // ACT
        AbstractActionExecutionContext context = createContext(request);

        // ASSERT
        assertTrue(context.getWorkflow().isPresent());
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(1));

        // verify webhook properties
        verifyWebhookProperty(
                context.getWorkflow().get().getPropertyList(), WEBHOOK_UNRELATED_PROP,
                WEBHOOK_UNRELATED_PROP_VALUE);
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
                                .setName(WEBHOOK_UNRELATED_PROP)
                                .setValue(WEBHOOK_UNRELATED_PROP_VALUE)
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
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(5));
        final List<Property> webhookProperties = context.getWorkflow().get().getPropertyList();

        // verify webhook properties
        verifyWebhookProperty(webhookProperties, WEBHOOK_UNRELATED_PROP,
                WEBHOOK_UNRELATED_PROP_VALUE);
        verifyWebhookProperty(webhookProperties, WebhookConstants.HTTP_METHOD, METHOD);
        verifyWebhookProperty(webhookProperties, WebhookConstants.URL, URL);
        verifyWebhookProperty(webhookProperties, WebhookConstants.TEMPLATED_ACTION_BODY,
                TEMPLATE_BODY);
        verifyWebhookProperty(webhookProperties, WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME,
                String.valueOf(TRUST_SELF_SIGNED_CERT));
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
        assertThat(context.getWorkflow().get().getPropertyCount(), equalTo(7));
        final List<Property> webhookProperties = context.getWorkflow().get().getPropertyList();

        // verify webhook properties
        verifyWebhookProperty(webhookProperties, WebhookConstants.HTTP_METHOD, METHOD);
        verifyWebhookProperty(webhookProperties, WebhookConstants.URL, URL);
        verifyWebhookProperty(webhookProperties, WebhookConstants.TEMPLATED_ACTION_BODY,
                TEMPLATE_BODY);
        verifyWebhookProperty(webhookProperties, WebhookConstants.AUTHENTICATION_METHOD,
                AUTH_METHOD);
        verifyWebhookProperty(webhookProperties, WebhookConstants.USER_NAME, USER_NAME);
        verifyWebhookProperty(webhookProperties, WebhookConstants.PASSWORD, PASSWORD);
        verifyWebhookProperty(webhookProperties,
                WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME,
                String.valueOf(TRUST_SELF_SIGNED_CERT));
    }

    private void verifyWebhookProperty(@Nonnull final List<Property> webhookProperties,
            @Nonnull final String webhookPropertyName, @Nonnull final String webhookPropertyValue) {
        final List<String> propertyValues = webhookProperties.stream()
                .filter(property -> webhookPropertyName.equals(property.getName()))
                .map(Property::getValue)
                .collect(Collectors.toList());

        if (propertyValues.size() == 1) {
            assertThat(propertyValues.get(0), equalTo(webhookPropertyValue));
        } else if (propertyValues.isEmpty()) {
            Assert.fail(String.format("There is no specified %s webhook property.", webhookPropertyName));
        } else {
            Assert.fail(String.format("More than one %s properties found!", webhookPropertyName));
        }
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