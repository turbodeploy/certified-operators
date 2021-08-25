package com.vmturbo.topology.processor.actions.data.context;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        assertEquals(context.getWorkflow().get().getPropertyList().get(0).getName(), WEBHOOK_UNRELATED_PROP);
        assertEquals(context.getWorkflow().get().getPropertyList().get(0).getValue(), WEBHOOK_UNRELATED_PROP_VALUE);
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