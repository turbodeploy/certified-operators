package com.vmturbo.action.orchestrator.execution;

import static org.mockito.Matchers.any;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionMoles.ActionExecutionServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    /**
     * The class under test
     */
    private ActionExecutor actionExecutor;

    private final long probeId = 10;
    private final long targetId = 7;

    private final ActionExecutionServiceMole actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceMole());

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Rule
    public final GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Optional<WorkflowDTO.Workflow> workflowOpt = Optional.empty();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        // The class under test
        actionExecutor = new ActionExecutor(server.getChannel());
    }

    @Test
    public void testMove() {
        final long targetEntityId = 1L;

        final ActionDTO.Action action =
                testActionBuilder.buildMoveAction(targetEntityId, 2L, 1, 3L, 1);

        try {
            actionExecutor.execute(targetId, action, workflowOpt);
        } catch (ExecutionStartException e) {
            // We expect this to happen, since the backend implementation
            // is not implemented.
        }

        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        Mockito.verify(actionExecutionBackend).executeAction(actionSpecCaptor.capture(), any());
        final ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        Assert.assertTrue(sentSpec.hasActionInfo());
        Assert.assertEquals(ActionTypeCase.MOVE, sentSpec.getActionInfo().getActionTypeCase());
        final Move move = sentSpec.getActionInfo().getMove();
        Assert.assertEquals(targetId, sentSpec.getTargetId());
        Assert.assertEquals(targetEntityId, move.getTarget().getId());
        Assert.assertEquals(1, move.getChangesCount());
        Assert.assertEquals(2, move.getChanges(0).getSource().getId());
        Assert.assertEquals(3, move.getChanges(0).getDestination().getId());
    }

}
