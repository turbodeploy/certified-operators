package com.vmturbo.action.orchestrator.execution;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.ActionExecution;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionMoles.ActionExecutionServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    /**
     * The class under test
     */
    private ActionExecutor actionExecutor;

    private static final long TARGET_ID = 7L;

    private final ActionExecutionServiceMole actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceMole());

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Mock
    private WorkflowStore workflowStore;

    @Mock
    private ActionTranslator actionTranslator;

    @Mock
    private ActionExecutionListener actionExecutionListener;

    @Mock
    private ActionExecutionStore actionExecutionStore;

    @Rule
    public final GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Optional<WorkflowDTO.Workflow> workflowOpt = Optional.empty();


    private final long targetEntityId = 1L;

    private final ActionDTO.ActionSpec testAction = ActionDTO.ActionSpec.newBuilder()
        .setRecommendation(
            testActionBuilder
                .buildMoveAction(targetEntityId, 2L, 1, 3L, 1))
        .build();

    @Mock
    private Action testActionObj;

    private final List<ActionWithWorkflow> actionList = Collections.singletonList(
            new ActionWithWorkflow(testAction, workflowOpt));

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
    private final ActionTemplateApplicator actionTemplateApplicator = mock(ActionTemplateApplicator.class);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(testActionObj.getId()).thenReturn(testAction.getRecommendation().getId());
        when(testActionObj.getRecommendation()).thenReturn(testAction.getRecommendation());
        when(testActionObj.getRecommendationOid()).thenReturn(2001L);
        // license check client by default will act as if a valid license is installed.
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(true);
        actionExecutionStore = Mockito.mock(ActionExecutionStore.class);
        // The class under test
        actionExecutor = new ActionExecutor(server.getChannel(), actionExecutionStore,
                10, TimeUnit.SECONDS, licenseCheckClient,
                actionTemplateApplicator, workflowStore, actionTranslator);
    }

    /**
     * Test starting an asynchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMove() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionStartLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionStartLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        // wait a bit so the execution get started before sending update for the action
        executionStartLatch.await();

        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.SUCCEEDED);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, never()).onActionFailure(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());

        verify(actionExecutionBackend).executeAction(actionSpecCaptor.capture(), any());
        final ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        ActionDTO.ActionInfo info = sentSpec.getActionSpec().getRecommendation().getInfo();
        assertTrue(sentSpec.hasActionSpec());
        Assert.assertEquals(ActionTypeCase.MOVE, info.getActionTypeCase());
        final Move move = info.getMove();
        Assert.assertEquals(TARGET_ID, sentSpec.getTargetId());
        Assert.assertEquals(targetEntityId, move.getTarget().getId());
        Assert.assertEquals(1, move.getChangesCount());
        Assert.assertEquals(2, move.getChanges(0).getSource().getId());
        Assert.assertEquals(3, move.getChanges(0).getDestination().getId());
    }

    /**
     * Test timing out of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveTimeout() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        boolean successful = executionCompletionLatch.await(20, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, times(1)).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test success of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveSucceed() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionStartLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionStartLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        // wait a bit so the execution get started before sending update for the action
        executionStartLatch.await();

        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.SUCCEEDED);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, never()).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test failure of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveFailed() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionStartLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionStartLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        executionStartLatch.await();

        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.FAILED);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, never()).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test execution of an action that has multiple steps.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMultiStepExecution() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionFirstLatch = new CountDownLatch(1);
        final CountDownLatch executionSecondLatch = new CountDownLatch(1);
        final MutableInt executionCounter = new MutableInt(0);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            if (executionCounter.intValue() == 0) {
                executionFirstLatch.countDown();
            } else {
                executionSecondLatch.countDown();
            }
            executionCounter.increment();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        ActionTranslation actionTranslation = Mockito.mock(ActionTranslation.class);
        when(testActionObj.getActionTranslation()).thenReturn(actionTranslation);
        when(actionTranslation.getTranslatedRecommendation())
                .thenReturn(Optional.of(testAction.getRecommendation()));
        ExecutableStep executableStep = Mockito.mock(ExecutableStep.class);
        when(testActionObj.getCurrentExecutableStep()).thenReturn(Optional.of(executableStep));
        when(executableStep.getTargetId()).thenReturn(TARGET_ID);
        when(actionTranslator.translateToSpec(any())).thenReturn(testAction);
        when(testActionObj.getWorkflow(any(), any())).thenReturn(Optional.empty());

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        executionFirstLatch.await();
        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.POST_IN_PROGRESS);
        executionSecondLatch.await();
        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.SUCCEEDED);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, never()).onActionFailure(any());
        verify(actionExecutionBackend, times(2)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test execution of an action that has multiple steps and second step fails because of
     * workflow store exceptions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMultiStepExecutionSecondStepFailsBecauseOfWorkflowStore() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionFirstLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionFirstLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Mockito.doAnswer(invocationOnMock -> {
            actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.FAILED);
            return null;
        }).when(actionExecutionListener)
        .onActionFailure(any());

        ActionTranslation actionTranslation = Mockito.mock(ActionTranslation.class);
        when(testActionObj.getActionTranslation()).thenReturn(actionTranslation);
        when(actionTranslation.getTranslatedRecommendation())
                .thenReturn(Optional.of(testAction.getRecommendation()));
        ExecutableStep executableStep = Mockito.mock(ExecutableStep.class);
        when(testActionObj.getCurrentExecutableStep()).thenReturn(Optional.of(executableStep));
        when(executableStep.getTargetId()).thenReturn(TARGET_ID);
        when(actionTranslator.translateToSpec(any())).thenReturn(testAction);
        when(testActionObj.getWorkflow(any(), any())).thenThrow(new WorkflowStoreException("Failed"));

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        executionFirstLatch.await();
        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.POST_IN_PROGRESS);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, times(1)).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test execution of an action that has multiple steps and second step fails because of
     * action translation is not available.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMultiStepExecutionSecondStepFailsTranslationIsNotAvailable() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionFirstLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionFirstLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Mockito.doAnswer(invocationOnMock -> {
                    actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.FAILED);
                    return null;
                }).when(actionExecutionListener)
                .onActionFailure(any());

        ActionTranslation actionTranslation = Mockito.mock(ActionTranslation.class);
        when(testActionObj.getActionTranslation()).thenReturn(actionTranslation);
        when(actionTranslation.getTranslatedRecommendation())
                .thenReturn(Optional.empty());
        ExecutableStep executableStep = Mockito.mock(ExecutableStep.class);
        when(testActionObj.getCurrentExecutableStep()).thenReturn(Optional.of(executableStep));
        when(executableStep.getTargetId()).thenReturn(TARGET_ID);
        when(actionTranslator.translateToSpec(any())).thenReturn(testAction);
        when(testActionObj.getWorkflow(any(), any())).thenReturn(Optional.empty());

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        executionFirstLatch.await();
        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.POST_IN_PROGRESS);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, times(1)).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test execution of an action that has multiple steps and second step fails because of
     * next execution step is not available.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMultiStepExecutionSecondStepFailsNoNextExecutionStep() throws Exception {
        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionFirstLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeAction(any())).thenAnswer(invocationOnMock -> {
            executionFirstLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Mockito.doAnswer(invocationOnMock -> {
                    actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.FAILED);
                    return null;
                }).when(actionExecutionListener)
                .onActionFailure(any());

        ActionTranslation actionTranslation = Mockito.mock(ActionTranslation.class);
        when(testActionObj.getActionTranslation()).thenReturn(actionTranslation);
        when(actionTranslation.getTranslatedRecommendation())
                .thenReturn(Optional.of(testAction.getRecommendation()));
        ExecutableStep executableStep = Mockito.mock(ExecutableStep.class);
        when(testActionObj.getCurrentExecutableStep()).thenReturn(Optional.empty());
        when(executableStep.getTargetId()).thenReturn(TARGET_ID);
        when(actionTranslator.translateToSpec(any())).thenReturn(testAction);
        when(testActionObj.getWorkflow(any(), any())).thenReturn(Optional.empty());

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, actionList, actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        executionFirstLatch.await();
        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.POST_IN_PROGRESS);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, times(1)).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeAction(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
    }

    /**
     * Test execution of multiple actions together.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMultipleActionExecution() throws Exception {
        final ActionDTO.ActionSpec testAction2 = ActionDTO.ActionSpec.newBuilder()
            .setRecommendation(
                testActionBuilder
                        .buildMoveAction(targetEntityId, 4L, 1, 5L, 1))
            .build();

        Action testActionObj2 = mock(Action.class);
        when(testActionObj2.getId()).thenReturn(testAction2.getRecommendation().getId());
        when(testActionObj2.getRecommendation()).thenReturn(testAction2.getRecommendation());
        when(testActionObj2.getRecommendationOid()).thenReturn(2002L);

        final CountDownLatch executionCompletionLatch = new CountDownLatch(1);
        final CountDownLatch executionStartLatch = new CountDownLatch(1);

        when(actionExecutionBackend.executeActionList(any())).thenAnswer(invocationOnMock -> {
            executionStartLatch.countDown();
            return ActionExecution.ExecuteActionResponse.getDefaultInstance();
        });

        Runnable runnable = () -> {
            try {
                actionExecutor.executeSynchronously(TARGET_ID, Arrays.asList(
                    new ActionWithWorkflow(testAction, workflowOpt),
                    new ActionWithWorkflow(testAction2, workflowOpt)), actionExecutionListener);
            } catch (Exception exception) {
                fail("Failed with exception. " + exception.getMessage());
            } finally {
                executionCompletionLatch.countDown();
            }
        };
        new Thread(runnable).start();

        // wait a bit so the execution get started before sending update for the action
        executionStartLatch.await();

        actionExecutor.onActionUpdate(testActionObj, ActionDTO.ActionState.SUCCEEDED);
        actionExecutor.onActionUpdate(testActionObj2, ActionDTO.ActionState.SUCCEEDED);

        boolean successful = executionCompletionLatch.await(5, TimeUnit.SECONDS);

        if (!successful) {
            fail("The execution for the action did not finish.");
        }

        verify(actionExecutionListener, never()).onActionFailure(any());
        verify(actionExecutionBackend, times(1)).executeActionList(any());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj.getId());
        verify(actionExecutionStore, times(1))
                .removeCompletedAction(testActionObj2.getId());
    }


    /**
     * Verify that an action can't be completed when the license is invalid.
     */
    @Test(expected = ExecutionStartException.class)
    public void testActionWithInvalidLicense() throws ExecutionStartException {
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(false);
        actionExecutor.execute(TARGET_ID, testAction, workflowOpt);
    }
}
