package com.vmturbo.action.orchestrator.approval;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * Tests accepting actions.
 */
public class ActionApprovalManagerTest {

    private static long ids = 1;
    private static final long ACTION_ID = ids++;
    private static final long ACTION_RECOMMENDATION_OID = ids++;
    private static final long TARGET_ID = ids++;
    private static final long ACTION_PLAN_ID = ids++;
    private static final String EXTERNAL_USER_ID = "ExternalUserId";

    @Mock
    private ActionExecutor actionExecutor;

    @Mock
    private ActionTargetSelector actionTargetSelector;

    @Mock
    private EntitiesAndSettingsSnapshotFactory entitiesAndSettingsSnapshotFactory;

    @Mock
    private ActionTranslator actionTranslator;

    @Mock
    private WorkflowStore workflowStore;

    @Mock
    private AcceptedActionsDAO acceptedActionsDAO;

    private ActionApprovalManager actionApprovalManager;

    @Mock
    private ActionStore actionStore;

    @Mock
    private ActionModeCalculator actionModeCalculator;

    @Mock
    private EntitySeverityCache entitySeverityCache;

    @Mock
    private ActionExecutionListener actionExecutionListener;

    /**
     * Setup all the mocks.
     */
    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);

        ActionTargetInfo actionTargetInfo = mock(ActionTargetInfo.class);
        when(actionTargetInfo.supportingLevel()).thenReturn(SupportLevel.SUPPORTED);
        when(actionTargetInfo.targetId()).thenReturn(Optional.of(TARGET_ID));

        when(actionTargetSelector.getTargetForAction(any(), any(), any())).thenReturn(actionTargetInfo);

        when(actionStore.getEntitySeverityCache()).thenReturn(Optional.of(entitySeverityCache));

        when(actionTranslator.translateToSpec(any())).thenReturn(ActionSpec.newBuilder()
            .buildPartial());

        actionApprovalManager = new ActionApprovalManager(
            actionExecutor,
            actionTargetSelector,
            entitiesAndSettingsSnapshotFactory,
            actionTranslator,
            workflowStore, acceptedActionsDAO, actionExecutionListener);
    }

    /**
     * An external action should be accepted an start executing.
     *
     * @throws ExecutionInitiationException if something goes wrong.
     */
    @Test
    public void testAcceptExternalApproval() throws ExecutionInitiationException {
        Action action = new MockedAction(
            ActionDTO.Action.newBuilder()
                .setId(ACTION_ID)
                .buildPartial(),
            ACTION_PLAN_ID,
            ACTION_RECOMMENDATION_OID);
        action.getActionTranslation().setTranslationSuccess(
            ActionDTO.Action.newBuilder().buildPartial());
        actionApprovalManager.attemptAndExecute(actionStore, EXTERNAL_USER_ID, action);
        // after accepting, the action should have transitioned from READY to IN_PROGRESS
        Assert.assertEquals(ActionState.IN_PROGRESS, action.getState());
    }

    /**
     * Test failed acceptance for action not in {@link ActionState#READY} state.
     *
     * @throws ExecutionInitiationException if something goes wrong.
     */
    @Test
    public void testFailedAcceptExternalApproval() throws ExecutionInitiationException {
        final Action action =
                new MockedAction(ActionDTO.Action.newBuilder().setId(ACTION_ID).buildPartial(),
                        ACTION_PLAN_ID, ACTION_RECOMMENDATION_OID);
        action.getActionTranslation()
                .setTranslationSuccess(ActionDTO.Action.newBuilder().buildPartial());
        when(actionStore.getAction(ACTION_ID)).thenReturn(Optional.of(action));
        actionApprovalManager.attemptAndExecute(actionStore, EXTERNAL_USER_ID, action);
        Assert.assertEquals(ActionState.IN_PROGRESS, action.getState());

        try {
            actionApprovalManager.attemptAndExecute(actionStore, EXTERNAL_USER_ID, action);
        } catch (ExecutionInitiationException ex) {
            Assert.assertThat(ex.getMessage(), Matchers.containsString(
                "Only action with READY state can be accepted. Action " + ACTION_ID + " has "
                    + ActionState.IN_PROGRESS + " state."));
            return;
        }
        fail("The call show have thrown an exception");

    }

    /**
     * Test postpone action execution for accepted action when execution window is not
     * active.
     *
     * @throws ExecutionInitiationException if something goes wrong.
     */
    @Test
    public void testPostponeExecutionForExternalApprovedAction() throws ExecutionInitiationException {
        final ActionSchedule actionSchedule = Mockito.spy(
                new ActionSchedule(1L, 2L, "America/Chicago", 12L, "testSchedule",
                        ActionMode.MANUAL, "admin"));
        // isActiveSchedule() return status of action schedule updated during previous
        // broadcast, so it is not related to real time and can be different from isActiveScheduleNow()
        Mockito.when(actionSchedule.isActiveSchedule()).thenReturn(true);
        Mockito.when(actionSchedule.isActiveScheduleNow()).thenReturn(false);

        final Action action = Mockito.spy(
                new MockedAction(ActionDTO.Action.newBuilder().setId(ACTION_ID).buildPartial(),
                        ACTION_PLAN_ID, ACTION_RECOMMENDATION_OID));
        Mockito.when(action.getSchedule()).thenReturn(Optional.of(actionSchedule));
        Mockito.when(action.getAssociatedSettingsPolicies())
                .thenReturn(Collections.singletonList(224L));
        action.getActionTranslation()
                .setTranslationSuccess(ActionDTO.Action.newBuilder().buildPartial());
        when(actionStore.getAction(ACTION_ID)).thenReturn(Optional.of(action));

        actionApprovalManager.attemptAndExecute(actionStore, EXTERNAL_USER_ID, action);
        Mockito.verify(action, Mockito.never()).receive(Mockito.eq(new QueuedEvent()));
        Assert.assertEquals(ActionState.ACCEPTED, action.getState());
    }

    /**
     * Special mocked action so I can override getMode() used by Action.modePermitsExecution().
     * Using a spy is not enough. Spy works for objects that try to access getMode() thru the mocked
     * object. However, there are hardcoded references to the internal Action object used by
     * ActionStateMachine and the Action itself.
     */
    public class MockedAction extends Action {

        /**
         * Creates a mocked instance using the mocked actionModeCalculator from
         * ActionApprovalManagerTest.
         *
         * @param recommendation the nested ActionDTO.Action recommendation.
         * @param actionPlanId the id of the plan this action is part of.
         * @param recommendationOid the oid that represents the effect of this action.
         */
        public MockedAction(@Nonnull final ActionDTO.Action recommendation,
                      final long actionPlanId,
                      long recommendationOid) {
            super(recommendation, actionPlanId,
                ActionApprovalManagerTest.this.actionModeCalculator,
                recommendationOid);
        }

        /**
         * Replace the mode with external approval.
         *
         * @return ActionMode.EXTERNAL_APPROVAL.
         */
        @Override
        public ActionMode getMode() {
            return ActionMode.EXTERNAL_APPROVAL;
        }
    }
}
