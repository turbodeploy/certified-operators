package com.vmturbo.action.orchestrator.action;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests that save change window got called based on expected action listener callbacks.
 */
public class ActionChangeWindowUpdaterTest {
    private final ExecutedActionsChangeWindowDao actionDao;
    private final ActionChangeWindowUpdater windowUpdater;
    private final ActionView actionView;
    private final ActionEvent actionEvent;

    private final long actionOid = 1001L;
    private final long entityOid = 2001L;

    /**
     * Testing change window updater listener.
     */
    public ActionChangeWindowUpdaterTest() {
        this.actionDao = mock(ExecutedActionsChangeWindowDao.class);
        this.actionView = mock(ActionView.class);
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(actionOid)
                .setInfo(ActionInfo.newBuilder().setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(entityOid)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .setEnvironmentType(EnvironmentType.CLOUD)
                                        .build())
                                .build())
                        .build())
                .setDeprecatedImportance(0d)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        when(actionView.getRecommendation())
                .thenReturn(action);
        this.actionEvent = mock(ActionEvent.class);
        this.windowUpdater = new ActionChangeWindowUpdater(this.actionDao);
    }

    /**
     * Successful post state and transition is true.
     *
     * @throws ActionStoreOperationException Thrown on store save error.
     */
    @Test
    public void onSuccessActionYesTransition() throws ActionStoreOperationException {
        windowUpdater.onActionEvent(actionView, ActionState.READY,
                ActionState.SUCCEEDED, actionEvent, true);
        verify(actionDao).saveExecutedAction(actionOid, entityOid);
    }

    /**
     * Successful post state but transition is true.
     */
    @Test
    public void onSuccessActionNoTransition() {
        windowUpdater.onActionEvent(actionView, ActionState.READY,
                ActionState.SUCCEEDED, actionEvent, false);
        verifyZeroInteractions(actionView);
    }

    /**
     * Failed post state but transition is true.
     */
    @Test
    public void onFailedAction() {
        windowUpdater.onActionEvent(actionView, ActionState.READY,
                ActionState.FAILED, actionEvent, true);
        verifyZeroInteractions(actionView);
    }

    /**
     * Non success post state but transition is true.
     */
    @Test
    public void onIncompleteActionYesTransition() {
        windowUpdater.onActionEvent(actionView, ActionState.READY,
                ActionState.ACCEPTED, actionEvent, true);
        verifyZeroInteractions(actionView);
    }
}
