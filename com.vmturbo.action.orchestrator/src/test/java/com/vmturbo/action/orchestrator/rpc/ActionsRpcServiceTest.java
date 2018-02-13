package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.Builder;

/**
 * test helper methods {@link ActionsRpcService#getActionCountsByDateResponseBuilder}.
 */
public class ActionsRpcServiceTest {
    private static final long ACTION_PLAN_ID = 9876;

    @Test
    public void testGetActionCountsByDateResponseBuilder() throws Exception {
        final ActionView actionView1 = executableMoveAction(123L, 1L, 2L, 10L, ActionState.SUCCEEDED);
        final ActionView actionView2 = executableMoveAction(124L, 1L, 3L, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 = executableMoveAction(125L, 4L, 2L, 12L, ActionState.FAILED);

        List<ActionView> actionViewList = ImmutableList.of(actionView1, actionView2, actionView3);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList, k2, actionViewList);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
        // one is mode = manual, state = succeeded, the second is mode = manual, state = failed
        assertEquals(2, builder.getActionCountsByDateBuilderList().get(0).getCountsByStateAndModeCount());
    }


    @Test
    public void testGetActionCountsByDateResponseBuilderWithTwoTypes() throws Exception {
        final ActionView actionView1 = executableMoveAction(123L, 1L, 2L, 10L, ActionState.SUCCEEDED);
        final ActionView actionView2 = executableMoveAction(124L, 1L, 3L, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 = executableMoveAction(125L, 4L, 2L, 12L, ActionState.FAILED);

        final ActionView actionView4 = executableActivateAction(126L, 13L);
        final ActionView actionView5 = executableActivateAction(127L, 14L);

        List<ActionView> actionViewList1 = ImmutableList.of(
                actionView1, actionView2, actionView3);
        List<ActionView> actionViewList2 = ImmutableList.of(
                actionView4, actionView5);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList1, k2, actionViewList2);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(2, builder.getActionCountsByDateBuilderList().size());
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
    }

    private ActionView executableMoveAction(long id, long sourceId, long destId, long targetId, ActionState state) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionTest.makeMoveInfo(targetId, sourceId, destId))
                .build();

        SerializationState orchesratorAction = new SerializationState(ACTION_PLAN_ID,
                action,
                LocalDateTime.now(),
                ActionDecision.getDefaultInstance(),
                ExecutionStep.getDefaultInstance(),
                state,
                new ActionTranslation(action));
        return spy(new Action(orchesratorAction));
    }

    private ActionView executableActivateAction(long id, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder()
                                .setTargetId(targetId))
                ).build();

        return spy(new Action(action, ACTION_PLAN_ID));
    }
}
