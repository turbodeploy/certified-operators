package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
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
    private static final long ASSOCIATED_ID_ACCT = 123123;
    private static final long ASSOCIATED_RESOURCE_GROUP_ID = 111;
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    @Test
    public void testGetActionCountsByDateResponseBuilder() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

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
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }


    @Test
    public void testGetActionCountsByDateResponseBuilderWithTwoTypes() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

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
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }

    private ActionView executableMoveAction(
                long id,
                long sourceId,
                int sourceType,
                long destId,
                int destType,
                long targetId,
                ActionState state) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(id)
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(TestActionBuilder
                .makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
            .build();

        String actionDescription = "Move VM"+targetId+" from PM"+sourceId+" to PM"+destId;
        SerializationState orchestratorAction = new SerializationState(ACTION_PLAN_ID,
            action,
            LocalDateTime.now(),
            ActionDecision.getDefaultInstance(),
            ExecutionStep.getDefaultInstance(),
            state,
            new ActionTranslation(action),
            ASSOCIATED_ID_ACCT,
            ASSOCIATED_RESOURCE_GROUP_ID,
            actionDescription.getBytes(),
                2244L);
        return spy(new Action(orchestratorAction, actionModeCalculator));
    }

    private ActionView executableActivateAction(long id, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder()
                                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                                .build())
                        .build())
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator, 2244L));
    }
}
