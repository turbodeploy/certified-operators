package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Integration tests related to the LiveActionStore.
 */
public class LiveActionStoreTest {

    private final long topologyId = 0xDEADEEF;
    private final long firstPlanId = 0xBEADED;
    private final long secondPlanId = 0xDADDA;

    private final long vm1 = 1;
    private final long vm2 = 2;
    private final long vm3 = 3;

    private final long hostA = 0xA;
    private final long hostB = 0xB;
    private final long hostC = 0xC;
    private final long hostD = 0xD;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Permit spying on actions inserted into the store so that their state can be mocked
     * out for testing purposes.
     */
    private class SpyActionFactory implements IActionFactory {
        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Action newAction(@Nonnull final ActionDTO.Action recommendation, long actionPlanId) {
            return spy(new Action(recommendation, actionPlanId));
        }

        @Nonnull
        @Override
        public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                                @Nonnull final EntitySettingsCache entitySettingsMap,
                                long actionPlanId){
            return spy(new Action(recommendation, entitySettingsMap, actionPlanId));
        }

        @Nonnull
        @Override
        public Action newAction(@Nonnull ActionDTO.Action recommendation, @Nonnull LocalDateTime recommendationTime, long actionPlanId) {
            return spy(new Action(recommendation, recommendationTime, actionPlanId));
        }
    }

    private static final long TOPOLOGY_CONTEXT_ID = 123456;

    private final ActionTranslator actionTranslator = mock(ActionTranslator.class);

    private final ActionSupportResolver filter = Mockito.mock(ActionSupportResolver.class);

    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);

    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private ActionStore actionStore;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws TargetResolutionException, UnsupportedActionException {
        actionStore = new LiveActionStore(spyActionFactory, TOPOLOGY_CONTEXT_ID, filter,
                entitySettingsCache);
        when(entitySettingsCache.getTypeForEntity(anyLong())).thenReturn(Optional.empty());

        when(filter.resolveActionsSupporting(anyCollection())).thenAnswer(invocationOnMock
                -> invocationOnMock.getArguments()[0]);
        filter.resolveActionsSupporting(new LinkedList<>());
        IdentityGenerator.initPrefix(0);

        when(actionTranslator.translate(any(Stream.class))).thenAnswer(invocationOnMock ->
            invocationOnMock.getArguments()[0]);
    }

    private static ActionDTO.Action.Builder move(long targetId, long sourceId, long destinationId) {
        return ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),
            targetId, sourceId, destinationId).toBuilder();
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, hostB))
            .addAction(move(vm2, hostB, hostC))
            .build();

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulateWithRepeatsAddsDuplicates() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, hostB))
            .addAction(move(vm1, hostA, hostB))
            .build();

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulatePreservesReRecommended() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, hostB);
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, hostB))
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(1, actionStore.size());
        assertTrue(actionStore.getAction(firstMove.getId()).isPresent());
    }

    @Test
    public void testPopulateNotRecommendedAreClearedAndRemoved() throws Exception {
        // Can't use spies when checking for action state because action state machine will call
        // methods in the original action, not in the spy.
        ActionStore actionStore =
                new LiveActionStore(new ActionFactory(), TOPOLOGY_CONTEXT_ID, filter,
                        entitySettingsCache);

        ActionDTO.Action.Builder firstMove = move(vm1, hostA, hostB);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        Action actionToClear = actionStore.getAction(firstMove.getId()).get();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
        assertEquals(ActionState.CLEARED, actionToClear.getState());
    }

    @Test
    public void testPopulateQueuedInProgressAreNotCleared() throws Exception {
        ActionDTO.Action.Builder queuedMove = move(vm1, hostA, hostB);
        ActionDTO.Action.Builder inProgressMove = move(vm2, hostA, hostB);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(queuedMove)
            .addAction(inProgressMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulateQueuedInProgressNotDuplicated() throws Exception {
        ActionDTO.Action.Builder queuedMove = move(vm1, hostA, hostB);
        ActionDTO.Action.Builder inProgressMove = move(vm2, hostA, hostB);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(queuedMove)
            .addAction(inProgressMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);

        ActionDTO.Action.Builder reRecommendQueued = move(vm1, hostA, hostB);
        ActionDTO.Action.Builder reRecommendInProgress = move(vm2, hostA, hostB);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .addAction(reRecommendInProgress)
            .addAction(reRecommendQueued)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(2, actionStore.size());
        assertTrue(actionStore.getActionView(queuedMove.getId()).isPresent());
        assertTrue(actionStore.getActionView(inProgressMove.getId()).isPresent());
    }

    @Test
    public void testPopulateClearedSucceededFailedAreRemoved() throws Exception {
        ActionDTO.Action.Builder successMove = move(vm3, hostA, hostB);
        ActionDTO.Action.Builder failedMove = move(vm1, hostB, hostC);
        ActionDTO.Action.Builder clearedMove = move(vm2, hostC, hostD);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(successMove)
            .addAction(failedMove)
            .addAction(clearedMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(successMove.getId()).get().getState()).thenReturn(ActionState.SUCCEEDED);
        when(actionStore.getAction(failedMove.getId()).get().getState()).thenReturn(ActionState.FAILED);
        when(actionStore.getAction(clearedMove.getId()).get().getState()).thenReturn(ActionState.CLEARED);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
    }

    @Test
    public void testPopulateOneDuplicateReRecommended() {
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, hostB))
            .addAction(move(vm1, hostA, hostB))
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, hostB))
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(1, actionStore.size());
    }

    @Test
    public void testPopulateReRecommendedWithAdditionalDuplicate() {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, hostB);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, hostB);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, hostB))
            .addAction(move(vm1, hostA, hostB))
            .addAction(move(vm1, hostA, hostB))
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(3, actionStore.size());
        assertTrue(actionStore.getAction(firstMove.getId()).isPresent());
        assertTrue(actionStore.getAction(secondMove.getId()).isPresent());
    }

    @Test
    public void testPopulation() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, hostB);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, hostB);

        ActionPlan plan = ActionPlan.newBuilder()
            .setTopologyId(topologyId)
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        actionStore.populateRecommendedActions(plan);
        assertThat(actionStore.getActionViews().values().stream()
                .map(spec -> spec.getRecommendation().getId())
                .collect(Collectors.toList()),
            containsInAnyOrder(firstMove.getId(), secondMove.getId()));
    }

    @Test
    public void testClearThrowsIllegalStateException() {
        expectedException.expect(IllegalStateException.class);
        actionStore.clear();
    }

    @Test
    public void testGetTopologyContextId() {
        assertEquals(TOPOLOGY_CONTEXT_ID, actionStore.getTopologyContextId());
    }

    @Test
    public void testGetEntitySettings() {
        ActionPlan plan = ActionPlan.newBuilder()
                .setTopologyId(topologyId)
                .setId(firstPlanId)
                .addAction(move(vm1, hostA, hostB))
                .build();

        actionStore.populateRecommendedActions(plan);
        verify(entitySettingsCache).update(eq(ImmutableSet.of(vm1, hostA, hostB)),
                eq(plan.getTopologyContextId()), eq(plan.getTopologyId()));
        verify(spyActionFactory).newAction(any(),
                eq(entitySettingsCache),
                eq(firstPlanId));
        assertEquals(1, actionStore.size());
    }
}