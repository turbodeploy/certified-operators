package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.LiveActions.RecommendationTracker;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
    private final int vmType = 1;

    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);


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
            return spy(new Action(recommendation, actionPlanId, actionModeCalculator));
        }

        @Nonnull
        @Override
        public Action newAction(@Nonnull ActionDTO.Action recommendation, @Nonnull LocalDateTime recommendationTime,
                                long actionPlanId) {
            return spy(new Action(recommendation, recommendationTime, actionPlanId, actionModeCalculator));
        }
    }

    private static final long TOPOLOGY_CONTEXT_ID = 123456;

    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();

    private final ActionTargetSelector targetSelector = Mockito.mock(ActionTargetSelector.class);

    private final ProbeCapabilityCache probeCapabilityCache = Mockito.mock(ProbeCapabilityCache.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private ActionStore actionStore;

    private LiveActionsStatistician actionsStatistician = mock(LiveActionsStatistician.class);

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    private Clock clock = new MutableFixedClock(1_000_000);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws TargetResolutionException, UnsupportedActionException {
        actionStore = new LiveActionStore(spyActionFactory, TOPOLOGY_CONTEXT_ID, targetSelector,
            probeCapabilityCache, entitySettingsCache, actionHistoryDao, actionsStatistician,
            actionTranslator, clock, userSessionContext);

        when(targetSelector.getTargetsForActions(any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions
                .collect(Collectors.toMap(ActionDTO.Action::getId, action -> ImmutableActionTargetInfo.builder()
                    .supportingLevel(SupportLevel.SUPPORTED)
                    .build()));
        });
        setEntitiesOIDs();
        IdentityGenerator.initPrefix(0);
    }

    private static ActionDTO.Action.Builder move(long targetId,
                                                 long sourceId, int sourceType,
                                                 long destinationId, int destinationType) {
        return ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),
            targetId, sourceId, sourceType, destinationId, destinationType).toBuilder();
    }

    public void setEntitiesOIDs() {
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getEntityFromOid(eq(vm1)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm1,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm2)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm2,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm3)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm3,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostA)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostA,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostB)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostB,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostC)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostC,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostD)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostD,
                EntityType.PHYSICAL_MACHINE.getNumber()));
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm2, hostB, vmType, hostC, vmType))
            .build();

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulateWithRepeatsAddsDuplicates() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulatePreservesReRecommended() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
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
        ActionStore actionStore = new LiveActionStore(
                new ActionFactory(actionModeCalculator), TOPOLOGY_CONTEXT_ID,
                targetSelector, probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                actionsStatistician, actionTranslator, clock, userSessionContext);

        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        Action actionToClear = actionStore.getAction(firstMove.getId()).get();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
        assertEquals(ActionState.CLEARED, actionToClear.getState());
    }

    @Test
    public void testPopulateInProgressAreNotCleared() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder inProgressMove =
            move(vm2, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .addAction(inProgressMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulateInProgressNotDuplicated() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder inProgressMove =
            move(vm2, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .addAction(inProgressMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);

        ActionDTO.Action.Builder reRecommendQueued =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder reRecommendInProgress =
            move(vm2, hostA, vmType, hostB, vmType);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(reRecommendInProgress)
            .addAction(reRecommendQueued)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(2, actionStore.size());
        assertThat(actionStore.getActionView(inProgressMove.getId()).isPresent(), is(true));
    }

    @Test
    public void testPopulateClearedSucceededFailedAreRemoved() throws Exception {
        ActionDTO.Action.Builder successMove =
            move(vm3, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder failedMove =
            move(vm1, hostB, vmType, hostC, vmType);
        ActionDTO.Action.Builder clearedMove =
            move(vm2, hostC, vmType, hostD, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
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
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
    }

    @Test
    public void testPopulateOneDuplicateReRecommended() {
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(1, actionStore.size());
    }

    @Test
    public void testPopulateReRecommendedWithAdditionalDuplicate() {
        ActionDTO.Action.Builder firstMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(3, actionStore.size());
        assertTrue(actionStore.getAction(firstMove.getId()).isPresent());
        assertTrue(actionStore.getAction(secondMove.getId()).isPresent());
    }

    @Test
    public void testPopulation() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        actionStore.populateRecommendedActions(plan);
        assertThat(actionStore.getActionViews().getAll()
                .map(spec -> spec.getRecommendation().getId())
                .collect(Collectors.toList()),
            containsInAnyOrder(firstMove.getId(), secondMove.getId()));
    }

    @Test
    public void testGetActionViews() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        actionStore.populateRecommendedActions(plan);
        assertThat(actionStore.getActionViews().getAll()
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
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        actionStore.populateRecommendedActions(plan);

        verify(entitySettingsCache).newSnapshot(eq(ImmutableSet.of(vm1, hostA, hostB)),
            eq(plan.getInfo().getMarket().getSourceTopologyInfo().getTopologyContextId()),
            eq(plan.getInfo().getMarket().getSourceTopologyInfo().getTopologyId()));
        verify(spyActionFactory).newAction(any(),
            eq(firstPlanId));
        assertEquals(1, actionStore.size());
    }

    @Test
    public void testPurgeOfNonRecommendedAction() {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        when(queuedAction.get().getState()).thenReturn(ActionState.QUEUED);
        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(), is(ActionState.QUEUED));

        ActionDTO.Action.Builder queuedMoveSameSrc =
            move(vm1, hostA, vmType, hostC, vmType);
        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(queuedMoveSameSrc)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertThat (actionStore.size(), is(2));
        // The 1st action should have received a NotRecommendedEvent.
        verify(actionStore.getAction(queuedMove.getId()).get()).receive(isA(NotRecommendedEvent.class));
        // 2nd one should be in READY state.
        assertThat(actionStore.getAction(queuedMoveSameSrc.getId()).get().getState(), is(ActionState.READY));
    }

    @Test
    public void testTranslationOfRecommendedActions() {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        actionStore.populateRecommendedActions(plan);
        final Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        assertTrue(queuedAction.isPresent());
        // Translation should have succeeded.
        assertThat(queuedAction.get().getTranslationStatus(), is(TranslationStatus.TRANSLATION_SUCCEEDED));
    }

    @Test
    public void testDropFailedTranslations() {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        doAnswer(invocation -> {
            Stream<Action> actionStream = (Stream<Action>)invocation.getArgumentAt(0, Stream.class);
            return actionStream.peek(action -> action.getActionTranslation().setTranslationFailure());
        }).when(actionTranslator).translate(any(Stream.class));

        actionStore.populateRecommendedActions(plan);
        final Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        // Translation should have failed, so the action shouldn't be in the store..
        assertFalse(queuedAction.isPresent());
    }

    @Captor
    private ArgumentCaptor<Stream<Action>> translationCaptor;

    @Test
    public void testRetentionOfReRecommendedAction() {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
//        queuedAction.get().receive(new AutomaticAcceptanceEvent("foo", 123L));
        when(queuedAction.get().getState()).thenReturn(ActionState.QUEUED);
//        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(),
//            is(ActionState.QUEUED));

        ActionDTO.Action.Builder queuedMoveReRecommended =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(queuedMoveReRecommended)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertThat (actionStore.size(), is(1));
        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(),
            is(ActionState.QUEUED));
    }

    @Test
    public void testUpdateOfReRecommendedAction() {
        final ActionDTO.Action.Builder move = move(vm1, hostA, vmType, hostB, vmType)
            // Initially the importance is 1 and executability is "false".
            .setDeprecatedImportance(1)
            .setExecutable(false);

        final ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move)
            .build();

        actionStore.populateRecommendedActions(firstPlan);

        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getDeprecatedImportance(),
            is(move.getDeprecatedImportance()));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getExecutable(),
            is(move.getExecutable()));

        final ActionDTO.Action.Builder updatedMove = move(vm1, hostA, vmType, hostB, vmType)
            .setDeprecatedImportance(2)
            .setExecutable(true);
        final ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(updatedMove)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertThat (actionStore.size(), is(1));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getDeprecatedImportance(),
            is(updatedMove.getDeprecatedImportance()));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getExecutable(),
            is(updatedMove.getExecutable()));
    }

    @Test
    public void testRecommendationTracker() {
        ActionDTO.Action move1 =
            move(vm1, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move2 =
            move(vm2, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move3 =
            move(vm1, hostA, vmType, hostC, vmType).build();
        // Add some duplicates actionInfos to fill the queue with more than 1 entry
        ActionDTO.Action move4 =
            move(vm1, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move5 =
            move(vm2, hostA, vmType, hostB, vmType).build();

        ActionFactory actionFactory = new ActionFactory(actionModeCalculator);
        List<Action> actions = ImmutableList.of(move1, move2, move3, move4, move5)
            .stream()
            .map(action -> actionFactory.newAction(action, firstPlanId))
            .collect(Collectors.toList());

        // Now test the recommendation tracker structure.
        // Run many iterations where a different action is taken from the tracker in each iteration
        // to cover various cases (i.e. remove from front, middle, end)
        int numIterations = actions.size();
        for (int i=0; i < numIterations; i++) {
            RecommendationTracker recommendations = new RecommendationTracker();
            actions.forEach(action -> recommendations.add(action));
            Action actionToRemove = actions.get(i);
            recommendations.take(actionToRemove.getRecommendation().getInfo());
            Set<Long> actionIdsRemaining =
                actions.stream()
                    .map(a -> a.getId())
                    .filter(id -> id!=actionToRemove.getId())
                    .collect(Collectors.toSet());
            assertThat(actionIdsRemaining, new ReflectionEquals(
                StreamSupport.stream(recommendations.spliterator(), false)
                    .map(action -> action.getId())
                    .collect(Collectors.toSet())));
        }
    }

}
