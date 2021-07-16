package com.vmturbo.action.orchestrator.store.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStoreFactory;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.LiveActionStore.ActionSource;
import com.vmturbo.action.orchestrator.store.LiveActionStore.RecommendationTracker;
import com.vmturbo.action.orchestrator.store.atomic.AggregatedAction;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.ImmutableAtomicActionResult;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionDTOsAndStore;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionDifference;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionIdentityStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionPlanAndStore;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionPlanSummaryStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionsToTranslateAndStore;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.AddRemoveAndClearAtomicActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.AddRemoveAndClearMarketActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CompilePreviousActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CreateAtomicActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CreateLastExecutedRecommendationsTrackerStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetEntitiesAndSettingsSnapshotStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetInvolvedEntityIdsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetOrCreateLiveActionStoreStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.IdentifiedActionDTO;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.IdentifiedActionsAndStore;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.MarketReRecommendedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PopulateActionStoreStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PopulateLiveActionsSegment;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PrepareAggregatedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.RefreshProbeCapabilitiesStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.SupportLevelAndPrerequisitesStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.TranslateActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateAutomationStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateReRecommendedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateSeverityCacheStage;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.Pipeline.Status.Type;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;
import com.vmturbo.components.common.pipeline.Stage;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for various {@link ActionPipelineStages} stages.
 */
public class ActionPipelineStagesTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 1234;

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStoreFactory actionStoreFactory = mock(ActionStoreFactory.class);
    private final LiveActionStore actionStore = mock(LiveActionStore.class);
    private final AtomicActionFactory atomicActionFactory = mock(AtomicActionFactory.class);
    private static final long MOVE_ACTION_ID = 9999L;

    private final ActionDTO.Action moveAction = ActionDTO.Action.newBuilder()
        .setId(MOVE_ACTION_ID)
        .setDeprecatedImportance(0)
        .setExplanation(Explanation.getDefaultInstance())
        .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(ActionEntity.newBuilder().setId(1111L).setType(EntityType.VIRTUAL_MACHINE_VALUE).build())
            .addChanges(ChangeProvider.newBuilder()
                .setDestination(ActionEntity.newBuilder().setId(2222L).setType(EntityType.PHYSICAL_MACHINE_VALUE))
                .setDestination(ActionEntity.newBuilder().setId(3333L).setType(EntityType.PHYSICAL_MACHINE_VALUE))
                .build())
            .build()))
        .build();
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1234L)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(5678L)
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID))))
        .addAction(moveAction)
        .build();

    private static final long TOPOLOGY_ID = 9999L;
    private final ActionPipelineContext context = spy(new ActionPipelineContext(TOPOLOGY_ID,
        TopologyType.REALTIME, ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(TOPOLOGY_ID)
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                    .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
                    .build())
                .build())
        .build()));

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final IActionFactory actionFactory = mock(IActionFactory.class);
    final ActionDifference actionDifference = new ActionDifference();

    private final ActionDTOsAndStore actionDTOsAndStore = new ActionDTOsAndStore(
        Collections.singletonList(moveAction), actionStore);
    private static final long ACTION_OID = 54321L;
    private final IdentifiedActionsAndStore identifiedActionsAndStore = new IdentifiedActionsAndStore(
        Collections.singletonList(new IdentifiedActionDTO(moveAction, ACTION_OID)),
        actionStore);

    /**
     * setup.
     */
    @Before
    public void setup() {
        when(actionStorehouse.getActionStoreFactory()).thenReturn(actionStoreFactory);
        when(actionStoreFactory.newStore(eq(REALTIME_TOPOLOGY_CONTEXT_ID))).thenReturn(actionStore);
    }

    /**
     * testPopulateActionStoreStage.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPopulateActionStoreStage() throws Exception {
        final PopulateActionStoreStage stage = new PopulateActionStoreStage(actionStorehouse);
        when(actionStorehouse.storeActions(actionPlan)).thenReturn(actionStore);
        final StageResult<ActionStore> result = stage.execute(actionPlan);
        assertEquals(actionStore, result.getResult());
        verify(actionStorehouse).storeActions(eq(actionPlan));
    }

    /**
     * testUpdateAutomationStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testUpdateAutomationStage() throws PipelineStageException, InterruptedException {
        final ActionAutomationManager automationManager = mock(ActionAutomationManager.class);
        final UpdateAutomationStage stage = new UpdateAutomationStage(automationManager);
        stage.execute(actionStore);

        verify(automationManager).updateAutomation(actionStore);
    }

    /**
     * testUpdateSeverityCacheStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testUpdateSeverityCacheStage() throws PipelineStageException, InterruptedException {
        final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
        when(actionStore.getEntitySeverityCache()).thenReturn(Optional.of(severityCache));

        final UpdateSeverityCacheStage stage = new UpdateSeverityCacheStage();
        stage.execute(actionStore);

        verify(severityCache).refresh(eq(actionStore));
    }

    /**
     * Test the {@link GetOrCreateLiveActionStoreStage}.
     *
     * @throws PipelineStageException on exception.
     */
    @Test
    public void testGetOrCreateLiveActionStoreStage() throws PipelineStageException {
        final ActionStorehouse storehouse = mock(ActionStorehouse.class);
        when(storehouse.measurePlanAndGetOrCreateStore(eq(actionPlan))).thenReturn(actionStore);
        final GetOrCreateLiveActionStoreStage stage = new GetOrCreateLiveActionStoreStage(storehouse);
        stage.setContext(context);

        final StageResult<ActionPlanAndStore> result = stage.executeStage(actionPlan);
        assertEquals(actionPlan, result.getResult().getActionPlan());
        assertEquals(actionStore, result.getResult().getActionStore());
    }

    /**
     * Test {@link GetInvolvedEntityIdsStage}.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testGetInvolvedEntityIdsStage() throws PipelineStageException, InterruptedException {
        final ActionPlanAndStore planAndStore = new ActionPlanAndStore(actionPlan, actionStore);
        final GetInvolvedEntityIdsStage stage = new GetInvolvedEntityIdsStage();

        stage.setContext(context);
        stage.execute(planAndStore);
        final Set<Long> involvedEntities = captureContextMember(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS);

        assertEquals(ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList()), involvedEntities);
    }

    /**
     * Test PrepareAggregatedActionsStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testPrepareAggregatedActionsStage() throws PipelineStageException, InterruptedException {
        final ActionPlanAndStore planAndStore = new ActionPlanAndStore(actionPlan, actionStore);
        final AggregatedAction aggregatedAction = new AggregatedAction(ActionTypeCase.RESIZE, ActionEntity.newBuilder()
            .setId(44L)
            .setType(EntityType.CONTAINER_VALUE)
            .build(), "foo");
        final Map<Long, AggregatedAction> aggregatedActions = ImmutableMap.of(
            22L, aggregatedAction);
        aggregatedActions.get(22L).addAction(moveAction, Optional.empty());
        final Set<Long> involvedEntityIds = new HashSet<>();

        when(atomicActionFactory.canMerge()).thenReturn(true);
        when(atomicActionFactory.aggregate(any())).thenReturn(aggregatedActions);

        provideContextMember(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS, involvedEntityIds);

        final PrepareAggregatedActionsStage stage = new PrepareAggregatedActionsStage(atomicActionFactory);
        stage.setContext(context);
        stage.execute(planAndStore);

        final Map<Long, AggregatedAction> actionIdToActions =
            captureContextMember(ActionPipelineContextMembers.ACTION_ID_TO_AGGREGATE_ACTION);
        final Map<Long, AggregatedAction> aggActions =
            captureContextMember(ActionPipelineContextMembers.AGGREGATED_ACTIONS);
        assertEquals(ImmutableMap.of(MOVE_ACTION_ID, aggregatedAction), actionIdToActions);
        assertEquals(aggregatedActions, aggActions);
    }

    /**
     * Test GetEntitiesAndSettingsSnapshotStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testGetEntitiesAndSettingsSnapshotStage() throws PipelineStageException, InterruptedException {
        final EntitiesAndSettingsSnapshotFactory factory = mock(EntitiesAndSettingsSnapshotFactory.class);
        final ActionPlanAndStore planAndStore = new ActionPlanAndStore(actionPlan, actionStore);
        final Set<Long> involvedEntityIds = ImmutableSet.of(1L, 2L, 3L);
        when(factory.newSnapshot(eq(involvedEntityIds), anyLong()))
            .thenReturn(snapshot);

        provideContextMember(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS, involvedEntityIds);

        final GetEntitiesAndSettingsSnapshotStage stage = new GetEntitiesAndSettingsSnapshotStage(factory);
        stage.setContext(context);
        stage.execute(planAndStore);

        final EntitiesAndSettingsSnapshot snapshotResult =
            captureContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT);
        assertEquals(snapshot, snapshotResult);
    }

    /**
     * test the CreateLastExecutedRecommendationsTrackerStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testCreateLastExecutedRecommendationsTrackerStage() throws PipelineStageException, InterruptedException {
        final ActionHistoryDao actionDao = mock(ActionHistoryDao.class);
        final ActionView actionView = mock(ActionView.class);
        when(actionDao.getActionHistoryByDate(any(LocalDateTime.class), any(LocalDateTime.class)))
            .thenReturn(Collections.singletonList(actionView));
        when(actionView.getState()).thenReturn(ActionState.SUCCEEDED);
        when(actionView.getRecommendation()).thenReturn(moveAction);
        final Action action = mock(Action.class);
        when(actionFactory.newAction(any(ActionDTO.Action.class), anyLong(), anyLong()))
            .thenReturn(action);
        when(action.getRecommendationOid()).thenReturn(MOVE_ACTION_ID);

        final CreateLastExecutedRecommendationsTrackerStage stage =
            new CreateLastExecutedRecommendationsTrackerStage(actionDao, actionFactory,
                Clock.systemUTC(), 100);
        stage.setContext(context);
        stage.execute(new ActionPlanAndStore(actionPlan, actionStore));

        final RecommendationTracker tracker =
            captureContextMember(ActionPipelineContextMembers.LAST_EXECUTED_RECOMMENDATIONS_TRACKER);
        assertEquals(1, tracker.size());
        assertTrue(tracker.take(MOVE_ACTION_ID).isPresent());
    }

    /**
     * test RefreshProbeCapabilitiesStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testRefreshProbeCapabilitiesStage() throws PipelineStageException, InterruptedException {
        final ProbeCapabilityCache cache = mock(ProbeCapabilityCache.class);

        final RefreshProbeCapabilitiesStage stage = new RefreshProbeCapabilitiesStage(cache);
        stage.setContext(context);
        stage.execute(new ActionPlanAndStore(actionPlan, actionStore));

        verify(cache).fullRefresh();
    }

    /**
     * Test that the populate live actions segment correctly passes through to inner stages.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPopulateLiveActionsSegment() throws Exception {
        final ReentrantLock lock = new ReentrantLock();
        PopulateLiveActionsSegment segment = new PopulateLiveActionsSegment(lock, 2,
            SegmentDefinition.finalStage(new Stage<ActionPlanAndStore, LiveActionStore, ActionPipelineContext>() {
                @Nonnull
                @Override
                protected StageResult<LiveActionStore> executeStage(@Nonnull ActionPlanAndStore input)  {
                    return StageResult.withResult((LiveActionStore)input.getActionStore())
                        .andStatus(Status.withWarnings("warning"));
                }
            })
        );
        segment.setContext(context);

        final StageResult<ActionStore> result = segment.execute(new ActionPlanAndStore(actionPlan, actionStore));

        // We should have a warning result from executing the stage above.
        assertEquals(Type.WARNING, result.getStatus().getType());
        assertThat(result.getStatus().getMessage(), containsString("warning"));
    }

    /**
     * Test that throwing an exception inside the {@link PopulateLiveActionsSegment}
     * still results in the lock being released.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPopulateLiveActionsSegmentException() throws Exception {
        final ReentrantLock lock = spy(new ReentrantLock());
        PopulateLiveActionsSegment segment = new PopulateLiveActionsSegment(lock, 2,
            SegmentDefinition.finalStage(new ThrowExceptionStage())
        );
        segment.setContext(context);

        try {
            segment.execute(new ActionPlanAndStore(actionPlan, actionStore));
            fail("Should never reach this line of code");
        } catch (PipelineStageException e) {
            verify(lock).tryLock(2, TimeUnit.MINUTES);
            verify(lock).unlock();
            assertThat(e.getMessage(), containsString("blow up"));
        }
    }

    /**
     * test {@link ActionPlanSummaryStage}.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testActionPlanSummaryStage() throws PipelineStageException, InterruptedException {
        final ActionPlanAndStore planAndStore = new ActionPlanAndStore(actionPlan, actionStore);
        final ActionCounts storeCounts = new ActionCounts("storeCounts", Stream.of(moveAction));
        when(actionStore.getActionCounts()).thenReturn(storeCounts);

        provideContextMember(ActionPipelineContextMembers.PREVIOUS_ACTION_PLAN_COUNTS,
            new ActionCounts("previous", Stream.empty()));
        provideContextMember(ActionPipelineContextMembers.CURRENT_ACTION_PLAN_COUNTS,
            new ActionCounts("current", Stream.empty()));

        final ActionPlanSummaryStage stage = new ActionPlanSummaryStage();
        stage.setContext(context);
        stage.execute(planAndStore);

        final ActionCounts actionCounts =
            captureContextMember(ActionPipelineContextMembers.ACTION_STORE_STARTING_COUNTS);
        assertEquals(storeCounts, actionCounts);
    }

    /**
     * test the ActionIdentityStage.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testActionIdentityStage() throws Exception {
        @SuppressWarnings("unchecked")
        final IdentityService<ActionInfo> identityService = (IdentityService<ActionInfo>)mock(IdentityService.class);
        when(identityService.getOidsForObjects(eq(Collections.singletonList(moveAction.getInfo()))))
            .thenReturn(Collections.singletonList(ACTION_OID));

        final ActionIdentityStage stage = new ActionIdentityStage(identityService,
            ActionPipelineContextMembers.ATOMIC.getInputActionCount());
        stage.setContext(context);
        final StageResult<IdentifiedActionsAndStore> result = stage.execute(actionDTOsAndStore);
        assertEquals(Collections.singletonList(ACTION_OID),
            result.getResult().getActions().stream()
                .map(IdentifiedActionDTO::getOid)
                .collect(Collectors.toList()));
    }

    /**
     * Test that we discard previously executed actions.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testSupportLevelAndPrerequisitesStageDiscardPreviouslyExecuted()
        throws PipelineStageException, InterruptedException {
        final Action action = mock(Action.class);
        when(action.getRecommendationOid()).thenReturn(ACTION_OID);

        final ActionTargetSelector targetSelector = mock(ActionTargetSelector.class);
        final RecommendationTracker recommendationTracker = new RecommendationTracker();
        recommendationTracker.add(action);
        provideContextMember(ActionPipelineContextMembers.LAST_EXECUTED_RECOMMENDATIONS_TRACKER, recommendationTracker);
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);

        final SupportLevelAndPrerequisitesStage stage = new SupportLevelAndPrerequisitesStage(targetSelector);
        stage.setContext(context);
        final StageResult<IdentifiedActionsAndStore> identifiedActions = stage.execute(identifiedActionsAndStore);

        // We should have discarded the input action because it was previously executed.
        assertTrue(identifiedActions.getResult().getActions().isEmpty());
    }

    /**
     * Test that we retain non-previously executed actions.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testSupportLevelAndPrerequisitesStage()
        throws PipelineStageException, InterruptedException {
        final Action action = mock(Action.class);
        when(action.getRecommendationOid()).thenReturn(ACTION_OID + 1);

        final ActionTargetSelector targetSelector = mock(ActionTargetSelector.class);
        final RecommendationTracker recommendationTracker = new RecommendationTracker();
        recommendationTracker.add(action);
        provideContextMember(ActionPipelineContextMembers.LAST_EXECUTED_RECOMMENDATIONS_TRACKER, recommendationTracker);
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);
        when(targetSelector.getTargetsForActions(any(Stream.class), eq(snapshot), anyMap()))
            .thenReturn(ImmutableMap.of(moveAction.getId(), ImmutableActionTargetInfo.builder()
                .disruptive(true)
                .supportingLevel(SupportLevel.SHOW_ONLY)
                .build()));

        final SupportLevelAndPrerequisitesStage stage = new SupportLevelAndPrerequisitesStage(targetSelector);
        stage.setContext(context);
        final StageResult<IdentifiedActionsAndStore> identifiedActions = stage.execute(identifiedActionsAndStore);

        // We should have retained the input action.
        assertEquals(1, identifiedActions.getResult().getActions().size());
        final IdentifiedActionDTO identifiedActionDTO = identifiedActions.getResult().getActions().get(0);
        assertTrue(identifiedActionDTO.getAction().getDisruptive());
        assertEquals(SupportLevel.SHOW_ONLY, identifiedActionDTO.getAction().getSupportingLevel());
    }

    /**
     * test CompilePreviousActionsStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testCompilePreviousActionsStage() throws PipelineStageException, InterruptedException {
        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);

        final CompilePreviousActionsStage stage = new CompilePreviousActionsStage(
            ActionPipelineContextMembers.MARKET.getActionDifference(), ActionSource.MARKET);
        stage.setContext(context);
        stage.execute(identifiedActionsAndStore);

        verify(actionStore).compilePreviousActions(any(RecommendationTracker.class),
            anyListOf(ActionView.class), anyListOf(Action.class), eq(ActionSource.MARKET));
    }

    /**
     * Test that the stage correctly creates a new action.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testUpdateReRecommendedActionsStageNewAction() throws PipelineStageException, InterruptedException {
        final Map<Long, AggregatedAction> actionIdToAggregateAction = ImmutableMap.of(
            moveAction.getId(), mock(AggregatedAction.class));
        provideContextMember(ActionPipelineContextMembers.ACTION_ID_TO_AGGREGATE_ACTION,
            actionIdToAggregateAction);
        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);

        final Action action = mock(Action.class);
        when(action.getState()).thenReturn(ActionState.READY);
        when(actionFactory.newAction(any(ActionDTO.Action.class), anyLong(), anyLong()))
            .thenReturn(action);

        final MarketReRecommendedActionsStage stage = new MarketReRecommendedActionsStage(actionFactory);
        stage.setContext(context);
        stage.execute(identifiedActionsAndStore);

        final List<Action> mergedActions = captureContextMember(
            ActionPipelineContextMembers.MERGED_MARKET_ACTIONS_FOR_ATOMIC);
        assertEquals(1, mergedActions.size());
        assertThat(mergedActions, contains(action));

    }

    /**
     * Test that the stage correctly updates a re-recommended action.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testUpdateReRecommendedActionsStageReRecommendedAction() throws PipelineStageException, InterruptedException {
        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);

        final Action action = mock(Action.class);
        when(action.getState()).thenReturn(ActionState.READY);
        when(actionFactory.newAction(any(ActionDTO.Action.class), anyLong(), anyLong()))
            .thenReturn(action);

        final UpdateReRecommendedActionsStage stage = new UpdateReRecommendedActionsStage(actionFactory,
            ActionPipelineContextMembers.MARKET.getActionDifference(),
            ActionPipelineContextMembers.MARKET.getNewActionCount());
        stage.setContext(context);
        final StageResult<ActionsToTranslateAndStore> result = stage.execute(identifiedActionsAndStore);
        assertEquals(1, result.getResult().getActionsToTranslate().size());
        assertThat(result.getResult().getActionsToTranslate(), contains(action));
    }

    /**
     * Test that the stage correctly categorizes actions for translation, addition, and actions
     * that need neither.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
        public void testUpdateReRecommendedActionsStageCategorization() throws PipelineStageException, InterruptedException {
        final Map<ActionState, Function<ActionsToTranslateAndStore, List<Action>>> mapping = new HashMap<>();
        final Function<ActionsToTranslateAndStore, List<Action>> toTranslate =
            ActionsToTranslateAndStore::getActionsToTranslate;
        final Function<ActionsToTranslateAndStore, List<Action>> toAdd =
            result -> actionDifference.getActionsToAdd();
        final Function<ActionsToTranslateAndStore, List<Action>> none = result -> null;

        mapping.put(ActionState.READY, toTranslate);
        mapping.put(ActionState.ACCEPTED, toTranslate);
        mapping.put(ActionState.REJECTED, toTranslate);

        mapping.put(ActionState.QUEUED, toAdd);
        mapping.put(ActionState.PRE_IN_PROGRESS, toAdd);
        mapping.put(ActionState.IN_PROGRESS, toAdd);
        mapping.put(ActionState.POST_IN_PROGRESS, toAdd);

        mapping.put(ActionState.FAILED, none);
        mapping.put(ActionState.SUCCEEDED, none);
        mapping.put(ActionState.CLEARED, none);
        mapping.put(ActionState.FAILING, none);

        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);
        for (Entry<ActionState, Function<ActionsToTranslateAndStore, List<Action>>> entry : mapping.entrySet()) {
            actionDifference.getActionsToAdd().clear();

            final Action action = mock(Action.class);
            when(action.getState()).thenReturn(entry.getKey());
            when(actionFactory.newAction(any(ActionDTO.Action.class), anyLong(), anyLong()))
                .thenReturn(action);

            final UpdateReRecommendedActionsStage stage = new UpdateReRecommendedActionsStage(actionFactory,
                ActionPipelineContextMembers.MARKET.getActionDifference(),
                ActionPipelineContextMembers.MARKET.getNewActionCount());
            stage.setContext(context);
            final StageResult<ActionsToTranslateAndStore> result = stage.execute(identifiedActionsAndStore);
            final List<Action> actions = entry.getValue().apply(result.getResult());
            if (actions != null) {
                assertEquals(1, actions.size());
                assertThat(actions, contains(action));
            } else {
                assertTrue(result.getResult().getActionsToTranslate().isEmpty());
                assertTrue(actionDifference.getActionsToAdd().isEmpty());
            }
        }
    }

    /**
     * test MarketReRecommendedActionsStage populates the MERGED_MARKET_ACTIONS_FOR_ATOMIC
     * context member with the actions merged into atomic actions so that they can be used
     * by downstream stages.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testMarketReRecommendedActionsStage() throws PipelineStageException, InterruptedException {
        final Map<Long, AggregatedAction> actionIdToAggregateAction = ImmutableMap.of(
            moveAction.getId(), mock(AggregatedAction.class));
        provideContextMember(ActionPipelineContextMembers.ACTION_ID_TO_AGGREGATE_ACTION,
            actionIdToAggregateAction);
        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);

        final Action action = mock(Action.class);
        when(action.getState()).thenReturn(ActionState.READY);
        when(actionFactory.newAction(any(ActionDTO.Action.class), anyLong(), anyLong()))
            .thenReturn(action);

        final MarketReRecommendedActionsStage stage = new MarketReRecommendedActionsStage(actionFactory);
        stage.setContext(context);
        stage.execute(identifiedActionsAndStore);

        final List<Action> mergedActions = captureContextMember(
            ActionPipelineContextMembers.MERGED_MARKET_ACTIONS_FOR_ATOMIC);
        assertEquals(1, mergedActions.size());
        assertThat(mergedActions, contains(action));
    }

    /**
     * Test action translation stage successful translation.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testTranslateActionsStageSuccess() throws PipelineStageException, InterruptedException {
        final ActionTranslator actionTranslator = mock(ActionTranslator.class);
        final Action action = mock(Action.class);
        when(action.getTranslationStatus()).thenReturn(TranslationStatus.TRANSLATION_SUCCEEDED);
        when(actionTranslator.translate(any(Stream.class), eq(snapshot)))
            .thenReturn(Stream.of(action));
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);
        provideContextMember(ActionPipelineContextMembers.ATOMIC.getActionDifference(), actionDifference);

        final TranslateActionsStage stage = new TranslateActionsStage(actionTranslator,
            ActionPipelineContextMembers.ATOMIC.getActionDifference());
        stage.setContext(context);
        stage.execute(new ActionsToTranslateAndStore(Collections.singletonList(action), actionStore));

        // A successfully translated action should go in the toAdd list
        assertEquals(1, actionDifference.getActionsToAdd().size());
        assertThat(actionDifference.getActionsToAdd(), contains(action));
    }

    /**
     * Test action translation stage unsuccessful translation.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testTranslateActionsStageFailure() throws PipelineStageException, InterruptedException {
        final ActionTranslator actionTranslator = mock(ActionTranslator.class);
        final Action action = mock(Action.class);
        when(action.getTranslationStatus()).thenReturn(TranslationStatus.TRANSLATION_FAILED);
        when(actionTranslator.translate(any(Stream.class), eq(snapshot)))
            .thenReturn(Stream.of(action));
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);
        provideContextMember(ActionPipelineContextMembers.ATOMIC.getActionDifference(), actionDifference);

        final TranslateActionsStage stage = new TranslateActionsStage(actionTranslator,
            ActionPipelineContextMembers.ATOMIC.getActionDifference());
        stage.setContext(context);
        stage.execute(new ActionsToTranslateAndStore(Collections.singletonList(action), actionStore));

        // A failed translated action should go in the toRemove list
        assertEquals(1, actionDifference.getActionsToRemove().size());
        assertThat(actionDifference.getActionsToRemove(), contains(action));
    }

    /**
     * Test the creation of atomic actions.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testCreateAtomicActionsStage() throws PipelineStageException, InterruptedException {
        final Map<Long, AggregatedAction> aggregatedActions = new HashMap<>();
        final ActionDTO.Action dedupAction = moveAction.toBuilder().setId(moveAction.getId() + 1).build();
        final ActionDTO.Action atomicAction = moveAction.toBuilder().setId(moveAction.getId() + 2).build();
        provideContextMember(ActionPipelineContextMembers.AGGREGATED_ACTIONS, aggregatedActions);
        when(atomicActionFactory.atomicActions(eq(aggregatedActions)))
            .thenReturn(Collections.singletonList(ImmutableAtomicActionResult.builder()
                .atomicAction(atomicAction)
                .deDuplicatedActions(ImmutableMap.of(dedupAction, Collections.singletonList(moveAction)))
                .build()
            ));

        final CreateAtomicActionsStage stage = new CreateAtomicActionsStage(atomicActionFactory);
        stage.setContext(context);
        final StageResult<ActionDTOsAndStore> result = stage.execute(actionStore);

        assertEquals(2, result.getResult().getActionDTOs().size());
        assertThat(result.getResult().getActionDTOs(), containsInAnyOrder(atomicAction, dedupAction));
    }

    /**
     * Test the adding, removal, and clearing of atomic actions.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testAddRemoveAndClearAtomicActionsStage() throws PipelineStageException, InterruptedException {
        final List<Action> mergedActions = new ArrayList<>();
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);
        provideContextMember(ActionPipelineContextMembers.MERGED_MARKET_ACTIONS_FOR_ATOMIC, mergedActions);
        provideContextMember(ActionPipelineContextMembers.ATOMIC.getActionDifference(), actionDifference);
        provideContextMember(ActionPipelineContextMembers.ATOMIC.getInputActionCount(), 1);
        provideContextMember(ActionPipelineContextMembers.ATOMIC.getNewActionCount(), 2);

        final Action toClear = mock(Action.class);
        when(toClear.getRecommendationOid()).thenReturn(MOVE_ACTION_ID + 1);
        when(toClear.getState()).thenReturn(ActionState.READY);
        final Action toNotClear = mock(Action.class);
        when(toClear.getRecommendationOid()).thenReturn(MOVE_ACTION_ID + 2);
        when(toNotClear.getState()).thenReturn(ActionState.IN_PROGRESS);
        actionDifference.getPreviousRecommendations().add(toClear);
        actionDifference.getPreviousRecommendations().add(toNotClear);

        final AddRemoveAndClearAtomicActionsStage stage = new AddRemoveAndClearAtomicActionsStage(
            ActionPipelineContextMembers.ATOMIC.getActionDifference(),
            ActionPipelineContextMembers.ATOMIC.getInputActionCount(),
            ActionPipelineContextMembers.ATOMIC.getNewActionCount(),
            ActionPipelineContextMembers.ATOMIC.getActionsToAdd());
        stage.setContext(context);
        stage.execute(actionStore);

        verify(actionStore).updateAtomicActions(eq(snapshot), eq(actionDifference.getActionsToRemove()),
            eq(actionDifference.getActionsToAdd()), eq(mergedActions));

        // Ensure the action still in the tracker was cleared
        verify(toClear).receive(any(NotRecommendedEvent.class));
        verify(toNotClear, never()).receive(any(NotRecommendedEvent.class));
    }

    /**
     * Test the adding, removal, and clearing of atomic actions.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on exception.
     */
    @Test
    public void testAddRemoveAndClearMarketActionsStage() throws PipelineStageException, InterruptedException {
        final List<Action> mergedActions = new ArrayList<>();
        provideContextMember(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT, snapshot);
        provideContextMember(ActionPipelineContextMembers.MARKET.getActionDifference(), actionDifference);
        provideContextMember(ActionPipelineContextMembers.MARKET.getInputActionCount(), 1);
        provideContextMember(ActionPipelineContextMembers.MARKET.getNewActionCount(), 2);

        final Action toClear = mock(Action.class);
        when(toClear.getRecommendationOid()).thenReturn(MOVE_ACTION_ID + 1);
        when(toClear.getState()).thenReturn(ActionState.READY);
        final Action toNotClear = mock(Action.class);
        when(toClear.getRecommendationOid()).thenReturn(MOVE_ACTION_ID + 2);
        when(toNotClear.getState()).thenReturn(ActionState.IN_PROGRESS);
        actionDifference.getPreviousRecommendations().add(toClear);
        actionDifference.getPreviousRecommendations().add(toNotClear);

        final AddRemoveAndClearMarketActionsStage stage = new AddRemoveAndClearMarketActionsStage(
            ActionPipelineContextMembers.MARKET.getActionDifference(),
            ActionPipelineContextMembers.MARKET.getInputActionCount(),
            ActionPipelineContextMembers.MARKET.getNewActionCount(),
            ActionPipelineContextMembers.MARKET.getActionsToAdd());
        stage.setContext(context);
        stage.execute(actionStore);

        verify(actionStore).updateMarketActions(eq(snapshot), eq(actionDifference.getActionsToRemove()),
            eq(actionDifference.getActionsToAdd()));

        // Ensure the action still in the tracker was cleared
        verify(toClear).receive(any(NotRecommendedEvent.class));
        verify(toNotClear, never()).receive(any(NotRecommendedEvent.class));
    }

    private <T> void provideContextMember(@Nonnull final PipelineContextMemberDefinition<T> memberDef,
                                          @Nonnull final T contextMember) {
        provideContextMember(memberDef, contextMember, context);
    }

    private static <T> void provideContextMember(@Nonnull final PipelineContextMemberDefinition<T> memberDef,
                                                 @Nonnull final T contextMember,
                                                 @Nonnull final ActionPipelineContext context) {
        context.addMember(memberDef, contextMember);
    }

    private <T> T captureContextMember(@Nonnull final  PipelineContextMemberDefinition<T> memberDef) {
        return captureContextMember(memberDef, context);
    }

    private static <T> T captureContextMember(@Nonnull final PipelineContextMemberDefinition<T> memberDef,
                                              @Nonnull final ActionPipelineContext context) {
        final ArgumentCaptor<T> captor = ArgumentCaptor.forClass(memberDef.getMemberClass());
        verify(context).addMember(eq(memberDef), captor.capture());
        return captor.getValue();
    }

    /**
     * A stage that throws an exception when run.
     */
    private static class ThrowExceptionStage extends Stage<ActionPlanAndStore, LiveActionStore, ActionPipelineContext> {
        @Nonnull
        @Override
        protected StageResult<LiveActionStore> executeStage(@Nonnull ActionPlanAndStore input) {
            throw new RuntimeException("blow up");
        }
    }
}
