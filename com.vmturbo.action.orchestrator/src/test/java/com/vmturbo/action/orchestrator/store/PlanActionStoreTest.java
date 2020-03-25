package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.db.tables.MarketAction.MARKET_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.ListUtils;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.pojos.MarketAction;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Integration tests related to the {@link PlanActionStoreTest}.
 */
public class PlanActionStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(com.vmturbo.action.orchestrator.db.Action.ACTION);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private final long firstPlanId = 0xBEADED;
    private final long secondPlanId = 0xDADDA;

    private final long firstContextId = 0xFED;
    private final long secondContextId = 0xFEED;

    private static final long hostA = 0xA;
    private static final long hostB = 0xB;

    private static final long realtimeId = 777777L;
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
        public Action newPlanAction(@Nonnull ActionDTO.Action recommendation, @Nonnull LocalDateTime recommendationTime,
                                    long actionPlanId, String description,
                @Nullable final Long associatedAccountId,
                @Nullable final Long associatedResourceGroupId) {
            return spy(new Action(recommendation, recommendationTime, actionPlanId,
                    actionModeCalculator, description, associatedAccountId, associatedResourceGroupId));
        }
    }

    private final LocalDateTime actionRecommendationTime = LocalDateTime.now();
    private final IActionFactory actionFactory = mock(IActionFactory.class);
    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private PlanActionStore actionStore;

    private final EntitiesAndSettingsSnapshotFactory entitiesSnapshotFactory = mock(EntitiesAndSettingsSnapshotFactory.class);
    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);

    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
        actionStore = new PlanActionStore(spyActionFactory, dsl, firstContextId,
            null, null,
            entitiesSnapshotFactory, actionTranslator, realtimeId);

        // Enforce that all actions created with this factory get the same recommendation time
        // so that actions can be easily compared.
        when(actionFactory.newPlanAction(any(ActionDTO.Action.class),
                any(LocalDateTime.class), anyLong(), anyString(), any(), any())).thenAnswer(
            invocation -> {
                Object[] args = invocation.getArguments();
                return new Action((ActionDTO.Action) args[0], actionRecommendationTime, (Long) args[2],
                        actionModeCalculator, "Move VM from H1 to H2", 321L, 121L);
            });
        setEntitiesOIDs();
        when(actionTargetSelector.getTargetsForActions(any(), any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action ->
                ImmutableActionTargetInfo.builder()
                    .targetId(100L).supportingLevel(SupportLevel.SUPPORTED).build()));
        });
    }

    public void setEntitiesOIDs() {
        when(entitiesSnapshotFactory.newSnapshot(any(), any(), anyLong())).thenReturn(snapshot);
        // Hack: if plan source topology is not available, the fall back on realtime.
        when(entitiesSnapshotFactory.newSnapshot(any(), any(), eq(realtimeId))).thenReturn(snapshot);
        for (long i=1; i<10;i++) {
            createMockEntity(i,EntityType.VIRTUAL_MACHINE.getNumber());
        }
        createMockEntity(hostA,EntityType.PHYSICAL_MACHINE.getNumber());
        createMockEntity(hostB,EntityType.PHYSICAL_MACHINE.getNumber());
    }

    private void createMockEntity(long id, int type) {
        when(snapshot.getEntityFromOid(eq(id)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(id, type));
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        assertEquals(0, allMarketActions().size());
        actionStore.populateRecommendedActions(marketActionPlan(firstPlanId, firstContextId, actionList(3)));

        assertEquals(3, allMarketActions().size());
        assertEquals(3, marketActionsForContext(firstContextId).size());
        assertEquals(0, marketActionsForContext(secondContextId).size());
    }

    @Test
    public void testPopulateRecommendedActionsOverwrite() throws Exception {
        assertEquals(0, allMarketActions().size());
        actionStore.populateRecommendedActions(marketActionPlan(firstPlanId, firstContextId, actionList(3)));
        assertEquals(3, allMarketActions().size());

        actionStore.populateRecommendedActions(marketActionPlan(firstPlanId, firstContextId, actionList(1)));
        assertEquals(1, allMarketActions().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPopulateRecommendedActionsWithDifferentContext() throws Exception {
        actionStore.populateRecommendedActions(marketActionPlan(firstPlanId, firstContextId, actionList(3)));
        actionStore.populateRecommendedActions(marketActionPlan(secondPlanId, secondContextId, actionList(4)));
    }

    @Test
    public void testSizeInitial() throws Exception {
        assertEquals(0, actionStore.size());
    }

    @Test
    public void testSizeAfterPopulate() throws Exception {
        actionStore.populateRecommendedActions(marketActionPlan(firstPlanId, firstContextId, actionList(3)));
        assertEquals(3, actionStore.size());
    }

    @Test
    public void testGetActionViewsWhenEmpty() throws Exception {
        assertTrue(actionStore.getActionViews().isEmpty());
    }

    @Test
    public void testGetActionViews() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        Collection<ActionView> actionViews = actionStore.getActionViews().getAll().collect(Collectors.toList());
        assertEquals(1, actionViews.size());
        assertEquals(action, actionViews.iterator().next().getRecommendation());
    }

    @Test
    public void testGetActionView() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        final ActionView actionView = actionStore.getActionView(1).get();
        assertEquals(action, actionView.getRecommendation());
        assertEquals(firstPlanId, actionView.getActionPlanId());
        assertTrue(actionView.getRecommendation().getExecutable());
        assertEquals(ActionMode.RECOMMEND, actionView.getMode());
        assertFalse(actionView.getDecision().isPresent());
        assertFalse(actionView.getCurrentExecutableStep().isPresent());
    }

    @Test
    public void testGetNonExistingActionView() throws Exception {
        assertFalse(actionStore.getActionView(1234L).isPresent());
    }

    @Test
    public void testGetActionFromEmptyStore() throws Exception {
        assertFalse(actionStore.getAction(1234L).isPresent());
    }

    @Test
    public void testGetAction() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        ActionOrchestratorTestUtils.assertActionsEqual(
            actionFactory.newPlanAction(action, actionRecommendationTime, firstPlanId, "", null,
                    null),
            actionStore.getAction(1L).get()
        );
    }

    @Test
    public void testGetActionsFromEmptyStore() throws Exception {
        assertTrue(actionStore.getActions().isEmpty());
    }

    @Test
    public void testGetActions() throws Exception {
        final List<ActionDTO.Action> actionList = actionList(3);
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, actionList);
        actionStore.populateRecommendedActions(actionPlan);

        Map<Long, Action> actionsMap = actionStore.getActions();
        actionList.forEach(action ->
            ActionOrchestratorTestUtils.assertActionsEqual(
                actionFactory.newPlanAction(action, actionRecommendationTime, firstPlanId, "",
                        null, null),
                actionsMap.get(action.getId())
            )
        );
    }

    @Test
    public void testOverwriteActionsEmptyStore() throws Exception {
        final List<Action> actionList = actionList(3).stream()
            .map(marketAction -> actionFactory.newPlanAction(marketAction, actionRecommendationTime,
                    firstPlanId, "", null, null))
            .collect(Collectors.toList());

        actionStore.overwriteActions(ImmutableMap.of(ActionPlanType.MARKET, actionList));
        Map<Long, Action> actionsMap = actionStore.getActions();
        actionList.forEach(action ->
                ActionOrchestratorTestUtils.assertActionsEqual(
                    action,
                    actionsMap.get(action.getId())
                )
        );
    }

    @Test
    public void testOverwriteActions() throws Exception {
        final ActionPlan actionPlan1 = marketActionPlan(firstPlanId, firstContextId, actionList(4));
        final ActionPlan actionPlan2 = buyRIActionPlan(secondPlanId, firstContextId, actionList(3));
        actionStore.populateRecommendedActions(actionPlan1);
        actionStore.populateRecommendedActions(actionPlan2);

        final int numOverwriteActions = 2;
        final List<Action> marketOverwriteActions = actionList(numOverwriteActions).stream()
            .map(marketAction -> actionFactory.newPlanAction(marketAction, actionRecommendationTime,
                firstPlanId, "", null, null))
            .collect(Collectors.toList());
        final List<Action> buyRIOverwriteActions = actionList(numOverwriteActions).stream()
            .map(buyRIAction -> actionFactory.newPlanAction(buyRIAction, actionRecommendationTime,
                secondPlanId, "", null, null))
            .collect(Collectors.toList());

        actionStore.overwriteActions(ImmutableMap.of(
            ActionPlanType.MARKET, marketOverwriteActions,
            ActionPlanType.BUY_RI, buyRIOverwriteActions));
        Map<ActionPlanType, Collection<Action>> actionsByActionPlanType = actionStore.getActionsByActionPlanType();
        assertEquals(2, actionsByActionPlanType.size());
        Map<Long, Action> actionsMap = actionStore.getActions();
        ListUtils.union(marketOverwriteActions, buyRIOverwriteActions).forEach(action ->
            ActionOrchestratorTestUtils.assertActionsEqual(
                action,
                actionsMap.get(action.getId())
            )
        );
    }

    @Test
    public void testClearEmpty() {
        assertTrue(actionStore.clear());
        assertEquals(0, actionStore.size());
        assertFalse(actionStore.getActionPlanId(ActionPlanType.MARKET).isPresent());
    }

    @Test
    public void testClear() {
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, actionList(2));
        actionStore.populateRecommendedActions(actionPlan);

        assertEquals(2, actionStore.size());
        assertEquals(firstPlanId, (long) actionStore.getActionPlanId(ActionPlanType.MARKET).get());

        assertTrue(actionStore.clear());
        assertEquals(0, actionStore.size());
        assertFalse(actionStore.getActionPlanId(ActionPlanType.MARKET).isPresent());
    }

    @Test
    public void testGetTopologyContextId() {
        assertEquals(firstContextId, actionStore.getTopologyContextId());
    }

    @Test
    public void testStoreLoaderWithNoStores() {
        List<ActionStore> loadedStores =
            new PlanActionStore.StoreLoader(dsl, actionFactory, actionModeCalculator, entitiesSnapshotFactory, actionTranslator, realtimeId,
                null, null).loadActionStores();

        assertTrue(loadedStores.isEmpty());
    }

    @Test
    public void testStoreLoader() {
        // map of topology context id to action store
        Map<Long, ActionStore> expectedActionStores = Maps.newHashMap();
        Map<Long, ActionStore> actualActionStores = Maps.newHashMap();

        // Setup first planActionStore. This has 2 Market actions and 3 Buy RI Actions
        final ActionPlan marketActionPlan = marketActionPlan(firstPlanId, firstContextId, actionList(2));
        final ActionPlan buyRIActionPlan = buyRIActionPlan(secondPlanId, firstContextId, actionList(3));
        actionStore.populateRecommendedActions(marketActionPlan);
        actionStore.populateRecommendedActions(buyRIActionPlan);
        expectedActionStores.put(firstContextId, actionStore);

        // Setup second planActionStore. This has 9 Buy RI Actions
        final ActionPlan buyRIActionPlan2 = buyRIActionPlan(3L, secondContextId, actionList(9));
        PlanActionStore actionStore2 = new PlanActionStore(spyActionFactory, dsl, secondContextId,
            null, null,
            entitiesSnapshotFactory, actionTranslator, realtimeId);
        actionStore2.populateRecommendedActions(buyRIActionPlan2);
        expectedActionStores.put(secondContextId, actionStore2);

        // Load the stores from DB
        List<ActionStore> loadedStores =
            new PlanActionStore.StoreLoader(dsl, actionFactory, actionModeCalculator, entitiesSnapshotFactory, actionTranslator, realtimeId,
                 null, null).loadActionStores();
        loadedStores.forEach(store -> actualActionStores.put(store.getTopologyContextId(), store));

        // Assert that what we load from DB is the same as what we setup initially
        for (Map.Entry<Long, ActionStore> storeEntry : expectedActionStores.entrySet()) {
            long topologyContextId = storeEntry.getKey();
            ActionStore expectedStore = storeEntry.getValue();
            ActionStore actualStore = actualActionStores.get(topologyContextId);
            Map<ActionPlanType, Collection<Action>> actualActions = actualStore.getActionsByActionPlanType();

            for (Map.Entry<ActionPlanType, Collection<Action>> actionsByActionPlanType :
                expectedStore.getActionsByActionPlanType().entrySet()) {
                ActionPlanType type = actionsByActionPlanType.getKey();
                Collection<Action> expectedActions = actionsByActionPlanType.getValue();
                assertEquals(expectedActions.size(), actualActions.get(type).size());
            }
        }
    }

    @Test
    public void testPopulateMultipleActionPlans() {
        final ActionPlan marketActionPlan = marketActionPlan(firstPlanId, firstContextId, actionList(2));
        final ActionPlan buyRIActionPlan = buyRIActionPlan(secondPlanId, firstContextId, actionList(3));

        actionStore.populateRecommendedActions(marketActionPlan);
        actionStore.populateRecommendedActions(buyRIActionPlan);

        Map<ActionPlanType, Collection<Action>> actionsByActionPlanType = actionStore.getActionsByActionPlanType();
        assertEquals(2, actionsByActionPlanType.get(ActionPlanType.MARKET).size());
        assertEquals(firstPlanId, actionStore.getActionPlanId(ActionPlanType.MARKET).get().longValue());
        assertEquals(3, actionsByActionPlanType.get(ActionPlanType.BUY_RI).size());
        assertEquals(secondPlanId, actionStore.getActionPlanId(ActionPlanType.BUY_RI).get().longValue());
    }

    @Test
    public void testReplaceActions() {
        final ActionPlan marketActionPlan1 = marketActionPlan(firstPlanId, firstContextId, actionList(2));
        final ActionPlan marketActionPlan2 = marketActionPlan(secondPlanId, firstContextId, actionList(3));

        actionStore.populateRecommendedActions(marketActionPlan1);
        assertEquals(2, actionStore.getActionsByActionPlanType().get(ActionPlanType.MARKET).size());
        assertEquals(firstPlanId, actionStore.getActionPlanId(ActionPlanType.MARKET).get().longValue());

        actionStore.populateRecommendedActions(marketActionPlan2);
        assertEquals(3, actionStore.getActionsByActionPlanType().get(ActionPlanType.MARKET).size());
        assertEquals(secondPlanId, actionStore.getActionPlanId(ActionPlanType.MARKET).get().longValue());
    }

    private static List<ActionDTO.Action> actionList(final int numActions) {
        List<ActionDTO.Action> actions =  IntStream.range(0, numActions)
            .mapToObj(i -> ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),i+1,hostA,14,hostB,14))
            .collect(Collectors.toList());
        return actions;
    }

    private static ActionPlan marketActionPlan(final long planId, final long topologyContextId,
                                               @Nonnull final Collection<ActionDTO.Action> actions) {

        ActionPlan plan = ActionPlan.newBuilder()
            .setId(planId)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(IdentityGenerator.next())
                        .setTopologyContextId(topologyContextId)
                        .setTopologyType(TopologyType.PLAN))))
            .addAllAction(Objects.requireNonNull(actions))
            .build();
        return plan;
    }

    private static ActionPlan buyRIActionPlan(final long planId, final long topologyContextId,
                                               @Nonnull final Collection<ActionDTO.Action> actions) {

        return ActionPlan.newBuilder()
            .setId(planId)
            .setInfo(ActionPlanInfo.newBuilder()
                .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                    .setTopologyContextId(topologyContextId)))
            .addAllAction(Objects.requireNonNull(actions))
            .build();
    }

    private List<MarketAction> allMarketActions() {
        return dsl.selectFrom(MARKET_ACTION)
            .fetch()
            .into(MarketAction.class);
    }

    private List<MarketAction> marketActionsForContext(long contextId) {
        return dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.TOPOLOGY_CONTEXT_ID.eq(contextId))
            .fetch()
            .into(MarketAction.class);
    }
}
