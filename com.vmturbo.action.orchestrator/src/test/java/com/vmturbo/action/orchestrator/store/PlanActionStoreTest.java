package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.db.tables.MarketAction.MARKET_ACTION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.pojos.MarketAction;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Integration tests related to the {@link PlanActionStoreTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class PlanActionStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;
    private DSLContext dsl;

    private final long firstPlanId = 0xBEADED;
    private final long secondPlanId = 0xDADDA;

    private final long firstContextId = 0xFED;
    private final long secondContextId = 0xFEED;

    private final LocalDateTime actionRecommendationTime = LocalDateTime.now();
    private final IActionFactory actionFactory = mock(IActionFactory.class);
    private PlanActionStore actionStore;
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.map(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
                return action;
            })));
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
        actionStore = new PlanActionStore(actionFactory, dsl, firstContextId);

        // Enforce that all actions created with this factory get the same recommendation time
        // so that actions can be easily compared.
        when(actionFactory.newAction(any(ActionDTO.Action.class),
                any(LocalDateTime.class), anyLong())).thenAnswer(
            invocation -> {
                Object[] args = invocation.getArguments();
                return new Action((ActionDTO.Action) args[0], actionRecommendationTime, (Long) args[2],
                        actionModeCalculator);
            });
    }

    @After
    public void teardown() throws Exception {
        flyway.clean();
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        assertEquals(0, allMarketActions().size());
        actionStore.populateRecommendedActions(actionPlan(firstPlanId, firstContextId, actionList(3)));

        assertEquals(3, allMarketActions().size());
        assertEquals(3, marketActionsForContext(firstContextId).size());
        assertEquals(0, marketActionsForContext(secondContextId).size());
    }

    @Test
    public void testPopulateRecommendedActionsOverwrite() throws Exception {
        assertEquals(0, allMarketActions().size());
        actionStore.populateRecommendedActions(actionPlan(firstPlanId, firstContextId, actionList(3)));
        assertEquals(3, allMarketActions().size());

        actionStore.populateRecommendedActions(actionPlan(firstPlanId, firstContextId, actionList(1)));
        assertEquals(1, allMarketActions().size());
    }

    @Test
    public void testPopulateRecommendedActionsWithDifferentContext() throws Exception {
        actionStore.populateRecommendedActions(actionPlan(firstPlanId, firstContextId, actionList(3)));
        assertEquals(3, marketActionsForContext(firstContextId).size());
        assertEquals(0, marketActionsForContext(secondContextId).size());

        actionStore.populateRecommendedActions(actionPlan(secondPlanId, secondContextId, actionList(4)));
        assertEquals(0, marketActionsForContext(firstContextId).size());
        assertEquals(4, marketActionsForContext(secondContextId).size());
    }

    @Test
    public void testSizeInitial() throws Exception {
        assertEquals(0, actionStore.size());
    }

    @Test
    public void testSizeAfterPopulate() throws Exception {
        actionStore.populateRecommendedActions(actionPlan(firstPlanId, firstContextId, actionList(3)));
        assertEquals(3, actionStore.size());
    }

    @Test
    public void testGetActionViewsWhenEmpty() throws Exception {
        assertTrue(actionStore.getActionViews().isEmpty());
    }

        @Test
    public void testGetActionViews() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        Collection<ActionView> actionViews = actionStore.getActionViews().values();
        assertEquals(1, actionViews.size());
        assertEquals(action, actionViews.iterator().next().getRecommendation());
    }

    @Test
    public void testGetActionView() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        final ActionView actionView = actionStore.getActionView(1).get();
        assertEquals(action, actionView.getRecommendation());
        assertEquals(firstPlanId, actionView.getActionPlanId());
        assertTrue(actionView.getRecommendation().getExecutable());
        assertEquals(ActionMode.MANUAL, actionView.getMode());
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
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedActions(actionPlan);

        ActionOrchestratorTestUtils.assertActionsEqual(
            actionFactory.newAction(action, actionRecommendationTime, firstPlanId),
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
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, actionList);
        actionStore.populateRecommendedActions(actionPlan);

        Map<Long, Action> actionsMap = actionStore.getActions();
        actionList.forEach(action ->
            ActionOrchestratorTestUtils.assertActionsEqual(
                actionFactory.newAction(action, actionRecommendationTime, firstPlanId),
                actionsMap.get(action.getId())
            )
        );
    }

    @Test
    public void testOverwriteActionsEmptyStore() throws Exception {
        final List<Action> actionList = actionList(3).stream()
            .map(marketAction -> actionFactory.newAction(marketAction, actionRecommendationTime,
                    firstPlanId))
            .collect(Collectors.toList());

        actionStore.overwriteActions(actionList);
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
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, actionList(4));
        actionStore.populateRecommendedActions(actionPlan);

        final int numOverwriteActions = 2;
        final List<Action> overwriteActions = actionList(numOverwriteActions).stream()
            .map(marketAction -> actionFactory.newAction(marketAction, actionRecommendationTime,
                    firstPlanId))
            .collect(Collectors.toList());

        actionStore.overwriteActions(overwriteActions);
        Map<Long, Action> actionsMap = actionStore.getActions();
        assertEquals(numOverwriteActions, actionsMap.size());
        overwriteActions.forEach(action ->
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
        assertFalse(actionStore.getActionPlanId().isPresent());
    }

    @Test
    public void testClear() {
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, actionList(2));
        actionStore.populateRecommendedActions(actionPlan);

        assertEquals(2, actionStore.size());
        assertEquals(firstPlanId, (long) actionStore.getActionPlanId().get());

        assertTrue(actionStore.clear());
        assertEquals(0, actionStore.size());
        assertFalse(actionStore.getActionPlanId().isPresent());
    }

    @Test
    public void testGetTopologyContextId() {
        assertEquals(firstContextId, actionStore.getTopologyContextId());
    }

    @Test
    public void testStoreLoaderWithNoStores() {
        List<ActionStore> loadedStores =
            new PlanActionStore.StoreLoader(dsl, actionFactory, actionModeCalculator).loadActionStores();

        assertTrue(loadedStores.isEmpty());
    }

    @Test
    public void testStoreLoader() {
        final ActionPlan actionPlan = actionPlan(firstPlanId, firstContextId, actionList(2));
        actionStore.populateRecommendedActions(actionPlan);

        List<ActionStore> loadedStores =
            new PlanActionStore.StoreLoader(dsl, actionFactory, actionModeCalculator).loadActionStores();
        assertEquals(1, loadedStores.size());
        assertEquals(2, loadedStores.get(0).size());
        assertEquals(firstContextId, loadedStores.get(0).getTopologyContextId());
    }

    private static List<ActionDTO.Action> actionList(final int numActions) {
        return IntStream.range(0, numActions)
            .mapToObj(i -> ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next()))
            .collect(Collectors.toList());
    }

    private static ActionPlan actionPlan(final long planId, final long topologyContextId,
                                         @Nonnull final Collection<ActionDTO.Action> actions) {

        return ActionPlan.newBuilder()
            .setId(planId)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(IdentityGenerator.next())
                        .setTopologyContextId(topologyContextId)
                        .setTopologyType(TopologyType.PLAN))))
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
