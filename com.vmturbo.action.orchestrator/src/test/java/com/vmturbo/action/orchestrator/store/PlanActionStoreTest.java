package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.db.tables.MarketAction.MARKET_ACTION;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.ListUtils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.pojos.MarketAction;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeaturesRequiredException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Integration tests related to the {@link PlanActionStoreTest}.
 */
@RunWith(Parameterized.class)
public class PlanActionStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public PlanActionStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(com.vmturbo.action.orchestrator.db.Action.ACTION, configurableDbDialect, dialect, "action",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

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
        public Action newAction(@Nonnull final ActionDTO.Action recommendation, long actionPlanId, long recommendationOid) {
            return spy(new Action(recommendation, actionPlanId, actionModeCalculator, recommendationOid));
        }

        @Nonnull
        @Override
        public Action newPlanAction(@Nonnull ActionDTO.Action recommendation, @Nonnull LocalDateTime recommendationTime,
                                    long actionPlanId, String description,
                                    @Nullable final Long associatedAccountId,
                                    @Nullable final Long associatedResourceGroupId) {
            return spy(new Action(recommendation, recommendationTime, actionPlanId,
                    actionModeCalculator, description, associatedAccountId, associatedResourceGroupId,
                    IdentityGenerator.next()));
        }
    }

    private final LocalDateTime actionRecommendationTime = LocalDateTime.now();
    private final IActionFactory actionFactory = mock(IActionFactory.class);
    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private PlanActionStore actionStore;

    private final EntitiesAndSettingsSnapshotFactory entitiesSnapshotFactory = mock(EntitiesAndSettingsSnapshotFactory.class);
    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);

    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    private LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private static ActionDTO.Action resize1;
    private static ActionDTO.Action resize2;
    private static ActionDTO.Action atomicAction;
    private static ActionDTO.Action dedupAction1;
    private static ActionPipelineStages.AtomicActionsPlan atomicActionsPlan;
    private static List<ActionDTO.Action> mergedActions;
    private static ActionPipelineStages.AtomicActionsPlan emptyAtomicActionsPlan;
    private static List<ActionDTO.Action> emptyMergedActions;
    private static int container1 = 21;
    private static int container2 = 22;
    private static int dedup1 = 23;
    private static int dedup2 = 24;
    private static int aggregate = 25;
    private static long resizeActionId1 = 101l;
    private static long resizeActionId2 = 102l;
    private static long dedupActionId1 = 103l;
    private static long dedupActionId2 = 104l;
    private static long atomicActionId = 105l;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        actionStore = new PlanActionStore(spyActionFactory, dsl, firstContextId,
            entitiesSnapshotFactory, actionTranslator, realtimeId,
            actionTargetSelector, licenseCheckClient);

        // Enforce that all actions created with this factory get the same recommendation time
        // so that actions can be easily compared.
        when(actionFactory.newPlanAction(any(ActionDTO.Action.class),
            any(LocalDateTime.class), anyLong(), anyString(), any(), any())).thenAnswer(
            invocation -> {
                Object[] args = invocation.getArguments();
                return new Action((ActionDTO.Action) args[0], actionRecommendationTime, (Long) args[2],
                        actionModeCalculator,  "Move VM from H1 to H2", 321L, 121L,
                        IdentityGenerator.next());
            });
        setEntitiesOIDs();

        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action ->
                ImmutableActionTargetInfo.builder()
                    .targetId(100L).supportingLevel(SupportLevel.SUPPORTED).build()));
        });

        setUpAtomicActionsPlan();
    }

    private void setUpAtomicActionsPlan() {
        resize1 = createResizeAction(resizeActionId1, container1, EntityType.CONTAINER_VALUE, CommonDTO.CommodityDTO.CommodityType.VCPU);
        resize2 = createResizeAction(resizeActionId2, container2, EntityType.CONTAINER_VALUE, CommonDTO.CommodityDTO.CommodityType.VCPU);

        mergedActions = new ArrayList<>();
        mergedActions.add(resize1);
        mergedActions.add(resize2);

        emptyMergedActions = new ArrayList<>();

        dedupAction1 = createDedupAtomicAction(dedupActionId1, dedup1);
        atomicAction = createAtomicAction(atomicActionId);

        atomicActionsPlan = new ActionPipelineStages.AtomicActionsPlan(Arrays.asList(atomicAction), Arrays.asList(dedupAction1));

        emptyAtomicActionsPlan = new ActionPipelineStages.AtomicActionsPlan(Collections.emptyList(), Collections.emptyList());
    }

    private static ActionDTO.Action createResizeAction(long actionId,
                                                       long entityId, int entityType,
                                                       CommonDTO.CommodityDTO.CommodityType commType) {
        ActionDTO.Explanation.ResizeExplanation resizeExplanation = ActionDTO.Explanation.ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(20)
                .setDeprecatedEndUtilization(90)
                .build();

        ActionDTO.Action resize = ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setDeprecatedImportance(0)
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setResize(ActionDTO.Resize.newBuilder()
                                .setTarget(ActionDTO.ActionEntity.newBuilder()
                                        .setId(entityId).setType(entityType))
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(commType.getNumber())))
                )
                .setExplanation(ActionDTO.Explanation.newBuilder()
                        .setResize(resizeExplanation))
                .build();
        return resize;
    }

    private static ActionDTO.Action createDedupAtomicAction(long actionId, int dedupEntity) {
        ActionDTO.ActionInfo actionInfo = ActionDTO.ActionInfo.newBuilder()
                .setAtomicResize(ActionDTO.AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(dedupEntity, EntityType.CONTAINER_SPEC))
                        .addResizes(createResizeInfo(dedupEntity, EntityType.CONTAINER_SPEC,
                                                    CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber()))
                        .build())
                .build();
        final ActionDTO.Action dedupAction =ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setDeprecatedImportance(0)
                .setInfo(actionInfo)
                .setExplanation(ActionDTO.Explanation.newBuilder()
                        .setAtomicResize(ActionDTO.Explanation.AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("dedup1")
                                .addEntityIds(container1)
                                .addPerEntityExplanation(
                                        ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity
                                                .newBuilder()
                                                .setTargetId(container1)
                                                .addResizeEntityIds(container1))
                        )
                )
                .build();

        return dedupAction;
    }

    private static ActionDTO.Action createAtomicAction(long actionId) {
        ActionDTO.ActionInfo actionInfo = ActionDTO.ActionInfo.newBuilder()
                .setAtomicResize(ActionDTO.AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(aggregate, EntityType.WORKLOAD_CONTROLLER))
                        .addResizes(createResizeInfo(dedup1, EntityType.CONTAINER_SPEC,
                                                        CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber()))
                        .addResizes(createResizeInfo(dedup2, EntityType.CONTAINER_SPEC,
                                                        CommonDTO.CommodityDTO.CommodityType.VCPU.getNumber()))
                        .build())
                .build();

        final ActionDTO.Action atomicAction = ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setDeprecatedImportance(0)
                .setInfo(actionInfo)
                .setExplanation(ActionDTO.Explanation.newBuilder()
                        .setAtomicResize(ActionDTO.Explanation.AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("aggregate")
                                .addEntityIds(dedup1)
                                .addEntityIds(dedup2)
                                .addPerEntityExplanation(
                                        ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity
                                                .newBuilder()
                                                .setTargetId(dedup1)
                                                .addResizeEntityIds(dedup1))
                                .addPerEntityExplanation(
                                        ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity
                                                .newBuilder()
                                                .setTargetId(dedup2)
                                                .addResizeEntityIds(dedup2))
                        ))
                .build();

        return atomicAction;
    }

    private static ActionDTO.ResizeInfo.Builder createResizeInfo(long id, EntityType entityType, int commType) {
        return ActionDTO.ResizeInfo.newBuilder()
                .setTarget(createActionEntity(id, entityType))
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(commType))
                .setCommodityAttribute(TopologyDTO.CommodityAttribute.CAPACITY)
                .setOldCapacity(124).setNewCapacity(456);
    }

    private static ActionDTO.ActionEntity createActionEntity(long oid, EntityType entityType) {
        return ActionDTO.ActionEntity.newBuilder()
                .setId(oid)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .setType(entityType.getNumber())
                .build();
    }

    public void setEntitiesOIDs() {
        when(entitiesSnapshotFactory.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        // Hack: if plan source topology is not available, the fall back on realtime.
        when(entitiesSnapshotFactory.newSnapshot(any(), eq(realtimeId))).thenReturn(snapshot);
        for (long i=1; i<10;i++) {
            createMockEntity(i,EntityType.VIRTUAL_MACHINE.getNumber());
        }
        createMockEntity(hostA,EntityType.PHYSICAL_MACHINE.getNumber());
        createMockEntity(hostB,EntityType.PHYSICAL_MACHINE.getNumber());
        for (long i=1; i<2;i++) {
            createMockEntity(20*i,EntityType.CONTAINER.getNumber());
        }
        createMockEntity(dedup1, EntityType.CONTAINER_SPEC.getNumber());
        createMockEntity(dedup2, EntityType.CONTAINER_SPEC.getNumber());
        createMockEntity(aggregate, EntityType.WORKLOAD_CONTROLLER.getNumber());

    }

    private void createMockEntity(long id, int type) {
        when(snapshot.getEntityFromOid(eq(id)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(id, type));
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        assertEquals(0, allMarketActions().size());
        List<ActionDTO.Action> actionList = actionListWithResizeActions(3);

        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(firstPlanId, firstContextId, actionList),
               atomicActionsPlan, mergedActions);

        assertEquals(5, allMarketActions().size());
        assertEquals(5, marketActionsForContext(firstContextId).size());
        assertEquals(0, marketActionsForContext(secondContextId).size());
    }

    @Test
    public void testPopulateRecommendedActionsOverwrite() throws Exception {
        assertEquals(0, allMarketActions().size());
        List<ActionDTO.Action> actionList = actionListWithResizeActions(3);
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(firstPlanId, firstContextId, actionList),
                atomicActionsPlan, mergedActions);
        assertEquals(5, allMarketActions().size());

        List<ActionDTO.Action> actionList2 = actionListWithResizeActions(1);
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(firstPlanId, firstContextId, actionList2),
                atomicActionsPlan, mergedActions);
        assertEquals(3, allMarketActions().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPopulateRecommendedActionsWithDifferentContext() throws Exception {
        List<ActionDTO.Action> actionList1 = actionListWithResizeActions(3);
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(firstPlanId, firstContextId, actionList1),
                                                        atomicActionsPlan, mergedActions);

        List<ActionDTO.Action> actionList2 = actionListWithResizeActions(4);
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(secondPlanId, secondContextId, actionList2),
                                                        atomicActionsPlan, mergedActions);
    }

    @Test
    public void testSizeInitial() throws Exception {
        assertEquals(0, actionStore.size());
    }

    @Test
    public void testSizeAfterPopulate() throws Exception {
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan(firstPlanId, firstContextId,
                actionListWithResizeActions(3)),
                atomicActionsPlan, mergedActions);
        assertEquals(5, actionStore.size());
    }

    @Test
    public void testGetActionViewsWhenEmpty() throws Exception {
        assertTrue(actionStore.getActionViews().isEmpty());
    }

    @Test
    public void testGetActionViews() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        List<ActionDTO.Action> actions = new ArrayList<>(Collections.singleton(action));
        actions.addAll(mergedActions);

        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, actions);
        actionStore.populateRecommendedAndAtomicActions(actionPlan, atomicActionsPlan, mergedActions);

        Collection<ActionView> actionViews = actionStore.getActionViews().getAll().collect(Collectors.toList());
        Map<Long, ActionView> actionViewsMap = actionViews.stream()
                .collect(Collectors.toMap(ActionView::getId, Function.identity()));

        assertEquals(3, actionViews.size());
        assertEquals(action, actionViewsMap.get(action.getId()).getRecommendation());
        assertEquals(atomicAction, actionViewsMap.get(atomicAction.getId()).getRecommendation());
        assertEquals(dedupAction1, actionViewsMap.get(dedupAction1.getId()).getRecommendation());
    }

    @Test
    public void testGetActionView() throws Exception {
        final ActionDTO.Action action = ActionOrchestratorTestUtils.createMoveRecommendation(1, 2, 1, 3, 1, 4);
        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, Collections.singletonList(action));
        actionStore.populateRecommendedAndAtomicActions(actionPlan, emptyAtomicActionsPlan, emptyMergedActions);

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
        actionStore.populateRecommendedAndAtomicActions(actionPlan, emptyAtomicActionsPlan, emptyMergedActions);

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

    /**
     * Test that getAction() is not available on the PlanActionStore if the "planner" feature is
     * not available on the license.
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void testGetActionUnlicensed() {
        doThrow(new LicenseFeaturesRequiredException(Collections.singleton(ProbeLicense.PLANNER)))
            .when(licenseCheckClient).checkFeatureAvailable(ProbeLicense.PLANNER);
        actionStore.getAction(1);
    }

    @Test
    public void testGetActions() throws Exception {
        final List<ActionDTO.Action> actionList = actionList(3);
        List<ActionDTO.Action> actionListWithResizes = new ArrayList<>(actionList);
        actionListWithResizes.addAll(mergedActions);

        final ActionPlan actionPlan = marketActionPlan(firstPlanId, firstContextId, actionListWithResizes);
        actionStore.populateRecommendedAndAtomicActions(actionPlan, atomicActionsPlan, mergedActions);

        Map<Long, Action> actionsMap = actionStore.getActions();
        actionsMap.keySet().stream().forEach(id -> System.out.println("action id: " + id));
        // Check action views for action that were merged
        actionList.forEach(action -> {
            ActionOrchestratorTestUtils.assertActionsEqual(
                    actionFactory.newPlanAction(action, actionRecommendationTime, firstPlanId, "",
                            null, null),
                    actionsMap.get(action.getId()));
        });

        // Check that the resize action that were merged are not available as merged actions
        assertFalse(actionsMap.containsKey(resize1.getId()));
        assertFalse(actionsMap.containsKey(resize2.getId()));

        // Check the actions views for the atomic actions
        ActionOrchestratorTestUtils.assertActionsEqual(
                actionFactory.newPlanAction(atomicAction, actionRecommendationTime, firstPlanId, "",
                        null, null),
                actionsMap.get(atomicAction.getId()));
    }

    /**
     * Test that getActions() is not available on the PlanActionStore if the "planner" feature is
     * not available on the license.
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void testGetActionsUnlicensed() {
        doThrow(new LicenseFeaturesRequiredException(Collections.singleton(ProbeLicense.PLANNER)))
                .when(licenseCheckClient).checkFeatureAvailable(ProbeLicense.PLANNER);
        actionStore.getActions();
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
        actionStore.populateRecommendedAndAtomicActions(actionPlan1, emptyAtomicActionsPlan, emptyMergedActions);

        actionStore.populateRecommendedAndAtomicActions(actionPlan2, emptyAtomicActionsPlan, emptyMergedActions);

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
        actionStore.populateRecommendedAndAtomicActions(actionPlan, emptyAtomicActionsPlan, emptyMergedActions);

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
                actionTargetSelector, licenseCheckClient).loadActionStores();

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
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan, emptyAtomicActionsPlan, emptyMergedActions);
        actionStore.populateRecommendedAndAtomicActions(buyRIActionPlan, emptyAtomicActionsPlan, emptyMergedActions);

        expectedActionStores.put(firstContextId, actionStore);

        // Setup second planActionStore. This has 9 Buy RI Actions
        final ActionPlan buyRIActionPlan2 = buyRIActionPlan(3L, secondContextId, actionList(9));
        PlanActionStore actionStore2 = new PlanActionStore(spyActionFactory, dsl, secondContextId,
            entitiesSnapshotFactory, actionTranslator, realtimeId,
            actionTargetSelector, licenseCheckClient);
        actionStore2.populateRecommendedAndAtomicActions(buyRIActionPlan2, emptyAtomicActionsPlan, emptyMergedActions);

        expectedActionStores.put(secondContextId, actionStore2);

        // Load the stores from DB
        List<ActionStore> loadedStores =
            new PlanActionStore.StoreLoader(dsl, actionFactory, actionModeCalculator, entitiesSnapshotFactory, actionTranslator, realtimeId,
                actionTargetSelector, licenseCheckClient).loadActionStores();
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

        actionStore.populateRecommendedAndAtomicActions(marketActionPlan, emptyAtomicActionsPlan, emptyMergedActions);

        actionStore.populateRecommendedAndAtomicActions(buyRIActionPlan, emptyAtomicActionsPlan, emptyMergedActions);

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

        actionStore.populateRecommendedAndAtomicActions(marketActionPlan1, emptyAtomicActionsPlan, emptyMergedActions);

        assertEquals(2, actionStore.getActionsByActionPlanType().get(ActionPlanType.MARKET).size());
        assertEquals(firstPlanId, actionStore.getActionPlanId(ActionPlanType.MARKET).get().longValue());

        actionStore.populateRecommendedAndAtomicActions(marketActionPlan2, emptyAtomicActionsPlan, emptyMergedActions);
        assertEquals(3, actionStore.getActionsByActionPlanType().get(ActionPlanType.MARKET).size());
        assertEquals(secondPlanId, actionStore.getActionPlanId(ActionPlanType.MARKET).get().longValue());
    }

    /**
     * Tests the case the action description is too long and we truncate the description.
     */
    @Test
    public void testTruncatingLongDescriptions() {
        // ARRANGE
        ActionDTO.Action longAction =
            ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(), 1L,
                hostA, 14, hostB, 14);
        ActionDTO.Action shortAction =
            ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(), 2L,
                hostA, 14, hostB, 14);

        // the name of the vm is very very long
        when(snapshot.getEntityFromOid(eq(1L)))
            .thenReturn(Optional.of(TopologyDTO.PartialEntity.ActionPartialEntity.newBuilder()
                .setOid(1L)
                .setEntityType(10)
                .setDisplayName(String.join("", Collections.nCopies(250, "very"))
                    + "LongName")
                .build()));

        // the name of this vm is reasonable
        when(snapshot.getEntityFromOid(eq(2L)))
            .thenReturn(Optional.of(TopologyDTO.PartialEntity.ActionPartialEntity.newBuilder()
                .setOid(2L)
                .setEntityType(10)
                .setDisplayName("Test 12")
                .build()));

        final ActionPlan marketActionPlan = marketActionPlan(firstPlanId, firstContextId,
            Arrays.asList(longAction, shortAction));

        // ACT
        actionStore.populateRecommendedAndAtomicActions(marketActionPlan, emptyAtomicActionsPlan, emptyMergedActions);

        // ASSERT
        assertThat(actionStore.getActionsByActionPlanType().get(ActionPlanType.MARKET).size(),
            is(2));
        assertThat(actionStore.getAction(longAction.getId()).get().getDescription(),
            endsWith("..."));
        assertTrue(actionStore.getAction(longAction.getId()).get().getDescription().length()
            <= MARKET_ACTION.DESCRIPTION.getDataType().length());
        assertFalse(actionStore.getAction(shortAction.getId()).get().getDescription().endsWith(
            "..."));
    }

    private static List<ActionDTO.Action> actionList(final int numActions) {
        List<ActionDTO.Action> actions =  IntStream.range(0, numActions)
            .mapToObj(i -> ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),i+1,hostA,14,hostB,14))
            .collect(Collectors.toList());

        return actions;
    }

    private static List<ActionDTO.Action> actionListWithResizeActions(final int numActions) {
        List<ActionDTO.Action> actions = actionList(numActions);
        actions.add(resize1);
        actions.add(resize2);

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
