package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.savings.ActionListener.EntityActionCosts;
import com.vmturbo.cost.component.savings.ActionListener.EntityActionInfo;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Tests for the action listener.
 */
public class ActionListenerTest {
    private static final Long succeededActionId1 = 1234L;
    private static final Long succeededActionId2 = 4321L;
    private static final String succeededActionMsg = "Success";
    private static final Long actionPlanId = 729L;
    private static final Long realTimeTopologyContextId = 777777L;
    private EntityEventsJournal store;
    private ActionListener actionListener;
    private final ActionsServiceMole actionsServiceRpc =
                                                       Mockito.spy(new ActionsServiceMole());
    private final CostServiceMole costServiceRpc =
                                                 Mockito.spy(new CostServiceMole());
    private ActionsServiceBlockingStub actionsService;
    private CostServiceBlockingStub costService;
    private static final double EPSILON_PRECISION = 0.0000001d;

    /**
     * Test gRPC server to mock out actions service gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceRpc);

    /**
     * Setup before each test.
     *
     * @throws IOException on error
     */
    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        store = new InMemoryEntityEventsJournal();
        actionsService = ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        // Test gRPC server to mock out actions service gRPC dependencies.
        GrpcTestServer costGrpcServer = GrpcTestServer.newServer(costServiceRpc);
        costGrpcServer.start();
        costService = CostServiceGrpc.newBlockingStub(costGrpcServer.getChannel());
        // Initialize ActionListener with a one hour action lifetime.
        actionListener = new ActionListener(store, actionsService, costService,
                                            realTimeTopologyContextId,
                EntitySavingsConfig.getSupportedEntityTypes(),
                EntitySavingsConfig.getSupportedActionTypes(), 3600000L, 0L);

        Map<Long, CurrencyAmount> beforeOnDemandComputeCostByEntityOidMap = new HashMap<>();
        beforeOnDemandComputeCostByEntityOidMap.put(1L, CurrencyAmount.newBuilder()
                        .setAmount(2).build());
        beforeOnDemandComputeCostByEntityOidMap.put(3L, CurrencyAmount.newBuilder()
                        .setAmount(3).build());
        Map<Long, CurrencyAmount> afterOnDemandComputeCostByEntityOidMap = new HashMap<>();
        afterOnDemandComputeCostByEntityOidMap.put(1L, CurrencyAmount.newBuilder()
                        .setAmount(2).build());
        afterOnDemandComputeCostByEntityOidMap.put(3L, CurrencyAmount.newBuilder()
                        .setAmount(1).build());
        doReturn(GetTierPriceForEntitiesResponse.newBuilder()
                         .putAllBeforeTierPriceByEntityOid(beforeOnDemandComputeCostByEntityOidMap)
                         .putAllAfterTierPriceByEntityOid(afterOnDemandComputeCostByEntityOidMap)
                         .build())
                        .when(costServiceRpc)
                        .getTierPriceForEntities(any(GetTierPriceForEntitiesRequest.class));
    }

    /**
     * Test processing of successfully executed actions.
     */
    @Test
    public void testProcessActionSuccess() {
        final Scale scale1 = Scale.newBuilder()
                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                            .setId(1L)
                            .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                            .build())
                        .build();
        final Scale scale2 = Scale.newBuilder()
                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                            .setId(3L)
                            .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                            .build())
                        .build();
        ActionSpec actionSpec1 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(1234L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(succeededActionId1)
                                        .setDeprecatedImportance(1.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scale1)
                                                        .build())
                                        .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpec2 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5658L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(succeededActionId2)
                            .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                            .setDeprecatedImportance(1.0)
                            .setInfo(ActionInfo.newBuilder()
                                            .setScale(scale2)
                                            .build())
                            .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();

        ActionSuccess actionSuccess1 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId1)
                        .setActionSpec(actionSpec1)
                        .build();
        ActionSuccess actionSuccess2 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId2)
                        .setActionSpec(actionSpec2)
                        .build();

        actionListener.onActionSuccess(actionSuccess1);
        assertEquals(1, store.size());
        actionListener.onActionSuccess(actionSuccess2);
        assertEquals(2, store.size());

        // If a succeeded message were to be received more than once
        // an additional entry should not be created.
        actionListener.onActionSuccess(actionSuccess2);
        assertEquals(2, store.size());

        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        savingsEvents.stream().forEach(se -> {
            assertEquals(se.getActionEvent().get().getEventType(),
                         ActionEventType.SCALE_EXECUTION_SUCCESS);
        });
    }

    /**
     * Test processing of new pending actions.
     */
    @Test
    public void testProcessNewPendingActions() {
        final Scale scale1 = Scale.newBuilder()
                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                    .setId(1L)
                                    .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                    .build())
                        .build();
        final Delete delete = Delete.newBuilder()
                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                    .setId(2L)
                                    .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
                                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                    .build())
                        .build();
        final Scale scale2 = Scale.newBuilder()
                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                    .setId(3L)
                                    .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
                                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                    .build())
                        .build();
        ActionSpec actionSpec1 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(1234L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(1L)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setDeprecatedImportance(1.0)
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scale1)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpec2 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5658L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(2L)
                                        .setDeprecatedImportance(1.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setDelete(delete)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpec3 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5654L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(3L)
                                        .setDeprecatedImportance(1.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scale2)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();


        FilteredActionResponse filteredResponse1 = FilteredActionResponse.newBuilder()
                        .setActionChunk(FilteredActionResponse.ActionChunk.newBuilder()
                                        .addActions(ActionOrchestratorAction.newBuilder()
                                                        .setActionSpec(actionSpec1)))
                        .build();
        FilteredActionResponse filteredResponse2 = FilteredActionResponse.newBuilder()
                        .setActionChunk(FilteredActionResponse.ActionChunk.newBuilder()
                                        .addActions(ActionOrchestratorAction.newBuilder()
                                                        .setActionSpec(actionSpec2)))
                        .build();
        FilteredActionResponse filteredResponse3 = FilteredActionResponse.newBuilder()
                        .setActionChunk(FilteredActionResponse.ActionChunk.newBuilder()
                                        .addActions(ActionOrchestratorAction.newBuilder()
                                                        .setActionSpec(actionSpec3)))
                        .build();

        doReturn(Arrays.asList(filteredResponse1, filteredResponse2, filteredResponse3))
                        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                        .setActionPlanId(actionPlanId)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                            .setMarket(MarketActionPlanInfo.newBuilder()
                                            .setSourceTopologyInfo(TopologyInfo
                                                .newBuilder()
                                                .setTopologyContextId(realTimeTopologyContextId))))
                        .build());

        // Monitoring savings for VM and other cloud entities Scale actions.
        assertEquals(3, store.size());
        assertEquals(3, actionListener.getEntityActionStateMap().size());
        assertEquals(3, actionListener.getCurrentPendingActionsActionSpecToEntityIdMap().size());

        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        savingsEvents.stream().forEach(se -> {
            assertEquals(se.getActionEvent().get().getEventType(),
                         ActionEventType.RECOMMENDATION_ADDED);
        });

        doReturn(Arrays.asList(filteredResponse2, filteredResponse3))
        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
        .setActionPlanId(actionPlanId)
        .setActionPlanInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                            .setSourceTopologyInfo(TopologyInfo
                                .newBuilder()
                                .setTopologyContextId(realTimeTopologyContextId))))
        .build());

        // Monitoring savings for VM and other cloud entities Scale actions.
        assertEquals(1, store.size());
        assertEquals(2, actionListener.getEntityActionStateMap().size());
        assertEquals(2, actionListener.getCurrentPendingActionsActionSpecToEntityIdMap().size());

        savingsEvents = store.removeAllEvents();
        savingsEvents.stream().forEach(se -> {
            assertEquals(se.getActionEvent().get().getEventType(),
                         ActionEventType.RECOMMENDATION_REMOVED);
        });
    }

    /**
     * Verify cost rpc query results are being collected correctly.
     */
    @Test
    public void queryEntityCosts() {
        long vmId1 = 101;
        long vmId2 = 102;
        long dbId1 = 201;
        long volumeId1 = 301;

        final ActionEntity actionEntityVm1 = ActionDTO.ActionEntity.newBuilder()
                        .setId(101L)
                        .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        final Scale scaleVm1 = Scale.newBuilder()
                        .setTarget(actionEntityVm1)
                        .build();
        final ActionEntity actionEntityVm2 = ActionDTO.ActionEntity.newBuilder()
                        .setId(102L)
                        .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        final Scale scaleVm2 = Scale.newBuilder()
                        .setTarget(actionEntityVm2)
                        .build();
        final ActionEntity actionEntityDb1 = ActionDTO.ActionEntity.newBuilder()
                        .setId(201L)
                        .setType(CommonDTO.EntityDTO.EntityType.DATABASE_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        final Scale scaleDb1 = Scale.newBuilder()
                        .setTarget(actionEntityDb1)
                        .build();
        final ActionEntity actionEntityVv1 = ActionDTO.ActionEntity.newBuilder()
                        .setId(301L)
                        .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
        final Scale scaleVv1 = Scale.newBuilder()
                        .setTarget(actionEntityVv1)
                        .build();
        ActionSpec actionSpecVm1 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(1234L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(1L)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setDeprecatedImportance(0.0)
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scaleVm1)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpecVm2 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5658L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(2L)
                                        .setDeprecatedImportance(0.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scaleVm2)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpecDb1 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5654L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(3L)
                                        .setDeprecatedImportance(3.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scaleDb1)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        ActionSpec actionSpecVv1 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(5654L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(4L)
                                        .setDeprecatedImportance(4.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scaleVv1)
                                                        .build())
                                        .build())
                        .setRecommendationTime(System.currentTimeMillis())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();

        // Setup cost category -> entities mapping.
        final Map<CostCategory, Set<Long>> categoryToEntities = new HashMap<>();
        // TreeSet needed because of argument match ordering.
        categoryToEntities.put(CostCategory.ON_DEMAND_COMPUTE, new TreeSet<>(ImmutableSet.of(vmId1,
                vmId2, dbId1)));
        categoryToEntities.put(CostCategory.ON_DEMAND_LICENSE, new TreeSet<>(ImmutableSet.of(vmId1,
                vmId2)));
        categoryToEntities.put(CostCategory.STORAGE, new TreeSet<>(ImmutableSet.of(dbId1,
                volumeId1)));

        final Map<Long, EntityActionInfo> entityIdToActionInfoMap = ImmutableMap.of(
                vmId1, new EntityActionInfo(actionSpecVm1, actionEntityVm1),
                vmId2, new EntityActionInfo(actionSpecVm2, actionEntityVm2),
                dbId1, new EntityActionInfo(actionSpecDb1, actionEntityDb1),
                volumeId1, new EntityActionInfo(actionSpecVv1, actionEntityVv1)
        );

        // Setup fake costs.
        final Map<Pair<Long, CostCategory>, EntityActionCosts> entityCosts = new HashMap<>();
        final Map<Long, EntityActionCosts> totalCosts = new HashMap<>();
        entityCosts.put(ImmutablePair.of(vmId1, CostCategory.ON_DEMAND_COMPUTE),
                new EntityActionCosts(100.0, 50.0));
        entityCosts.put(ImmutablePair.of(vmId1, CostCategory.ON_DEMAND_LICENSE),
                new EntityActionCosts(10.0, 5.0));
        totalCosts.put(vmId1, new EntityActionCosts(110.0, 55.0));

        entityCosts.put(ImmutablePair.of(vmId2, CostCategory.ON_DEMAND_COMPUTE),
                new EntityActionCosts(200.0, 400.0));
        entityCosts.put(ImmutablePair.of(vmId2, CostCategory.ON_DEMAND_LICENSE),
                new EntityActionCosts(20.0, 40.0));
        totalCosts.put(vmId2, new EntityActionCosts(220.0, 440.0));

        entityCosts.put(ImmutablePair.of(dbId1, CostCategory.ON_DEMAND_COMPUTE),
                new EntityActionCosts(300.0, 150.0));
        entityCosts.put(ImmutablePair.of(dbId1, CostCategory.STORAGE),
                new EntityActionCosts(50.0, 25.0));
        totalCosts.put(dbId1, new EntityActionCosts(350.0, 175.0));

        entityCosts.put(ImmutablePair.of(volumeId1, CostCategory.STORAGE),
                new EntityActionCosts(30.0, 15.0));
        totalCosts.put(volumeId1, new EntityActionCosts(30.0, 15.0));

        // Make up fake responses.
        makeCostResponse(CostCategory.ON_DEMAND_COMPUTE, entityCosts);
        makeCostResponse(CostCategory.ON_DEMAND_LICENSE, entityCosts);
        makeCostResponse(CostCategory.STORAGE, entityCosts);

        // Call query to fill in entity action info map.
        actionListener.queryEntityCosts(categoryToEntities, entityIdToActionInfoMap);

        // Verify returned map to confirm costs are setup correctly.
        assertNotNull(entityIdToActionInfoMap);

        totalCosts.forEach((entityId, expectedTotalCosts) -> {
            EntityActionInfo entityActionInfo = entityIdToActionInfoMap.get(entityId);
            assertNotNull(entityActionInfo);
            EntityActionCosts actualTotals = entityActionInfo.getTotalCosts();
            EntityActionCosts expectedTotals = totalCosts.get(entityId);
            assertEquals(expectedTotals.beforeCosts, actualTotals.beforeCosts, EPSILON_PRECISION);
            assertEquals(expectedTotals.afterCosts, actualTotals.afterCosts, EPSILON_PRECISION);
        });
    }

    /**
     * Tests if action id and source and destination oid fields are set correctly.
     */
    @Test
    public void getEntityActionInfo() {
        int entityType = EntityType.VIRTUAL_VOLUME_VALUE;
        long sourceTierId = 73705874639937L;
        long destinationTierId = 73741897536608L;
        ActionEntity actionEntity = ActionEntity.newBuilder()
                .setId(10001L)
                .setType(entityType)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        ActionSpec actionSpec = ActionSpec.newBuilder()
                .setRecommendationId(1001)
                .setRecommendation(Action.newBuilder().setId(101L)
                        .setExplanation(Explanation.getDefaultInstance())
                        .setDeprecatedImportance(1.0)
                        .setInfo(ActionInfo.newBuilder()
                                .setScale(Scale.newBuilder()
                                        .setTarget(actionEntity)
                                        .addChanges(ChangeProvider.newBuilder()
                                                .setSource(ActionEntity.newBuilder()
                                                        .setId(sourceTierId)
                                                        .setType(EntityType.STORAGE_TIER_VALUE)
                                                        .setEnvironmentType(EnvironmentType.CLOUD)
                                                        .build())
                                                .setDestination(ActionEntity.newBuilder()
                                                        .setId(destinationTierId)
                                                        .setType(EntityType.STORAGE_TIER_VALUE)
                                                        .setEnvironmentType(EnvironmentType.CLOUD)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setRecommendationTime(100001L)
                .setActionState(ActionState.READY)
                .build();

        long actionId = 101L;
        final EntityActionInfo entityActionInfo = new EntityActionInfo(actionSpec, actionEntity);
        assertNotNull(entityActionInfo);
        assertEquals(actionId, entityActionInfo.getActionId());
        assertEquals(actionEntity.getType(), entityActionInfo.getEntityType());
        assertEquals(sourceTierId, entityActionInfo.getSourceOid());
        assertEquals(destinationTierId, entityActionInfo.getDestinationOid());
    }

    /**
     * Makes up fake cost responses based on inputs.
     *
     * @param category Cost category to get responses for.
     * @param entityCosts Map of costs to use by category type.
     */
    private void makeCostResponse(CostCategory category,
            final Map<Pair<Long, CostCategory>, EntityActionCosts> entityCosts) {
        Set<Long> entities = entityCosts.keySet().stream()
                .filter(pair -> pair.getValue() == category)
                .map(Pair::getKey)
                .collect(Collectors.toCollection(TreeSet::new));

        final GetTierPriceForEntitiesRequest request = GetTierPriceForEntitiesRequest.newBuilder()
                .addAllOids(entities)
                .setCostCategory(category)
                .setTopologyContextId(realTimeTopologyContextId)
                .build();

        final GetTierPriceForEntitiesResponse response = GetTierPriceForEntitiesResponse.newBuilder()
                .putAllBeforeTierPriceByEntityOid(getCostCategoryMap(category, entityCosts, true))
                .putAllAfterTierPriceByEntityOid(getCostCategoryMap(category, entityCosts, false))
                .build();

        doReturn(response)
                .when(costServiceRpc).getTierPriceForEntities(request);

        assertNotNull(response);
    }

    /**
     * Gets a currency map with either before or after action costs, keyed off of entity id.
     *
     * @param category Costs category.
     * @param entityCosts Costs to use.
     * @param isBefore If true, before costs are set, else after action costs.
     * @return Currency map.
     */
    private Map<Long, CurrencyAmount> getCostCategoryMap(CostCategory category,
            Map<Pair<Long, CostCategory>, EntityActionCosts> entityCosts, boolean isBefore) {
        final Map<Long, CurrencyAmount> currencyMap = new HashMap<>();
        entityCosts.forEach((pair, actionCosts) -> {
            if (pair.getValue() == category) {
                final double cost = isBefore ? actionCosts.beforeCosts : actionCosts.afterCosts;
                final CurrencyAmount amt = CurrencyAmount.newBuilder().setAmount(cost).build();
                currencyMap.put(pair.getKey(), amt);
            }
        });
        return currencyMap;
    }
}
