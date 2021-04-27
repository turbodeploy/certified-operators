package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.savings.ActionListener.EntityActionInfo;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

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
    private EntityCostStore entityCostStore = mock(EntityCostStore.class);
    private ProjectedEntityCostStore projectedEntityCostStore = mock(ProjectedEntityCostStore.class);
    private static final double EPSILON_PRECISION = 0.0000001d;

    private static final long ASSOCIATED_SERVICE_ID = 4L;
    private static final int ASSOCIATED_ENTITY_TYPE = 10;

    private final CurrencyAmount currencyAmount = buildCurrencyAmount(1.111, 1);
    private final CurrencyAmount currencyAmount1 = buildCurrencyAmount(1.21, 1);
    private final CurrencyAmount currencyAmount2 = buildCurrencyAmount(1.51, 1);

    private final ComponentCost componentCost = buildComponentCost(currencyAmount.getAmount(),
                                                                   CostCategory.ON_DEMAND_COMPUTE,
                                                                   CostSource.ON_DEMAND_RATE);
    private final ComponentCost componentCost1 = buildComponentCost(currencyAmount1.getAmount(),
                                                                   CostCategory.STORAGE,
                                                                   CostSource.ON_DEMAND_RATE);
    private final ComponentCost componentCost2 = buildComponentCost(currencyAmount2.getAmount(),
                                                                    CostCategory.ON_DEMAND_COMPUTE,
                                                                    CostSource.ON_DEMAND_RATE);

    private final EntityCost entityCost = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                          ImmutableSet.of(componentCost),
                                                          ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost1 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost, componentCost1),
                                                           ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost2 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost1),
                                                           ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost3 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost2),
                                                           ASSOCIATED_ENTITY_TYPE);
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
        store = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        actionsService = ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        // Test gRPC server to mock out actions service gRPC dependencies.
        GrpcTestServer costGrpcServer = GrpcTestServer.newServer(costServiceRpc);
        costGrpcServer.start();
        costService = CostServiceGrpc.newBlockingStub(costGrpcServer.getChannel());
        // Initialize ActionListener with a one hour action lifetime.
        actionListener = new ActionListener(store, actionsService, costService,
                                            entityCostStore, projectedEntityCostStore,
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
     *
     * @throws DbException DBException if one of the queries to the DB were to fail.
     */
    @Test
    public void testQueryEntityCosts() throws DbException {
        final long vmId1 = 101;
        final long vmId2 = 102;
        final long dbId1 = 201;
        final long volumeId1 = 301;

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

        final Map<Long, EntityActionInfo> entityIdToActionInfoMap = ImmutableMap.of(
                vmId1, new EntityActionInfo(actionSpecVm1, actionEntityVm1),
                vmId2, new EntityActionInfo(actionSpecVm2, actionEntityVm2),
                dbId1, new EntityActionInfo(actionSpecDb1, actionEntityDb1),
                volumeId1, new EntityActionInfo(actionSpecVv1, actionEntityVv1)
        );

        // Make up fake responses.
        makeCostResponse();

        // Call query to fill in entity action info map.
        actionListener.queryEntityCosts(entityIdToActionInfoMap);

        // Verify returned map to confirm costs are setup correctly.
        assertNotNull(entityIdToActionInfoMap);

        final EntityActionInfo eai1 = entityIdToActionInfoMap.get(vmId1);
        final EntityActionInfo eai2 = entityIdToActionInfoMap.get(vmId2);

        // vmId1 entityCost1 --> entityCost2; vmId2 entityCost3 --> entityCost1
        // Storage costs won't be included for VMs.
        assertNotEquals("vmId1 before cost test", entityCost1.getTotalAmount().getAmount(), eai1.getEntityActionCosts().getBeforeCosts(), EPSILON_PRECISION);
        // since entityCost2 involve the category IP, which is not one of the categories we query for, the after cost ends up
        // being 0 here instead of entityCost2.getTotalAmount().getAmount().
        assertEquals("vmId1 after cost test", 0, eai1.getEntityActionCosts().getAfterCosts(),  EPSILON_PRECISION);


        assertEquals("vmId2 before cost test", entityCost3.getTotalAmount().getAmount(), eai2.getEntityActionCosts().getBeforeCosts(), EPSILON_PRECISION);
        // Storage costs won't be included for VMs.
        assertNotEquals("vmId2 after cost test", entityCost1.getTotalAmount().getAmount(), eai2.getEntityActionCosts().getAfterCosts(),  EPSILON_PRECISION);
    }

    /**
     * Tests if action id and source and destination oid fields are set correctly.
     */
    @Test
    public void getEntityActionInfo() {
        final int entityType = EntityType.VIRTUAL_VOLUME_VALUE;
        final long sourceTierId = 73705874639937L;
        final long destinationTierId = 73741897536608L;
        final long actionId = 101L;
        ActionEntity actionEntity = ActionEntity.newBuilder()
                .setId(10001L)
                .setType(entityType)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        ActionSpec actionSpec = ActionSpec.newBuilder()
                .setRecommendationId(1001)
                .setRecommendation(Action.newBuilder().setId(actionId)
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
     * @throws DbException DBException if one of the queries to the DB were to fail.
     */
    private void makeCostResponse() throws DbException {
        final long vmId1 = 101;
        final long vmId2 = 102;

        Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
        beforeEntityCostbyOid.put(vmId1, entityCost1);
        beforeEntityCostbyOid.put(vmId2, entityCost3);

        Map<Long, EntityCost> afterEntityCostbyOid = new HashMap<>();
        afterEntityCostbyOid.put(vmId1, entityCost2);
        afterEntityCostbyOid.put(vmId2, entityCost1);

        given(projectedEntityCostStore.getProjectedEntityCosts(any(EntityCostFilter.class))).willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));
    }

    /**
     * Build ComponentCost protobuf for testing.
     *
     * @param amount  The currency amount.
     * @param costCategory The Cost Category.
     * @param costSource The Cost Source.
     * @return ComponentCost.
     */
    private ComponentCost buildComponentCost(final double amount, @Nonnull final CostCategory costCategory,
                                @Nonnull final CostSource costSource) {
      final ComponentCost componentCost = ComponentCost.newBuilder()
                .setAmount(buildCurrencyAmount(amount, 1))
                .setCategory(costCategory)
                .setCostSource(costSource)
                .build();
       return componentCost;
    }

    /**
     * Build EntityCost protobuf for testing.
     *
     * @param serviceId The Associated Service Id.
     * @param componentCosts The Components Costs.
     * @param entityType The entity type.
     * @return EntityCost.
     */
    private EntityCost buildEntityCost(final long serviceId,
                                       @Nonnull final Set<ComponentCost> componentCosts,
                                       final int entityType) {
        final EntityCost entityCost = EntityCost.newBuilder()
                        .setAssociatedEntityId(serviceId)
                        .addAllComponentCost(componentCosts)
                        .setTotalAmount(buildCurrencyAmount(componentCosts.stream()
                                        .map(cc -> cc.getAmount().getAmount())
                                        .reduce(0d, Double::sum), 1))
                        .setAssociatedEntityType(entityType)
                        .build();
        return entityCost;
    }

    /**
     * Build CurrencyAmount protobuf for testing.
     *
     * @param amount The amount.
     * @param currency The Currency.
     * @return CurrencyAmount.
     */
    private CurrencyAmount buildCurrencyAmount(final double amount, final int currency) {
        final CurrencyAmount currencyAmount = CurrencyAmount.newBuilder().setAmount(amount).setCurrency(currency).build();
        return currencyAmount;
    }
}
