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

import org.jetbrains.annotations.NotNull;
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
    private final ActionsServiceMole actionsServiceRpc = Mockito.spy(new ActionsServiceMole());
    private final CostServiceMole costServiceRpc = Mockito.spy(new CostServiceMole());
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
            ImmutableSet.of(componentCost), ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost1 = buildEntityCost(ASSOCIATED_SERVICE_ID,
            ImmutableSet.of(componentCost, componentCost1), ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost2 = buildEntityCost(ASSOCIATED_SERVICE_ID,
            ImmutableSet.of(componentCost1), ASSOCIATED_ENTITY_TYPE);
    private final EntityCost entityCost3 = buildEntityCost(ASSOCIATED_SERVICE_ID,
            ImmutableSet.of(componentCost2), ASSOCIATED_ENTITY_TYPE);
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
                EntitySavingsConfig.getSupportedActionTypes(), 3600000L, 0L, 1);

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
        final ActionEntity actionEntityScale1 =
                createActionEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale2 =
                createActionEntity(3L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);
        final ActionSpec actionSpec1 = ActionDTO.ActionSpec.newBuilder()
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
        final ActionSpec actionSpec2 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(4321L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(succeededActionId2)
                            .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                            .setDeprecatedImportance(1.0)
                            .setInfo(ActionInfo.newBuilder()
                                            .setScale(scale2)
                                            .build())
                            .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();

        final ActionSuccess actionSuccess1 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId1)
                        .setActionSpec(actionSpec1)
                        .build();
        final ActionSuccess actionSuccess2 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId2)
                        .setActionSpec(actionSpec2)
                        .build();

        // create the conditions that happen after actions progress, so that we can test execution.
        final Map<Long, EntityActionInfo> entityActionIdToInfoMap = ImmutableMap.of(
                            1234L, new EntityActionInfo(actionSpec1, actionEntityScale1),
                            4321L, new EntityActionInfo(actionSpec2, actionEntityScale2)
                    );
        actionListener.getExecutedActionsCache().putAll(entityActionIdToInfoMap);

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
     *
     * @throws DbException in cases there was a problem terieving entity costs.
     */
    @Test
    public void testProcessNewPendingActions() throws DbException {
        final ActionEntity actionEntityScale1 =
                createActionEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityDelete =
                createActionEntity(2L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Delete delete = Delete.newBuilder()
                        .setTarget(actionEntityDelete)
                        .build();
        final ActionEntity actionEntityScale2 =
                createActionEntity(3L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);
        ActionSpec actionSpec1 =
                createActionSpec(scale1, 1234L, 1234L, System.currentTimeMillis());
        ActionSpec actionSpec2 =
                createActionSpec(4321L, 4321L, ActionInfo.newBuilder().setDelete(delete));
        ActionSpec actionSpec3 =
                createActionSpec(5658L, 5658L, ActionInfo.newBuilder().setScale(scale2));

        FilteredActionResponse filteredResponse1 = createFilteredActionResponse(actionSpec1);
        FilteredActionResponse filteredResponse2 = createFilteredActionResponse(actionSpec2);
        FilteredActionResponse filteredResponse3 = createFilteredActionResponse(actionSpec3);

        doReturn(Arrays.asList(filteredResponse1, filteredResponse2, filteredResponse3))
                        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // create the conditions that happen after actions progress, so that we can test execution.
        final Map<Long, EntityActionInfo> entityActionIdToInfoMap = ImmutableMap.of(
                            1234L, new EntityActionInfo(actionSpec1, actionEntityScale1),
                            4321L, new EntityActionInfo(actionSpec2, actionEntityDelete),
                            5658L, new EntityActionInfo(actionSpec3, actionEntityScale2)
                    );
        actionListener.getExecutedActionsCache().putAll(entityActionIdToInfoMap);

        // Make fake cost response
        final long vmIdScale1 = 1L;
        final long volumeIdDelete = 2L;
        final long volumeIdScale2 = 3L;

        Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
        beforeEntityCostbyOid.put(vmIdScale1, entityCost1);
        beforeEntityCostbyOid.put(volumeIdDelete, entityCost2);
        beforeEntityCostbyOid.put(volumeIdScale2, entityCost3);

        Map<Long, EntityCost> afterEntityCostbyOid = new HashMap<>();
        afterEntityCostbyOid.put(vmIdScale1, entityCost3);
        afterEntityCostbyOid.put(volumeIdDelete, entityCost1);
        afterEntityCostbyOid.put(volumeIdScale2, entityCost2);

        given(projectedEntityCostStore.getProjectedEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));

        // Execute Actions Update.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                        .setActionPlanId(actionPlanId)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                            .setMarket(MarketActionPlanInfo.newBuilder()
                                            .setSourceTopologyInfo(TopologyInfo
                                                .newBuilder()
                                                .setTopologyContextId(realTimeTopologyContextId))))
                        .build());


        assertEquals(3, store.size());
        assertEquals(3, actionListener.getExistingPendingActionsInfoToEntityIdMap().size());

        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        savingsEvents.forEach(se ->
                assertEquals(se.getActionEvent().get().getEventType(),
                        ActionEventType.RECOMMENDATION_ADDED));

        // Make fake cost response
        // First remove action no longer being generated from Market from costMap.
        beforeEntityCostbyOid.remove(vmIdScale1);
        afterEntityCostbyOid.remove(vmIdScale1);

        given(projectedEntityCostStore.getProjectedEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));

        doReturn(Arrays.asList(filteredResponse2, filteredResponse3))
        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // Execute Actions Update again.
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
        assertEquals(2, actionListener.getExistingPendingActionsInfoToEntityIdMap().size());

        savingsEvents = store.removeAllEvents();
        savingsEvents.forEach(se -> assertEquals(se.getActionEvent().get().getEventType(),
                ActionEventType.RECOMMENDATION_REMOVED));
    }

    @NotNull
    private FilteredActionResponse createFilteredActionResponse(ActionSpec actionSpec1) {
        return FilteredActionResponse.newBuilder()
                .setActionChunk(FilteredActionResponse.ActionChunk.newBuilder()
                        .addActions(
                                ActionOrchestratorAction.newBuilder().setActionSpec(actionSpec1)))
                .build();
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
        final long dbsId1 = 401;

        final ActionEntity actionEntityVm1 =
                createActionEntity(101L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scaleVm1 = createScale(actionEntityVm1);
        final ActionEntity actionEntityVm2 =
                createActionEntity(102L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scaleVm2 = createScale(actionEntityVm2);
        final ActionEntity actionEntityDb1 = createActionEntity(201L, EntityType.DATABASE_VALUE);
        final Scale scaleDb1 = createScale(actionEntityDb1);
        final ActionEntity actionEntityVv1 =
                createActionEntity(301L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scaleVv1 = createScale(actionEntityVv1);
        final ActionEntity actionEntityDbs1 =
                createActionEntity(401L, EntityType.DATABASE_SERVER_VALUE);
        final Scale scaleDbs1 = createScale(actionEntityDbs1);
        ActionSpec actionSpecVm1 =
                createActionSpec(scaleVm1, 1234L, 1L, System.currentTimeMillis());
        ActionSpec actionSpecVm2 =
                createActionSpec(5658L, 2L, ActionInfo.newBuilder().setScale(scaleVm2));
        ActionSpec actionSpecDb1 =
                createActionSpec(5654L, 3L, ActionInfo.newBuilder().setScale(scaleDb1));
        ActionSpec actionSpecVv1 =
                createActionSpec(5654L, 4L, ActionInfo.newBuilder().setScale(scaleVv1));
        ActionSpec actionSpecDbs1 =
                createActionSpec(5654L, 5L, ActionInfo.newBuilder().setScale(scaleDbs1));

        final Map<Long, EntityActionInfo> entityIdToActionInfoMap = ImmutableMap.of(
                vmId1, new EntityActionInfo(actionSpecVm1, actionEntityVm1),
                vmId2, new EntityActionInfo(actionSpecVm2, actionEntityVm2),
                dbId1, new EntityActionInfo(actionSpecDb1, actionEntityDb1),
                volumeId1, new EntityActionInfo(actionSpecVv1, actionEntityVv1),
                dbsId1, new EntityActionInfo(actionSpecDbs1, actionEntityDbs1)
        );

        // Make up fake responses.
        makeCostResponse();

        // Call query to fill in entity action info map.
        actionListener.queryEntityCosts(entityIdToActionInfoMap);

        // Verify returned map to confirm costs are setup correctly.
        assertNotNull(entityIdToActionInfoMap);
        assertEquals(5, entityIdToActionInfoMap.size());

        final EntityActionInfo eai1 = entityIdToActionInfoMap.get(vmId1);
        final EntityActionInfo eai2 = entityIdToActionInfoMap.get(vmId2);

        // vmId1 entityCost1 --> entityCost2; vmId2 entityCost3 --> entityCost1
        // Storage costs won't be included for VMs.
        assertNotEquals("vmId1 before cost test", entityCost1.getTotalAmount().getAmount(),
                eai1.getEntityActionCosts().getBeforeCosts(), EPSILON_PRECISION);
        // since entityCost2 involve the category IP, which is not one of the categories we query
        // for, the after cost ends up being 0 here instead of
        // entityCost2.getTotalAmount().getAmount().
        assertEquals("vmId1 after cost test", 0, eai1.getEntityActionCosts().getAfterCosts(),
                EPSILON_PRECISION);

        assertEquals("vmId2 before cost test", entityCost3.getTotalAmount().getAmount(),
                eai2.getEntityActionCosts().getBeforeCosts(), EPSILON_PRECISION);
        // Storage costs won't be included for VMs.
        assertNotEquals("vmId2 after cost test", entityCost1.getTotalAmount().getAmount(),
                eai2.getEntityActionCosts().getAfterCosts(),  EPSILON_PRECISION);
    }

    @NotNull
    private ActionSpec createActionSpec(long recommendationId, long actionId, ActionInfo.Builder builder) {
        return ActionSpec.newBuilder()
                .setRecommendationId(recommendationId)
                .setRecommendation(Action.newBuilder()
                        .setId(actionId)
                        .setDeprecatedImportance(0d)
                        .setExplanation(Explanation.getDefaultInstance())
                        .setInfo(builder.build())
                        .build())
                .setRecommendationTime(System.currentTimeMillis())
                .setActionState(ActionState.READY)
                .build();
    }

    @NotNull
    private ActionSpec createActionSpec(Scale scale, long recommendationId, long actionId,
            long recommendationTime) {
        return ActionSpec.newBuilder()
                .setRecommendationId(recommendationId)
                .setRecommendation(Action.newBuilder()
                        .setId(actionId)
                        .setExplanation(Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0d)
                        .setInfo(ActionInfo.newBuilder().setScale(scale).build())
                        .build())
                .setRecommendationTime(recommendationTime)
                .setActionState(ActionState.READY).build();
    }

    @NotNull
    private Scale createScale(ActionEntity actionEntityVm1) {
        return Scale.newBuilder().setTarget(actionEntityVm1).build();
    }

    @NotNull
    private ActionEntity createActionEntity(long id, int virtualMachineValue) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(virtualMachineValue)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
    }

    /**
     * Tests if action id and source and destination oid fields are set correctly.
     */
    @Test
    public void testGetEntityActionInfo() {
        final int entityType = EntityType.VIRTUAL_VOLUME_VALUE;
        final long sourceTierId = 73705874639937L;
        final long destinationTierId = 73741897536608L;
        final long actionId = 101L;
        ActionEntity actionEntity = createActionEntity(10001L, entityType);

        ActionSpec actionSpec = createActionSpec(Scale.newBuilder()
                .setTarget(actionEntity)
                .addChanges(ChangeProvider.newBuilder().setSource(ActionEntity.newBuilder()
                        .setId(sourceTierId)
                        .setType(EntityType.STORAGE_TIER_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build()).setDestination(ActionEntity.newBuilder()
                        .setId(destinationTierId)
                        .setType(EntityType.STORAGE_TIER_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build()).build())
                .build(), 1001, actionId, 100001L);

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

        given(projectedEntityCostStore.getProjectedEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOid);
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
    private ComponentCost buildComponentCost(final double amount,
            @Nonnull final CostCategory costCategory,
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
        final CurrencyAmount currencyAmount = CurrencyAmount.newBuilder().setAmount(amount)
                .setCurrency(currency).build();
        return currencyAmount;
    }
}
