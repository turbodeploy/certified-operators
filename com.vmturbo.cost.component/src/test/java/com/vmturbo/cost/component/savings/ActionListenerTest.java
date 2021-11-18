package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
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
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.InMemoryEntityCostStore;
import com.vmturbo.cost.component.savings.ActionListener.EntityActionInfo;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;
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
    private final SettingServiceMole settingServiceRpc = Mockito.spy(new SettingServiceMole());
    private SettingServiceBlockingStub settingsService;
    private EntityCostStore entityCostStore = mock(EntityCostStore.class);
    private InMemoryEntityCostStore projectedEntityCostStore = mock(InMemoryEntityCostStore.class);
    private static final double EPSILON_PRECISION = 0.0000001d;

    private static final long ASSOCIATED_SERVICE_ID = 4L;
    private static final int ASSOCIATED_ENTITY_TYPE_VM = 10;
    private static final int ASSOCIATED_ENTITY_TYPE_STORAGE = 60;

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
    private final ComponentCost componentCost3 = buildComponentCost(currencyAmount1.getAmount(),
                                                                    CostCategory.STORAGE,
                                                                    CostSource.ON_DEMAND_RATE);
    private final ComponentCost componentCost4 = buildComponentCost(currencyAmount2.getAmount(),
                                                                    CostCategory.STORAGE,
                                                                    CostSource.ON_DEMAND_RATE);

    private final EntityCost entityCost = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                          ImmutableSet.of(componentCost),
                                                          ASSOCIATED_ENTITY_TYPE_VM);
    private final EntityCost entityCost1 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost, componentCost1),
                                                           ASSOCIATED_ENTITY_TYPE_VM);
    private final EntityCost entityCost2 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost1),
                                                           ASSOCIATED_ENTITY_TYPE_VM);
    private final EntityCost entityCost3 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost2),
                                                           ASSOCIATED_ENTITY_TYPE_VM);
    private final EntityCost entityCost4 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost3, componentCost1),
                                                           ASSOCIATED_ENTITY_TYPE_STORAGE);
    private final EntityCost entityCost5 = buildEntityCost(ASSOCIATED_SERVICE_ID,
                                                           ImmutableSet.of(componentCost4),
                                                           ASSOCIATED_ENTITY_TYPE_STORAGE);

    private final Clock clock = Clock.systemUTC();

    private EntitySavingsStore<DSLContext> entitySavingsStore = mock(EntitySavingsStore.class);

    private EntityStateStore<DSLContext> entityStateStore = mock(EntityStateStore.class);

    /**
     * Test gRPC server to mock out actions service gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceRpc,
            settingServiceRpc);

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
        settingsService = SettingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        EntitySavingsRetentionConfig config = new EntitySavingsRetentionConfig(settingsService, 1L);
        // Initialize ActionListener with a one hour action lifetime.
        actionListener = new ActionListener(store, actionsService,
                                            entityCostStore, projectedEntityCostStore,
                                            realTimeTopologyContextId,
                EntitySavingsConfig.getSupportedEntityTypes(),
                EntitySavingsConfig.getSupportedActionTypes(), config,
                entitySavingsStore, entityStateStore, clock);

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
        setActionExpiration(1f);
    }

    /**
     * Set the action expiration for both actions and volume deletes to the specified value.
     *
     * @param valueInMonths action duration in months.
     */
    private void setActionExpiration(float valueInMonths) {
        when(settingServiceRpc.getGlobalSetting(any(GetSingleGlobalSettingRequest.class)))
                .thenReturn(GetGlobalSettingResponse.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setNumericSettingValue(NumericSettingValue.newBuilder()
                                        .setValue(valueInMonths)))
                        .build());
    }

    /**
     * Test cleanup.
     */
    @After
    public void cleanup() {
        grpcTestServer.getChannel().shutdownNow();
    }

    /**
     * Test processing of successfully executed actions.
     */
    @Test
    public void testProcessActionSuccess() {
        final ActionEntity actionEntityScale1 =
                createCloudActionEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale2 =
                createCloudActionEntity(3L, EntityType.VIRTUAL_VOLUME_VALUE);
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

        // The first action has an expiration of one month.
        setActionExpiration(1f);
        actionListener.onActionSuccess(actionSuccess1);
        assertEquals(1, store.size());
        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        SavingsEvent savingsEvent1 = savingsEvents.get(0);
        validateActionEvent("savingsEvent1", savingsEvent1, 1L);

        // The next action has an expiration of two months.
        setActionExpiration(2f);

        // If a succeeded message were to be received more than once
        // an additional entry should not be created.
        actionListener.onActionSuccess(actionSuccess2);
        assertEquals(1, store.size());

        actionListener.onActionSuccess(actionSuccess2);
        assertEquals(1, store.size());
        savingsEvents = store.removeAllEvents();
        SavingsEvent savingsEvent2 = savingsEvents.get(0);
        validateActionEvent("savingsEvent2", savingsEvent2, 2L);

        // Put the actions back
        store.addEvent(savingsEvent1);
        store.addEvent(savingsEvent2);
        store.removeAllEvents().stream().forEach(se -> {
            assertEquals(se.getActionEvent().get().getEventType(),
                         ActionEventType.SCALE_EXECUTION_SUCCESS);
        });

        // Verify that only actions for entities in TopologyDTOUtil.WORKLOAD_TYPES and
        // VIRTUAL_VOLUME are processed.
        final ActionEntity actionEntityScale3 =
                        createCloudActionEntity(5L, EntityType.DATABASE_VALUE);
        final Scale scale3 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale4 =
                        createCloudActionEntity(7L, EntityType.DATABASE_SERVER_VALUE);
        final Scale scale4 = createScale(actionEntityScale4);
        final ActionEntity actionEntityScale5 =
                        createCloudActionEntity(9L, EntityType.PHYSICAL_MACHINE_VALUE);
        final Scale scale5 = createScale(actionEntityScale5);
        final ActionSpec actionSpec3 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(2222L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(2222L)
                                        .setDeprecatedImportance(1.0)
                                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                        .setInfo(ActionInfo.newBuilder()
                                                        .setScale(scale3)
                                                        .build())
                                        .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        final ActionSpec actionSpec4 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(9999L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(9999L)
                            .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                            .setDeprecatedImportance(1.0)
                            .setInfo(ActionInfo.newBuilder()
                                            .setScale(scale4)
                                            .build())
                            .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        final ActionSpec actionSpec5 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(7777L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(7777L)
                            .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                            .setDeprecatedImportance(1.0)
                            .setInfo(ActionInfo.newBuilder()
                                            .setScale(scale5)
                                            .build())
                            .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        final ActionSuccess actionSuccess3 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId1)
                        .setActionSpec(actionSpec1)
                        .build();
        final ActionSuccess actionSuccess4 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId2)
                        .setActionSpec(actionSpec2)
                        .build();
        final ActionSuccess actionSuccess5 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(succeededActionId2)
                        .setActionSpec(actionSpec2)
                        .build();

        actionListener.onActionSuccess(actionSuccess3);
        assertEquals(1, store.size());
        actionListener.onActionSuccess(actionSuccess4);
        assertEquals(2, store.size());
        actionListener.onActionSuccess(actionSuccess5);
        assertEquals(2, store.size());

        // Only actions with EnvironmentType set to Cloud should be processed.
        store.removeAllEvents();
        final ActionEntity actionEntityScale6 =
                                              createOnPremActionEntity(7070L,
                                                                      EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale6 = createScale(actionEntityScale6);
        final ActionSpec actionSpec6 = createActionSpec(7070L, 7070L,
                                                        ActionInfo.newBuilder().setScale(scale6),
                                                        ActionState.SUCCEEDED);
        final ActionSuccess actionSuccess6 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(7070L)
                        .setActionSpec(actionSpec6)
                        .build();
        actionListener.onActionSuccess(actionSuccess6);
        assertEquals(0, store.size());

        // Only Scale / Delete action executions are supported.
        final ActionEntity actionEntityScale7 =
                                              createOnPremActionEntity(0770L,
                                                                       EntityType.VIRTUAL_MACHINE_VALUE);
        final Move move = createMove(actionEntityScale7);
        final ActionSpec actionSpec7 = createActionSpec(0770L, 0770L,
                                                        ActionInfo.newBuilder().setMove(move),
                                                        ActionState.SUCCEEDED);
        final ActionSuccess actionSuccess7 = ActionSuccess.newBuilder()
                        .setSuccessDescription(succeededActionMsg)
                        .setActionId(0770L)
                        .setActionSpec(actionSpec7)
                        .build();
        actionListener.onActionSuccess(actionSuccess7);
        assertEquals(0, store.size());
    }

    private void validateActionEvent(String message, SavingsEvent savingsEvent,
            long expectedExpiration) {
        assertTrue(message, savingsEvent.hasActionEvent());
        Optional<ActionEvent> optActionEvent = savingsEvent.getActionEvent();
        assertTrue(message, optActionEvent.isPresent());
        ActionEvent actionEvent = optActionEvent.get();
        assertTrue(message, savingsEvent.getExpirationTime().isPresent());

        Long expirationTime = savingsEvent.getExpirationTime().get(); // milliseconds
        Assert.assertEquals("Validating expiration",
                TimeUnit.HOURS.toMillis(expectedExpiration * 730), (long)expirationTime);
    }

    /**
     * Test processing of action updates.
     *
     * @throws DbException in cases there was a problem retrieving entity costs.
     * @throws EntitySavingsException On a savings related exception.
     */
    @Test
    public void testOnActionsUpdated() throws DbException, EntitySavingsException {
        final ActionEntity actionEntityScale1 =
                createCloudActionEntity(1L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityDelete =
                createCloudActionEntity(2L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Delete delete = Delete.newBuilder()
                        .setTarget(actionEntityDelete)
                        .build();
        final ActionEntity actionEntityScale2 =
                createCloudActionEntity(3L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);
        final ActionEntity actionEntityAllocate =
                        createCloudActionEntity(4L, EntityType.VIRTUAL_MACHINE_VALUE);
        final ActionEntity actionEntityWorkloadTier =
                                                    createCloudActionEntity(5L,
                                                       EntityType.COMPUTE_TIER_VALUE);
        // Actions could be in different states related to pre and post execution.
        // We create recommendation events for all states.
        final Allocate allocate = createAllocate(actionEntityAllocate, actionEntityWorkloadTier);
        ActionSpec actionSpec1 =
                createActionSpec(scale1, 1234L, 1234L, System.currentTimeMillis(), ActionState.READY);
        ActionSpec actionSpec2 =
                createActionSpec(4321L, 4321L, ActionInfo.newBuilder().setDelete(delete), null);
        ActionSpec actionSpec3 =
                createActionSpec(5658L, 5658L, ActionInfo.newBuilder().setScale(scale2), ActionState.IN_PROGRESS);
        ActionSpec actionSpec4 =
                createActionSpec(5154L, 5154L, ActionInfo.newBuilder().setAllocate(allocate), ActionState.POST_IN_PROGRESS);
        // create "duplicate" action for entity, as can be seen in the case of multi-attach volumes.
        ActionSpec actionSpec1Duplicate =
                        createActionSpec(scale1, 1234L, 8417L, System.currentTimeMillis(), ActionState.ACCEPTED);
        // only actionId's are likely to be different.

        FilteredActionResponse filteredResponse1 = createFilteredActionResponse(actionSpec1);
        FilteredActionResponse filteredResponse2 = createFilteredActionResponse(actionSpec2);
        FilteredActionResponse filteredResponse3 = createFilteredActionResponse(actionSpec3);
        FilteredActionResponse filteredResponse4 = createFilteredActionResponse(actionSpec4);
        FilteredActionResponse filteredResponse5 = createFilteredActionResponse(actionSpec1Duplicate);

        doReturn(Arrays.asList(filteredResponse1, filteredResponse2, filteredResponse3, filteredResponse4, filteredResponse5))
                        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // Make fake cost response
        final long vmIdScale1 = 1L;
        final long volumeIdDelete = 2L;
        final long volumeIdScale2 = 3L;
        final long vmIdAllocate = 4L;

        Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
        beforeEntityCostbyOid.put(vmIdScale1, entityCost1);
        beforeEntityCostbyOid.put(volumeIdDelete, entityCost4);
        beforeEntityCostbyOid.put(volumeIdScale2, entityCost3);
        beforeEntityCostbyOid.put(vmIdAllocate, entityCost1);

        Map<Long, EntityCost> afterEntityCostbyOid = new HashMap<>();
        afterEntityCostbyOid.put(vmIdScale1, entityCost3);
        afterEntityCostbyOid.put(volumeIdDelete, entityCost4);
        afterEntityCostbyOid.put(volumeIdScale2, entityCost2);
        afterEntityCostbyOid.put(vmIdAllocate, entityCost3);

        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());

        // Execute Actions Update.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                        .setActionPlanId(actionPlanId)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                            .setMarket(MarketActionPlanInfo.newBuilder()
                                            .setSourceTopologyInfo(TopologyInfo
                                                .newBuilder()
                                                .setTopologyContextId(realTimeTopologyContextId))))
                        .build());


        assertEquals(4, store.size());
        // Assert that 4 actions , one per entity are added, not the duplicate.
        assertEquals(4, actionListener.getExistingActionsInfoToEntityIdMap().size());

        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        savingsEvents.forEach(se ->
                assertEquals(se.getActionEvent().get().getEventType(),
                        ActionEventType.RECOMMENDATION_ADDED));

        // Make fake cost response
        // First remove action no longer being generated from Market from costMap.
        beforeEntityCostbyOid.remove(vmIdScale1);
        afterEntityCostbyOid.remove(vmIdScale1);
        beforeEntityCostbyOid.remove(vmIdAllocate);
        afterEntityCostbyOid.remove(vmIdAllocate);

        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
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
        // After removing all events from store in the last test, one new RECOMMENDATION_REMOVED
        // event should have been added in this test.
        assertEquals(2, store.size());
        assertEquals(2, actionListener.getExistingActionsInfoToEntityIdMap().size());

        savingsEvents = store.removeAllEvents();
        savingsEvents.stream().forEach(se -> {
            assertEquals(se.getActionEvent().get().getEventType(),
                         ActionEventType.RECOMMENDATION_REMOVED);
        });

        // Test for changing costs of an existing action.
        // In this case the old action will be replaced and a new one added.
        // Note that since this is replaced action, and we don't generate removal events for those,
        // the store size will be 1.
        afterEntityCostbyOid.put(volumeIdDelete, entityCost5);  //used to be entityCost4
        // Make fake cost response
        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class))).willReturn(afterEntityCostbyOid);
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

        assertEquals(1, store.size());
        assertEquals(2, actionListener.getExistingActionsInfoToEntityIdMap().size());

        savingsEvents = store.removeAllEvents();
        int numAdded = 0;
        int numRemoved = 0;
        for (SavingsEvent se : savingsEvents) {
            if (se.getActionEvent().get().getEventType()
                            == ActionEventType.RECOMMENDATION_REMOVED) {
                numRemoved++;
            } else if (se.getActionEvent().get().getEventType()
                            == ActionEventType.RECOMMENDATION_ADDED) {
                numAdded++;
            }
        }
        assertEquals(1, numAdded);
        assertEquals(0, numRemoved);

        // Setup
        // Remove entities with actions before restart for this test.
        Answer<Stream> entitiesWithActionBeforeRestart = invocation -> Stream.empty();
        when(entityStateStore.getAllEntityStates()).thenAnswer(entitiesWithActionBeforeRestart);
        LastRollupTimes lastRollupTimesZero = new LastRollupTimes(0, 0, 0, 0);
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimesZero);
        beforeEntityCostbyOid.clear();
        afterEntityCostbyOid.clear();
        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
        .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
        beforeEntityCostbyOid));

        // Verify that actions without recommendation are not processed.
        store.removeAllEvents();
        assertEquals(0, store.size());
        final ActionSpec actionSpec6 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(777L)
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        // This filteredActionRequest is called by  onActionsUpdate directly.
        FilteredActionResponse filteredResponse6 = createFilteredActionResponse(actionSpec6);
        doReturn(Arrays.asList(filteredResponse6))
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
        // Since this action didn't have a recommendation in the actionSpec, it should
        // not get processed.  The 2 events added are recovered removal actions.
        assertEquals(2, store.size());

        // Setup
        // Remove entities with actions before restart for this test.
        when(entityStateStore.getAllEntityStates()).thenAnswer(entitiesWithActionBeforeRestart);
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimesZero);
        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
        .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
        beforeEntityCostbyOid));
        beforeEntityCostbyOid.clear();
        afterEntityCostbyOid.clear();
        // Verify that actions without a supported action type are not processed.
        store.removeAllEvents();
        assertEquals(0, store.size());
        final ActionSpec actionSpec7 = ActionDTO.ActionSpec.newBuilder()
                        .setRecommendationId(77L)
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(77L)
                                           .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                                           .setDeprecatedImportance(1.0)
                                           .setInfo(ActionInfo.newBuilder()
                                                           .build())
                                           .build())
                        .setActionState(com.vmturbo.common.protobuf.action.ActionDTO.ActionState.READY)
                        .build();
        FilteredActionResponse filteredResponse7 = createFilteredActionResponse(actionSpec7);
        doReturn(Arrays.asList(filteredResponse7))
        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // Make fake cost response
        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class))).willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));

        // Execute Actions Update again.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanId(actionPlanId)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo
                                        .newBuilder()
                                        .setTopologyContextId(realTimeTopologyContextId))))
                .build());
        // Since this action didn't have an actionType in actionSpec, it should not be processed.
        assertEquals(0, store.size());
    }

    /**
     * The before and after action costs of an action can be unreliable when the action is in
     * progress. So if we already have an RECOMMENDATION_ADDED event for an action, we won't
     * create another RECOMMENDATION_ADDED event when the action status becomes IN_PROGRESS, even
     * if the cost has changed.
     *
     * <p>To support the automation use case, we will still need to create a RECOMMENDATION_ADDED event
     * if the action status is IN_PROGRESS and we don't already have a RECOMMENDATION_ADDED event
     * for the action.
     *
     * @throws Exception any exception
     */
    @Test
    public void testPriceChangeDuringActionExecution() throws Exception {
        final long vmIdScale1 = 1L;
        final long volumeIdScale2 = 3L;

        final ActionEntity actionEntityScale1 =
                createCloudActionEntity(vmIdScale1, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale2 =
                createCloudActionEntity(volumeIdScale2, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);

        ActionSpec actionSpec1 =
                createActionSpec(scale1, 1234L, 1234L, System.currentTimeMillis(), ActionState.READY);
        ActionSpec actionSpec2 =
                createActionSpec(scale2, 3333L, 4444, System.currentTimeMillis(), ActionState.IN_PROGRESS);

        FilteredActionResponse filteredResponse1 = createFilteredActionResponse(actionSpec1);
        FilteredActionResponse filteredResponse2 = createFilteredActionResponse(actionSpec2);

        doReturn(Arrays.asList(filteredResponse1, filteredResponse2))
                .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // Make fake cost response
        Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
        beforeEntityCostbyOid.put(vmIdScale1, entityCost1);
        beforeEntityCostbyOid.put(volumeIdScale2, entityCost3);

        Map<Long, EntityCost> afterEntityCostbyOid = new HashMap<>();
        afterEntityCostbyOid.put(vmIdScale1, entityCost3);
        afterEntityCostbyOid.put(volumeIdScale2, entityCost2);

        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOid));
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());

        // Execute Actions Update.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanId(actionPlanId)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo
                                        .newBuilder()
                                        .setTopologyContextId(realTimeTopologyContextId))))
                .build());

        // In this invocation of onActionsUpdated, we expect 2 events added to the event journal,
        // one for each action.
        // The first action is in READY state, so an event should be added.
        // The second action is in IN_PROGRESS state, but it is the first time we see this action,
        // so an event will be created for it. It is the automated action use case.
        assertEquals(2, store.size());
        assertEquals(2, actionListener.getExistingActionsInfoToEntityIdMap().size());


        Map<Long, EntityCost> beforeEntityCostbyOidDuringExecution = new HashMap<>();
        beforeEntityCostbyOidDuringExecution.put(vmIdScale1, entityCost1);
        Map<Long, EntityCost> afterEntityCostbyOidDuringExecution = new HashMap<>();
        afterEntityCostbyOidDuringExecution.put(vmIdScale1, entityCost1);

        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
                beforeEntityCostbyOidDuringExecution));
        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
                .willReturn(afterEntityCostbyOidDuringExecution);

        ActionSpec actionSpec1InProgress =
                createActionSpec(scale1, 1234L, 1234L, System.currentTimeMillis(), ActionState.IN_PROGRESS);
        FilteredActionResponse filteredResponse1b = createFilteredActionResponse(actionSpec1InProgress);
        doReturn(Arrays.asList(filteredResponse1b, filteredResponse2))
                .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        // Execute Actions Update.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanId(actionPlanId)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo
                                        .newBuilder()
                                        .setTopologyContextId(realTimeTopologyContextId))))
                .build());

        // In the second invocation of the onActionsUpdated method, the state of the first action
        // is changed to IN_PROGRESS, and the cost of the action is change. In this case, we don't
        // want to create a new event for it. So we expect the number of event in the journal to
        // remain at 2.
        assertEquals(2, store.size());
        assertEquals(2, actionListener.getExistingActionsInfoToEntityIdMap().size());
    }

    @Nonnull
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
                createCloudActionEntity(101L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scaleVm1 = createScale(actionEntityVm1);
        final ActionEntity actionEntityVm2 =
                createCloudActionEntity(102L, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scaleVm2 = createScale(actionEntityVm2);
        final ActionEntity actionEntityDb1 = createCloudActionEntity(201L, EntityType.DATABASE_VALUE);
        final Scale scaleDb1 = createScale(actionEntityDb1);
        final ActionEntity actionEntityVv1 =
                createCloudActionEntity(301L, EntityType.VIRTUAL_VOLUME_VALUE);
        final Scale scaleVv1 = createScale(actionEntityVv1);
        final ActionEntity actionEntityDbs1 =
                createCloudActionEntity(401L, EntityType.DATABASE_SERVER_VALUE);
        final Scale scaleDbs1 = createScale(actionEntityDbs1);
        ActionSpec actionSpecVm1 =
                createActionSpec(scaleVm1, 1234L, 1L, System.currentTimeMillis(), null);
        ActionSpec actionSpecVm2 =
                createActionSpec(5658L, 2L, ActionInfo.newBuilder().setScale(scaleVm2), null);
        ActionSpec actionSpecDb1 =
                createActionSpec(5654L, 3L, ActionInfo.newBuilder().setScale(scaleDb1), null);
        ActionSpec actionSpecVv1 =
                createActionSpec(5654L, 4L, ActionInfo.newBuilder().setScale(scaleVv1), null);
        ActionSpec actionSpecDbs1 =
                createActionSpec(5654L, 5L, ActionInfo.newBuilder().setScale(scaleDbs1), null);

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

    @Nonnull
    private ActionSpec createActionSpec(long recommendationId, long actionId, ActionInfo.Builder builder,
                                        ActionState actionState) {
        return ActionSpec.newBuilder()
                .setRecommendationId(recommendationId)
                .setRecommendation(Action.newBuilder()
                        .setId(actionId)
                        .setDeprecatedImportance(0d)
                        .setExplanation(Explanation.getDefaultInstance())
                        .setInfo(builder.build())
                        .build())
                .setRecommendationTime(System.currentTimeMillis())
                .setActionState(actionState == null ? ActionState.READY : actionState)
                .build();
    }

    @Nonnull
    private ActionSpec createActionSpec(Scale scale, long recommendationId, long actionId,
            long recommendationTime, ActionState actionState) {
        return ActionSpec.newBuilder()
                .setRecommendationId(recommendationId)
                .setRecommendation(Action.newBuilder()
                        .setId(actionId)
                        .setExplanation(Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0d)
                        .setInfo(ActionInfo.newBuilder().setScale(scale).build())
                        .build())
                .setRecommendationTime(recommendationTime)
                .setActionState(actionState == null ? ActionState.READY : actionState).build();
    }

    @Nonnull
    private Scale createScale(@Nonnull final ActionEntity actionEntity) {
        return Scale.newBuilder().setTarget(actionEntity).build();
    }

    @Nonnull
    private Allocate createAllocate(@Nonnull final ActionEntity actionEntity,
                              @Nonnull final ActionEntity actionEntityWorkloadTier) {
        return Allocate.newBuilder()
                        .setTarget(actionEntity)
                        .setWorkloadTier(actionEntityWorkloadTier)
                        .build();
    }

    @Nonnull
    private Move createMove(@Nonnull final ActionEntity actionEntity) {
        return Move.newBuilder().setTarget(actionEntity).build();
    }

    @Nonnull
    private ActionEntity createCloudActionEntity(long id, int entityTypeValue) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(entityTypeValue)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
    }

    @Nonnull
    private ActionEntity createOnPremActionEntity(long id, int entityTypeValue) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(entityTypeValue)
                .setEnvironmentType(EnvironmentType.ON_PREM)
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
        ActionEntity actionEntity = createCloudActionEntity(10001L, entityType);

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
                .build(), 1001, actionId, 100001L, null);

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

        given(projectedEntityCostStore.getEntityCosts(any(EntityCostFilter.class)))
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

    /**
     * 2 successful action executions happened when cost pod was down. When running logic to
     * recover execution success events, make sure the 2 events are added to the journal.
     *
     * @throws Exception any exception
     */
    @Test
    public void testRecoverMissedExecutionSuccessEvents() throws Exception {
        Long entity1Oid = 1L;
        Long entity2Oid = 2L;
        final ActionEntity actionEntityScale1 =
                createCloudActionEntity(entity1Oid, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale2 =
                createCloudActionEntity(entity2Oid, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);
        ActionSpec actionSucceeded1 =
                createActionSpec(scale1, 4321L, 4321L, 1, ActionState.SUCCEEDED);
        ActionSpec actionSucceeded2 =
                createActionSpec(scale2, 5658L, 5658L, 2, ActionState.SUCCEEDED);

        FilteredActionResponse filteredResponse1 = createFilteredActionResponse(actionSucceeded1);
        FilteredActionResponse filteredResponse2 = createFilteredActionResponse(actionSucceeded2);

        doReturn(Arrays.asList(filteredResponse1, filteredResponse2))
                .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        when(entityStateStore.getEntityStates(ImmutableSet.of(entity1Oid)))
                .thenReturn(ImmutableMap.of(entity1Oid, createEntityState(entity1Oid)));
        when(entityStateStore.getEntityStates(ImmutableSet.of(entity2Oid)))
                .thenReturn(ImmutableMap.of(entity2Oid, createEntityState(entity2Oid)));

        long startTime = 1627336047000L;
        long endTime = 1627339647000L;
        actionListener.recoverMissedExecutionSuccessEvents(startTime, endTime);
        assertEquals(2, store.size());
    }

    /**
     * 2 successful action executions happened when cost pod was down. When running logic to
     * recover execution success events, test that without a last rollup time for Entity Savings Store,
     *  prior to the cost pod crashing, no action events will be recovered.
     *
     * @throws Exception any exception
     */
    @Test
    public void testRecoverActionsNoLastRollupTime() throws Exception {
        Long entity1Oid = 1L;
        Long entity2Oid = 2L;
        final ActionEntity actionEntityScale1 =
                createCloudActionEntity(entity1Oid, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale1 = createScale(actionEntityScale1);
        final ActionEntity actionEntityScale2 =
                createCloudActionEntity(entity2Oid, EntityType.VIRTUAL_MACHINE_VALUE);
        final Scale scale2 = createScale(actionEntityScale2);
        ActionSpec actionSucceeded1 =
                createActionSpec(scale1, 4321L, 4321L, 1, ActionState.SUCCEEDED);
        ActionSpec actionSucceeded2 =
                createActionSpec(scale2, 5658L, 5658L, 2, ActionState.SUCCEEDED);

        FilteredActionResponse filteredResponse1 = createFilteredActionResponse(actionSucceeded1);
        FilteredActionResponse filteredResponse2 = createFilteredActionResponse(actionSucceeded2);

        AtomicReference<String> cursor = new AtomicReference<>("0");
        Long now = clock.millis();
        // This filteredActionRequest is called by recoverActions
        FilteredActionRequest actionRequest = actionListener
                        .filteredActionRequest(realTimeTopologyContextId,
                                               cursor, 0L, now,
                                               Collections.singletonList(ActionState.SUCCEEDED));
        doReturn(Arrays.asList(filteredResponse1, filteredResponse2))
                        .when(actionsServiceRpc).getAllActions(actionRequest);

        when(entityStateStore.getEntityStates(ImmutableSet.of(entity1Oid)))
                .thenReturn(ImmutableMap.of(entity1Oid, createEntityState(entity1Oid)));
        when(entityStateStore.getEntityStates(ImmutableSet.of(entity2Oid)))
                .thenReturn(ImmutableMap.of(entity2Oid, createEntityState(entity2Oid)));

        LastRollupTimes lastRollupTimesZero = new LastRollupTimes(0, 0, 0, 0);
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimesZero);
        // Execute Actions Update again.
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                        .setActionPlanId(actionPlanId)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                                        .setMarket(MarketActionPlanInfo.newBuilder()
                                                        .setSourceTopologyInfo(TopologyInfo
                                                                        .newBuilder()
                                                                        .setTopologyContextId(realTimeTopologyContextId))))
                        .build());
        // recoverActions() will not be called, hence no action events will be recovered.
        assertEquals(0, store.size());
    }

    /**
     * Test case: There are 6 pending actions before the cost pod was restarted. There are 4 pending
     * actions after cost pod comes back up. 2 RECOMMENDATION_REMOVED actions are added to the
     * journal.
     *
     * @throws Exception any exception.
     */
    @Test
    public void testRecoverMissedRecommendationRemovedEvents() throws Exception {
        Set<Long> entitiesWithActionAfterRestart = ImmutableSet.of(1L, 2L, 3L, 4L);
        long currentTimestamp = System.currentTimeMillis();

        Answer<Stream> entitiesWithActionBeforeRestart = invocation -> Stream.of(
                createEntityState(1L), createEntityState(2L), createEntityState(3L),
                createEntityState(4L)
        );
        when(entityStateStore.getAllEntityStates()).thenAnswer(entitiesWithActionBeforeRestart);

        actionListener.recoverMissedRecommendationRemovedEvents(entitiesWithActionAfterRestart,
                currentTimestamp);
        assertEquals(0, store.size());

        Set<EntityState> entityStatesBeforeRestart = ImmutableSet.of(createEntityState(1L),
                createEntityState(2L), createEntityState(3L),
                createEntityState(4L), createEntityState(5L), createEntityState(6L));

        entitiesWithActionBeforeRestart = invocation -> entityStatesBeforeRestart.stream();
        when(entityStateStore.getAllEntityStates()).thenAnswer(entitiesWithActionBeforeRestart);

        Map<Long, EntityState> stateMap = entityStatesBeforeRestart.stream()
                .filter(s -> !entitiesWithActionAfterRestart.contains(s.getEntityId()))
                .collect(Collectors.toMap(EntityState::getEntityId, Function.identity()));
        when(entityStateStore.getEntityStates(anySet())).thenReturn(stateMap);

        actionListener.recoverMissedRecommendationRemovedEvents(entitiesWithActionAfterRestart,
                currentTimestamp);

        assertEquals(2, store.size());
    }

    /**
     * Test get next period start time.
     */
    @Test
    public void testGetNextPeriodStartTime() {
        LastRollupTimes lastRollupTimesNonZero = new LastRollupTimes(0, 12L, 0, 0);
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimesNonZero);
        assertEquals(actionListener.getNextPeriodStartTime(), lastRollupTimesNonZero.getLastTimeByHour() + TimeUnit.HOURS.toMillis(1));

        LastRollupTimes lastRollupTimesZero = new LastRollupTimes(0, 0, 0, 0);
        when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimesZero);
        assertEquals(actionListener.getNextPeriodStartTime(), 0);
    }

    private EntityState createEntityState(long entityOid) {
        EntityState state = new EntityState(entityOid, SavingsUtil.EMPTY_PRICE_CHANGE);
        state.setCurrentRecommendation(new EntityPriceChange.Builder()
                .sourceCost(0)
                .destinationCost(0)
                .sourceOid(0L)
                .destinationOid(0L)
                .active(true).build());
        return state;
    }
}
