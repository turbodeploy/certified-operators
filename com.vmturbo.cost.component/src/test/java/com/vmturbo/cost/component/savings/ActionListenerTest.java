package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

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

    /**
     * Test gRPC server to mock out actions service gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceRpc);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        store = new InMemoryEntityEventsJournal();
        actionsService = ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        // Test gRPC server to mock out actions service gRPC dependencies.
        GrpcTestServer costGrpcServer = GrpcTestServer.newServer(costServiceRpc);
        costGrpcServer.start();
        costService = CostServiceGrpc.newBlockingStub(costGrpcServer.getChannel());
        actionListener = new ActionListener(store, actionsService, costService,
                                            realTimeTopologyContextId);

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
        Mockito.doReturn(GetTierPriceForEntitiesResponse.newBuilder()
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
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(1L)
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
                        .setRecommendation(ActionDTO.Action.newBuilder().setId(3L)
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
                        .setRecommendationId(5658L)
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

        Mockito.doReturn(Arrays.asList(filteredResponse1, filteredResponse2, filteredResponse3))
                        .when(actionsServiceRpc).getAllActions(any(FilteredActionRequest.class));

        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                        .setActionPlanId(actionPlanId)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                            .setMarket(MarketActionPlanInfo.newBuilder()
                                            .setSourceTopologyInfo(TopologyInfo
                                                .newBuilder()
                                                .setTopologyContextId(realTimeTopologyContextId))))
                        .build());

        // We're only monitoring savings for VM Scale actions at the moment.
        assertEquals(1, store.size());
        assertEquals(1, actionListener.getEntityActionStateMap().size());
    }

    /**
     * Helper method to create an ActionEntity.
     *
     * @param id The Action ID.
     * @param type The target EntityType.
     * @return The ActionEntity.
     */
    public static ActionEntity createActionEntity(long id, EntityType type) {
        return ActionEntity.newBuilder()
                        .setId(id)
                        .setType(type.getNumber())
                        .build();
    }
}
