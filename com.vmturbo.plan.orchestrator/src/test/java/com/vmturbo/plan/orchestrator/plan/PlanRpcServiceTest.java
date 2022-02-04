package com.vmturbo.plan.orchestrator.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyRIAnalysisServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.EntityOids;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

public class PlanRpcServiceTest {

    private final AnalysisServiceMole testAnalysisRpcService = spy(new AnalysisServiceMole());
    private final BuyRIAnalysisServiceMole testBuyRiRpcService = spy(new BuyRIAnalysisServiceMole());
    private final GroupServiceMole testGroupRpcService = spy(new GroupServiceMole());
    private final RepositoryServiceMole testRepositoryRpcService = spy(new RepositoryServiceMole());
    private final SupplyChainServiceMole testSupplyChainRpcService = spy(new SupplyChainServiceMole());
    private final long topologyId = 2222;
    // The  realtime topology context Id.
    private static final Long realtimeTopologyContextId = 777777L;

    //Runs tasks on same thread that's invoking execute/submit
    private ExecutorService sameThreadExecutor = MoreExecutors.newDirectExecutorService();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testAnalysisRpcService,
            testBuyRiRpcService, testGroupRpcService, testRepositoryRpcService,
            testSupplyChainRpcService);

    private PlanRpcService planService;
    @SuppressWarnings("unchecked")
    private StreamObserver<PlanInstance> response = mock(StreamObserver.class);

    private PlanDao planDaoMock = mock(PlanDao.class);

    @Before
    public void setup() {
        planService = new PlanRpcService(planDaoMock,
            mock(ApplicationContext.class),
            AnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            mock(PlanNotificationSender.class),
            sameThreadExecutor,
            mock(UserSessionContext.class),
            BuyRIAnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            mock(RepositoryClient.class),
            1, TimeUnit.SECONDS, realtimeTopologyContextId );
    }

    /**
     * Tests workflow of runQueuedPlan for run of OCP option 1: M2 + RI Buy.
     */
    @Test
    public void testRunQueuedPlanWithM2AndBuyRi() {
        PlanInstance planInstance = createOptimizePlanWithM2AndBuyRi();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
        // TODO: verify(testAnalysisRpcService, times(1)).startAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithM2AndBuyRi() {
        return PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build()))
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build())).build();
    }

    /**
     * Tests workflow of runQueuedPlan for run of OCP option 2: M2 with no RI Buy.
     */
    @Test
    public void testRunQueuedPlanWithM2AndNoBuyRi() {
        PlanInstance planInstance = createOptimizePlanWithM2AndNoBuyRi();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(0)).startBuyRIAnalysis(any());
        // TODO: verify(testAnalysisRpcService, times(1)).startAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithM2AndNoBuyRi() {
        return PlanInstance.newBuilder().setPlanId(2L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build())).build();
    }

    /**
     * Tests workflow of runQueuedPlan for run of BUY_RI_PLAN.
     */
    @Test
    public void testRunQueuedPlanWithBuyRi() {
        planService.runQueuedPlan(createBuyRIPlan(), response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
        verify(testAnalysisRpcService, times(0)).startAnalysis(any());
    }

    private PlanInstance createBuyRIPlan() {
        return PlanInstance.newBuilder().setPlanId(9L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                    .setType(StringConstants.BUY_RI_PLAN)
                    .addChanges(ScenarioChange.newBuilder()
                        .setRiSetting(RISetting.newBuilder())))).build();
    }

    /**
     * Tests workflow of runQueuedPlan for run of plan option 3: RI Buy only using allocation demand.
     */
    @Test
    public void testRunQueuedPlanWithOCPBuyRiOnly() {
        PlanInstance planInstance = createOptimizePlanWithBuyRiOnly();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithBuyRiOnly() {
        return PlanInstance.newBuilder().setPlanId(3L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .addChanges(getResizeScenarioChanges(StringConstants.DISABLED))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build())))).build();
    }

    private ScenarioChange getResizeScenarioChanges(String actionSetting) {
        final EnumSettingValue settingValue = EnumSettingValue.newBuilder()
                .setValue(actionSetting).build();
        final String resizeSettingName = ConfigurableActionSettings.Resize.getSettingName();
        final Setting resizeSetting = Setting.newBuilder().setSettingSpecName(resizeSettingName)
                .setEnumSettingValue(settingValue).build();
        return ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(resizeSetting).build())
                .build();
    }

    /**
     * Test filtering plans on scenarioId.
     */
    @Test
    public void testGetAllPlansWithScenarioIdFilters() {
        //GIVEN
        final long scenarioId = 456L;
        GetPlansOptions request = GetPlansOptions.newBuilder().addScenarioId(scenarioId).build();
        PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(12L)
                .setStatus(PlanStatus.SUCCEEDED)
                .setScenario(Scenario.newBuilder().setId(scenarioId)).build();

        doReturn(Collections.singleton(planInstance)).when(planDaoMock).getAllPlanInstances();

        //WHEN
        this.planService.getAllPlans(request, response);

        //THEN
        verify(response, times(1)).onNext(planInstance);
        verify(response, times(1)).onCompleted();
    }

    /**
     * Test filtering plans on scenarioId finds no match.
     */
    @Test
    public void testGetAllPlansWithScenarioIdFiltersNoMatch() {
        //GIVEN
        final long scenarioId = 456L;
        GetPlansOptions request = GetPlansOptions.newBuilder().addScenarioId(scenarioId).build();
        Set<PlanInstance> planInstances = Collections.singleton(PlanInstance.newBuilder()
                .setPlanId(12L)
                .setStatus(PlanStatus.SUCCEEDED)
                .setScenario(Scenario.newBuilder().setId(2L)).build());
        doReturn(planInstances).when(planDaoMock).getAllPlanInstances();

        //WHEN
        this.planService.getAllPlans(request, response);

        //THEN
        verify(response, times(0)).onNext(any());
        verify(response, times(1)).onCompleted();
    }

    @Test
    public void testGetUserScopeByEntityTypes() {
        //GIVEN the user scope group members
        final long groupId = 456L;
        final long vmId1 = 1L;
        final long vmId2 = 2L;
        final long dbId = 3L;
        final long dbsId = 4L;
        final long zoneId = 5L;
        when(testGroupRpcService.getMembers(any())).thenReturn(ImmutableList.of(
                GetMembersResponse.newBuilder().setGroupId(groupId)
                        .addMemberId(zoneId)
                        .build()));

        //GIVEN the supply chain of the group members
        final SupplyChainNode vmNode = SupplyChainNode.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(vmId1)
                        .addMemberOids(vmId2)
                        .build())
                .build();
        final SupplyChainNode dbNode = SupplyChainNode.newBuilder()
                .setEntityType(EntityType.DATABASE.getValue())
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(dbId)
                        .build())
                .build();
        final SupplyChainNode dbsNode = SupplyChainNode.newBuilder()
                .setEntityType(EntityType.DATABASE_SERVER.getValue())
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(dbsId)
                        .build())
                .build();
        final SupplyChainNode vvNode = SupplyChainNode.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE.getValue())
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(zoneId)
                        .build())
                .build();
        when(testSupplyChainRpcService.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addAllSupplyChainNodes(ImmutableList.of(vmNode, dbNode,
                                        dbsNode, vvNode)))
                        .build());

        //WHEN
        Map<Integer, EntityOids> scopeByEntityType = this.planService
                .getUserScopeByEntityTypes(Collections.singletonList(groupId));

        //THEN
        assertTrue(scopeByEntityType.containsKey(EntityType.DATABASE.getValue()));
        assertEquals(1, scopeByEntityType.get(EntityType.DATABASE.getValue())
                .getEntityOidsCount());

        assertTrue(scopeByEntityType.containsKey(EntityType.DATABASE_SERVER.getValue()));
        assertEquals(1, scopeByEntityType.get(EntityType.DATABASE_SERVER.getValue())
                .getEntityOidsCount());

        assertTrue(scopeByEntityType.containsKey(EntityType.VIRTUAL_MACHINE.getValue()));
        assertEquals(2, scopeByEntityType.get(EntityType.VIRTUAL_MACHINE.getValue())
                .getEntityOidsCount());
    }
}
