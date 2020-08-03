package com.vmturbo.plan.orchestrator.plan;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyRIAnalysisServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.PlanReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceBoughtServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit test for {@link PlanRpcService}.
 */
public class PlanTest {

    private static final long PLAN_ID = 1234455;
    private static final long SCENARIO_ID = 3455332;
    private static final long NEW_SCENARIO_ID = SCENARIO_ID + 1;

    private AnalysisServiceMole analysisBackend = spy(AnalysisServiceMole.class);

    private BuyRIAnalysisServiceMole buyRIBackend = spy(BuyRIAnalysisServiceMole.class);

    private GroupServiceMole groupBackend = spy(GroupServiceMole.class);

    private RepositoryServiceMole repositoryBackend = spy(RepositoryServiceMole.class);

    private PlanReservedInstanceServiceMole planRiBackend = spy(PlanReservedInstanceServiceMole.class);

    private ReservedInstanceBoughtServiceMole riBoughtBackend = spy(ReservedInstanceBoughtServiceMole.class);

    private PlanNotificationSender planNotificationSender = mock(PlanNotificationSender.class);

    private ExecutorService analysisExecutor = mock(ExecutorService.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private PlanDao planDao = mock(PlanDao.class);

    @Rule
    public GrpcTestServer grpcDependenciesServer = GrpcTestServer.newServer(analysisBackend, buyRIBackend, groupBackend, repositoryBackend, planRiBackend, riBoughtBackend);

    private PlanServiceBlockingStub planClient;

    private final CreatePlanRequest request = CreatePlanRequest.newBuilder().setTopologyId(1L).build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Captor
    private ArgumentCaptor<StartAnalysisRequest> analysisRequestCaptor;

    private PlanRpcService planRpcService;

    /**
     * Not a Rule because it's dependend on other gRPC servers in the grpcDependenciesServer.
     */
    private GrpcTestServer planGrpcServer;

    /**
     * Setup for each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        planRpcService = new PlanRpcService(planDao,
           AnalysisServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            planNotificationSender,
            analysisExecutor, userSessionContext,
            BuyRIAnalysisServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            PlanReservedInstanceServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcDependenciesServer.getChannel()),
            1, TimeUnit.MILLISECONDS, 777L);

        planGrpcServer = GrpcTestServer.newServer(planRpcService);
        planGrpcServer.start();
        planClient = PlanServiceGrpc.newBlockingStub(planGrpcServer.getChannel());
    }

    /**
     * Tear down anything used in the test.
     */
    @After
    public void teardown() {
        planGrpcServer.close();
    }

    @Test
    public void testDeletePlan() throws Exception {
        PlanInstance instance = PlanInstance.newBuilder()
            .setPlanId(123L)
            .setStatus(PlanStatus.READY)
            .build();

        doReturn(instance).when(planDao).deletePlan(instance.getPlanId());

        PlanInstance retInstance = planClient.deletePlan(PlanId.newBuilder().setPlanId(instance.getPlanId()).build());
        assertThat(retInstance, is(instance));
    }

    /**
     * Tests for adding a plan which refers to absent scenario. Exception is expected.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testCreatePlanAbsentScenario() throws Exception {
        when(planDao.createPlanInstance(any()))
            .thenThrow(new IntegrityException(Long.toString(SCENARIO_ID)));
        final CreatePlanRequest createPlanRequest = CreatePlanRequest.newBuilder(request)
            .setScenarioId(SCENARIO_ID).build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(SCENARIO_ID)));

        planClient.createPlan(createPlanRequest);
    }

    /**
     * Tests running a correctly created plan.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRunPlan() throws Exception {
        final PlanInstance instance = PlanInstance.newBuilder()
            .setPlanId(123L)
            .setStatus(PlanStatus.QUEUED)
            .build();
        doReturn(Optional.of(instance)).when(planDao).getPlanInstance(instance.getPlanId());
        doReturn(Optional.of(instance)).when(planDao).queuePlanInstance(instance);

        final PlanInstance newInstance = planClient.runPlan(PlanId.newBuilder()
            .setPlanId(instance.getPlanId())
            .build());

        Assert.assertEquals(PlanStatus.QUEUED, newInstance.getStatus());

        ArgumentCaptor<Runnable> analysisExecutionCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(analysisExecutor).submit(analysisExecutionCaptor.capture());
        analysisExecutionCaptor.getValue().run();

        // The plan orchestrator calls the analysis service asynchronously.
        verify(analysisBackend).startAnalysis(any(StartAnalysisRequest.class), any());
    }

    /**
     * Tests running a plan provides a changes list when it is associated with a scenario.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRunPlanWithScenario() throws Exception {
        final ScenarioInfo scenarioInfo = createScenario(SCENARIO_ID);
        final PlanInstance plan = PlanInstance.newBuilder()
            .setPlanId(123L)
            .setStatus(PlanStatus.QUEUED)
            .setScenario(ScenarioOuterClass.Scenario.newBuilder()
                .setId(SCENARIO_ID)
                .setScenarioInfo(scenarioInfo))
            .build();
        when(planDao.getPlanInstance(plan.getPlanId())).thenReturn(Optional.of(plan));
        when(planDao.queuePlanInstance(plan)).thenReturn(Optional.of(plan));

        final PlanId planId = PlanId.newBuilder().setPlanId(plan.getPlanId()).build();
        planClient.runPlan(planId);
        // The analysis starts asynchronously.
        ArgumentCaptor<Runnable> analysisExecutionCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(analysisExecutor).submit(analysisExecutionCaptor.capture());
        analysisExecutionCaptor.getValue().run();

        verify(analysisBackend, Mockito.timeout(1000).times(1))
            .startAnalysis(analysisRequestCaptor.capture(), any());

        final StartAnalysisRequest request = analysisRequestCaptor.getValue();
        Assert.assertEquals(scenarioInfo.getChangesList(), request.getScenarioChangeList());
    }

    @Test
    public void testUpdatePlanScenario() throws Exception {
        // arrange
        final PlanInstance instance = PlanInstance.newBuilder()
            .setPlanId(123L)
            .setStatus(PlanStatus.QUEUED)
            .build();
        when(planDao.getPlanInstance(instance.getPlanId())).thenReturn(Optional.of(instance));

        final ScenarioInfo newInfo = createScenario(NEW_SCENARIO_ID);
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(instance.getPlanId())
                .setScenarioId(NEW_SCENARIO_ID)
                .build();

        when(planDao.updatePlanInstance(eq(instance.getPlanId()), any())).thenReturn(instance);

        // act
        PlanInstance result = planClient.updatePlanScenario(planScenario);

        // assert
        verify(planDao).updatePlanScenario(instance.getPlanId(), NEW_SCENARIO_ID);
    }

    @Test
    public void testUpdatePlanAbsent() throws Exception {
        // arrange
        createScenario(NEW_SCENARIO_ID);
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(PLAN_ID)
                .setScenarioId(NEW_SCENARIO_ID)
                .build();
        when(planDao.updatePlanScenario(PLAN_ID, NEW_SCENARIO_ID)).thenThrow(new NoSuchObjectException(Long.toString(PLAN_ID)));


        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(PLAN_ID)));

        // act
        PlanInstance result = planClient.updatePlanScenario(planScenario);

        // assert

    }

    @Test
    public void testUpdateScenarioAbsent() throws Exception {
        // arrange
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(PLAN_ID)
                .setScenarioId(SCENARIO_ID)
                .build();

        when(planDao.updatePlanScenario(planScenario.getPlanId(), planScenario.getScenarioId())).thenThrow(new IntegrityException("Scenario doesn't exist"));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(SCENARIO_ID)));

        // act
        PlanInstance result = planClient.updatePlanScenario(planScenario);

        // assert

    }

    /**
     * Tests running not existing plan.
     */
    @Test
    public void testRunAbsentPlan() {
        final PlanId planId = PlanId.newBuilder().setPlanId(PLAN_ID).build();
        when(planDao.getPlanInstance(planId.getPlanId())).thenReturn(Optional.empty());
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(PLAN_ID)));
        planClient.runPlan(planId);
    }

    /**
     * Tests deletion of absent plan by id. Exception is expected.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeleteAbsentPlan() throws Exception {
        final long id = 12345;
        final PlanId planId = PlanId.newBuilder().setPlanId(id).build();
        when(planDao.deletePlan(planId.getPlanId())).thenThrow(new NoSuchObjectException(Long.toString(id)));
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(id)));
        planClient.deletePlan(planId);
    }

    /**
     * Tests retrieval of all the plans, registered to the plan orchestrator.
     */
    @Test
    public void testGetAllPlans() {
        when(planDao.getAllPlanInstances()).thenReturn(Collections.emptySet());
        Assert.assertEquals(Collections.emptySet(),
                Sets.newHashSet(planClient.getAllPlans(GetPlansOptions.getDefaultInstance())));

        final PlanInstance instance1 = PlanInstance.newBuilder()
            .setPlanId(12312)
            .setStatus(PlanStatus.READY)
            .build();
        final PlanInstance instance2 = PlanInstance.newBuilder()
            .setPlanId(2321)
            .setStatus(PlanStatus.READY)
            .build();

        when(planDao.getAllPlanInstances()).thenReturn(Sets.newHashSet(instance1, instance2));

        List<PlanInstance> instances = new ArrayList<>();
        planClient.getAllPlans(GetPlansOptions.getDefaultInstance()).forEachRemaining(instances::add);

        assertThat(instances, containsInAnyOrder(instance1, instance2));
    }

    @Nonnull
    private ScenarioInfo createScenario(final long scenarioId) {
        LocalDateTime curTime = LocalDateTime.now();
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
            .addChanges(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                    .setAdditionCount(2)
                    .setEntityId(1234L)))
                .build();
        return scenarioInfo;
    }
}
