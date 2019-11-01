package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit test for {@link PlanRpcService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {PlanTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class PlanTest {

    private static final long PLAN_ID = 1234455;
    private static final long SCENARIO_ID = 3455332;
    private static final long NEW_SCENARIO_ID = SCENARIO_ID + 1;

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    @Autowired
    private AnalysisServiceImplBase analysisServer;
    @Autowired
    private PlanServiceBlockingStub planClient;

    @Autowired
    private RepositoryClient repositoryClient;

    private final CreatePlanRequest request = CreatePlanRequest.newBuilder().setTopologyId(1L).build();

    @Rule
    public TestName testName = new TestName();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Captor
    private ArgumentCaptor<StartAnalysisRequest> analysisRequestCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Tests base CRUD operations on plan instance.
     *
     * @throws Exception if error occurred
     */
    @Test
    public void testCrud() throws Exception {
        final PlanInstance instance = planClient.createPlan(request);
        final PlanId planId = PlanId.newBuilder().setPlanId(instance.getPlanId()).build();
        Assert.assertEquals(request.getTopologyId(), instance.getSourceTopologyId());
        Assert.assertEquals(instance, planClient.getPlan(planId).getPlanInstance());
        Assert.assertEquals(instance, planClient.deletePlan(planId));
        Assert.assertFalse(planClient.getPlan(planId).hasPlanInstance());
    }

    @Test
    public void testDoNotDeleteTopologyWhenNone() throws Exception {
        final CreatePlanRequest createPlanRequest = CreatePlanRequest.newBuilder().build();

        final PlanInstance instance = planClient.createPlan(createPlanRequest);
        planClient.deletePlan(PlanId.newBuilder().setPlanId(instance.getPlanId()).build());

        Mockito.verify(repositoryClient,
                Mockito.never()).deleteTopology(Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
    }

    /**
     * Tests for adding a plan which refers to absent scenario. Exception is expected.
     */
    @Test
    public void testCreatePlanAbsentScenario() {
        final CreatePlanRequest createPlanRequest = CreatePlanRequest.newBuilder(request).setScenarioId(PLAN_ID).build();
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(PLAN_ID)));
        planClient.createPlan(createPlanRequest);
    }

    /**
     * Tests for adding a plan which refers to existing scenario.
     */
    @Test
    public void testCreatePlanExistingScenario() {
        final CreatePlanRequest createPlanRequest = CreatePlanRequest.newBuilder(request).setScenarioId(SCENARIO_ID).build();
        final ScenarioInfo scenario = createScenario(SCENARIO_ID);
        final PlanInstance plan = planClient.createPlan(createPlanRequest);
        Assert.assertEquals(scenario, plan.getScenario().getScenarioInfo());
    }

    /**
     * Tests running a correctly created plan.
     */
    @Test
    public void testRunPlan() {
        final PlanInstance instance = planClient.createPlan(request);
        Assert.assertFalse(instance.hasStartTime());
        Assert.assertEquals(PlanStatus.READY, instance.getStatus());
        final PlanId planId = PlanId.newBuilder().setPlanId(instance.getPlanId()).build();
        final long now = System.currentTimeMillis();
        final PlanInstance newInstance = planClient.runPlan(planId);
        Assert.assertEquals(PlanStatus.QUEUED, newInstance.getStatus());
        Assert.assertTrue(newInstance.hasStartTime());
        Assert.assertTrue(now <= newInstance.getStartTime());
        // The plan orchestrator calls the analysis service asynchronously.
        Mockito.verify(analysisServer, Mockito.timeout(1000))
                .startAnalysis(Mockito.any(StartAnalysisRequest.class), Mockito.any());
    }

    /**
     * Tests running a plan provides a changes list when it is associated with a scenario.
     */
    @Test
    public void testRunPlanWithScenario() {
        final ScenarioInfo scenarioInfo = createScenario(SCENARIO_ID);
        final CreatePlanRequest createPlanRequest = CreatePlanRequest.newBuilder(request).setScenarioId(SCENARIO_ID).build();
        final PlanInstance plan = planClient.createPlan(createPlanRequest);

        final PlanId planId = PlanId.newBuilder().setPlanId(plan.getPlanId()).build();
        planClient.runPlan(planId);
        // The analysis starts asynchronously.
        Mockito.verify(analysisServer, Mockito.timeout(1000).times(1))
            .startAnalysis(analysisRequestCaptor.capture(), Mockito.any());

        final StartAnalysisRequest request = analysisRequestCaptor.getValue();
        Assert.assertEquals(scenarioInfo.getChangesList(), request.getScenarioChangeList());
    }

    @Test
    public void testUpdatePlanScenario() {
        // arrange
        final PlanInstance instance = planClient.createPlan(request);
        final ScenarioInfo newInfo = createScenario(NEW_SCENARIO_ID);
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(instance.getPlanId())
                .setScenarioId(NEW_SCENARIO_ID)
                .build();

        // act
        PlanInstance result = planClient.updatePlanScenario(planScenario);

        // assert
        assertThat(result.getPlanId(), is(instance.getPlanId()));
        final ScenarioInfo updatedScenarioInfo = result.getScenario().getScenarioInfo();
        assertThat(updatedScenarioInfo, equalTo(newInfo));
        assertThat(updatedScenarioInfo.getChangesCount(), is(1));

    }

    @Test
    public void testUpdatePlanAbsent() {
        // arrange
        createScenario(NEW_SCENARIO_ID);
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(PLAN_ID)
                .setScenarioId(NEW_SCENARIO_ID)
                .build();
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(PLAN_ID)));

        // act
        PlanInstance result = planClient.updatePlanScenario(planScenario);

        // assert

    }

    @Test
    public void testUpdateScenarioAbsent() {
        // arrange
        PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                .setPlanId(PLAN_ID)
                .setScenarioId(SCENARIO_ID)
                .build();
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
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(PLAN_ID)));
        planClient.runPlan(planId);
        Mockito.verify(analysisServer, Mockito.never())
                .startAnalysis(Mockito.any(StartAnalysisRequest.class), Mockito.any());
    }

    /**
     * Tests deletion of absent plan by id. Exception is expected.
     */
    @Test
    public void testDeleteAbsentPlan() {
        final long id = 12345;
        final PlanId planId = PlanId.newBuilder().setPlanId(id).build();
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Status.Code.NOT_FOUND)
                .descriptionContains(Long.toString(id)));
        planClient.deletePlan(planId);
    }

    /**
     * Tests retrieval of all the plans, registered to the plan orchestrator.
     */
    @Test
    public void testGetAllPlans() {
        Assert.assertEquals(Collections.emptySet(),
                Sets.newHashSet(planClient.getAllPlans(GetPlansOptions.getDefaultInstance())));
        final long id1 = planClient.createPlan(CreatePlanRequest.newBuilder().build()).getPlanId();
        Assert.assertEquals(Sets.newHashSet(id1),
                Sets.newHashSet(planClient.getAllPlans(GetPlansOptions.getDefaultInstance()))
                        .stream()
                        .map(PlanInstance::getPlanId)
                        .collect(Collectors.toSet()));
        final long id2 = planClient.createPlan(CreatePlanRequest.newBuilder().build()).getPlanId();
        Assert.assertEquals(Sets.newHashSet(id1, id2),
                Sets.newHashSet(planClient.getAllPlans(GetPlansOptions.getDefaultInstance()))
                        .stream()
                        .map(PlanInstance::getPlanId)
                        .collect(Collectors.toSet()));
        planClient.deletePlan(PlanId.newBuilder().setPlanId(id1).build());
        Assert.assertEquals(Sets.newHashSet(id2),
                Sets.newHashSet(planClient.getAllPlans(GetPlansOptions.getDefaultInstance()))
                        .stream()
                        .map(PlanInstance::getPlanId)
                        .collect(Collectors.toSet()));
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
        Scenario scenario = new Scenario(scenarioId, curTime, curTime, scenarioInfo);
        dbConfig.dsl().newRecord(SCENARIO, scenario).store();
        return scenarioInfo;
    }
}
