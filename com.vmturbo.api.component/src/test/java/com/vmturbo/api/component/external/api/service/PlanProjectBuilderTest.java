package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATE_CONTAINER_WORKLOADS_PLAN;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATION_PLAN__ALLOCATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATION_PLAN__CONSUMPTION;
import static com.vmturbo.common.protobuf.utils.StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanProjectMoles.PlanProjectServiceMole;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.UpdatePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioMoles.ScenarioServiceMole;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link PlanProjectBuilder}.
 */
@RunWith(JUnitParamsRunner.class)
public class PlanProjectBuilderTest {
    private final PlanServiceMole planBackend = spy(PlanServiceMole.class);
    private final PlanProjectServiceMole planProjectBackend = spy(PlanProjectServiceMole.class);
    private final ScenarioServiceMole scenarioBackend = spy(ScenarioServiceMole.class);
    /**
     * Test gRPC server to mock out gRPC dependencies.
     */
    @Rule
    public final GrpcTestServer grpcTestServer = GrpcTestServer.newServer(planBackend, planProjectBackend, scenarioBackend);
    private PlanProjectBuilder testPlanProjectBuilder;

    private final ApiId mockReatimeMarketId = mock(ApiId.class);

    /**
     * Instantiating required parameters to start the tests.
     *
     * @throws IOException if such an exception is encountered.
     */
    @Before
    public void setup() throws IOException {
        when(mockReatimeMarketId.isRealtimeMarket()).thenReturn(true);
        testPlanProjectBuilder = new PlanProjectBuilder(
                PlanProjectServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                ScenarioServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    /**
     * Testing the "isPlanProjectRequired()" method.
     */
    @Test
    @Parameters({
            CLOUD_MIGRATION_PLAN + ", true",
            MIGRATE_CONTAINER_WORKLOADS_PLAN + ", true",
            OPTIMIZE_CONTAINER_CLUSTER_PLAN + ", false"
    })
    public void testIsPlanProjectRequired(final String planType, final boolean expected) {
        final Scenario testScenario = Scenario.newBuilder().setId(19)
                .setScenarioInfo(ScenarioInfo.newBuilder().setType(planType).setName("foo")).build();
        Assert.assertEquals(expected,
                testPlanProjectBuilder.isPlanProjectRequired(mockReatimeMarketId, testScenario));
    }

    /**
     * Testing the "createPlanProject()" method.
     */
    @Test
    @Parameters({
            CLOUD_MIGRATION_PLAN + ", CLOUD_MIGRATION",
            MIGRATE_CONTAINER_WORKLOADS_PLAN + ", CONTAINER_MIGRATION"
    })
    public void testCreatePlanProject(final String planType, final PlanProjectType planProjectType)
            throws OperationFailedException {
        long id = 0;
        final String scenarioName = "foo";
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder().setType(planType)
                .setName(scenarioName).build();
        final Scenario testScenario = Scenario.newBuilder().setId(++id)
                .setScenarioInfo(scenarioInfo).build();

        // Mock the 6 service calls during the following 4 steps during the course of the
        // "createPlanProject()" method:
        //
        // 1. First is to call the plan project service to create a plan project.
        //    The test methodology here is to confirm that the plan project info constructed in the
        //    createPlanProject() method as expected as the following object, or the mock will not
        //    return a proper plan project and this test will fail.
        final PlanProjectInfo planProjectInfo = PlanProjectInfo.newBuilder()
                .setName(scenarioName).setType(planProjectType).build();
        final PlanProject planProject = PlanProject.newBuilder().setPlanProjectId(++id)
                .setPlanProjectInfo(planProjectInfo).build();
        doReturn(planProject).when(planProjectBackend).createPlanProject(planProjectInfo);

        // 2. Secondly, we call the scenario service to create a new scenario for the ALLOCATION
        //    plan in the lift & shift situation.  Then, we call the plan service to create a plan.
        //    Same test methodology used here to confirm that the constructed allocation scenario
        //    and the corresponding plan request are both expected.
        final Set<ScenarioChange> allocationOverrides = PlanProjectBuilder.settingOverridesByPlanType
                .get(planType).get(MIGRATION_PLAN__ALLOCATION);
        final String allocationScenarioName = scenarioName + "_" + planType + MIGRATION_PLAN__ALLOCATION;
        final ScenarioInfo allocationScenarioInfo = ScenarioInfo.newBuilder(scenarioInfo)
                .addAllChanges(allocationOverrides).setName(allocationScenarioName).build();
        final Scenario allocationScenario = Scenario.newBuilder().setId(++id)
                .setScenarioInfo(allocationScenarioInfo).build();
        doReturn(allocationScenario).when(scenarioBackend).createScenario(allocationScenarioInfo);

        final CreatePlanRequest allocationPlanRequest = CreatePlanRequest.newBuilder()
                .setScenarioId(allocationScenario.getId())
                .setName(allocationScenarioName)
                .setProjectType(planProjectType)
                .setPlanProjectId(planProject.getPlanProjectId())
                .build();
        final PlanInstance allocationPlanInstance = PlanInstance.newBuilder()
                .setPlanId(++id).setStatus(PlanStatus.READY)
                .setPlanProjectId(planProject.getPlanProjectId())
                .setProjectType(planProjectType)
                .setScenario(allocationScenario)
                .build();
        doReturn(allocationPlanInstance).when(planBackend).createPlan(allocationPlanRequest);

        // 3. Thirdly, we call the scenario service again to update the original scenario this time
        //    for the CONSUMPTION plan for the optimized situation.
        //    Same test methodology used here to confirm that the constructed consumption scenario
        //    and the corresponding plan request are both expected.
        final Set<ScenarioChange> consumptionOverrides = PlanProjectBuilder.settingOverridesByPlanType
                .get(planType).get(MIGRATION_PLAN__CONSUMPTION);
        final String consumptionScenarioName = scenarioName + "_" + planType + MIGRATION_PLAN__CONSUMPTION;
        final ScenarioInfo consumptionScenarioInfo = ScenarioInfo.newBuilder(scenarioInfo)
                .addAllChanges(consumptionOverrides).setName(consumptionScenarioName).build();
        final UpdateScenarioRequest updateScenarioRequest = UpdateScenarioRequest.newBuilder()
                .setScenarioId(testScenario.getId()).setNewInfo(consumptionScenarioInfo)
                .build();
        final Scenario consumptionScenario = Scenario.newBuilder().setId(testScenario.getId())
                .setScenarioInfo(consumptionScenarioInfo).build();
        final UpdateScenarioResponse updateScenarioResponse = UpdateScenarioResponse.newBuilder()
                .setScenario(consumptionScenario).build();
        doReturn(updateScenarioResponse).when(scenarioBackend).updateScenario(updateScenarioRequest);

        final CreatePlanRequest consumptionPlanRequest = CreatePlanRequest.newBuilder()
                .setScenarioId(consumptionScenario.getId())
                .setName(scenarioName)
                .setProjectType(planProjectType)
                .setPlanProjectId(planProject.getPlanProjectId())
                .build();
        final PlanInstance consumptionPlanInstance = PlanInstance.newBuilder()
                .setPlanId(++id).setStatus(PlanStatus.READY)
                .setPlanProjectId(planProject.getPlanProjectId())
                .setProjectType(planProjectType)
                .setScenario(consumptionScenario)
                .build();
        doReturn(consumptionPlanInstance).when(planBackend).createPlan(consumptionPlanRequest);

        // 4. As the last step, we call the plan project service to update the plan project with
        //    the related plan info.
        final UpdatePlanProjectRequest updatePlanProjectRequest = UpdatePlanProjectRequest.newBuilder()
                .setPlanProjectId(planProject.getPlanProjectId())
                .setMainPlanId(consumptionPlanInstance.getPlanId())
                .addAllRelatedPlanIds(Collections.singletonList(allocationPlanInstance.getPlanId()))
                .build();
        final PlanProjectInfo updatedPlanProjectInfo = PlanProjectInfo.newBuilder()
                .mergeFrom(planProjectInfo)
                .setMainPlanId(consumptionPlanInstance.getPlanId())
                .addRelatedPlanIds(allocationPlanInstance.getPlanId())
                .build();
        final PlanProject updatedPlanProject = PlanProject.newBuilder()
                .setPlanProjectId(planProject.getPlanProjectId())
                .setPlanProjectInfo(updatedPlanProjectInfo)
                .build();
        doReturn(updatedPlanProject).when(planProjectBackend).updatePlanProject(updatePlanProjectRequest);

        final PlanProject actual = testPlanProjectBuilder.createPlanProject(testScenario);
        Assert.assertTrue(actual.hasPlanProjectInfo());
        Assert.assertTrue(actual.getPlanProjectInfo().hasType());
        Assert.assertEquals(planProjectType, actual.getPlanProjectInfo().getType());
        Assert.assertEquals(scenarioName, actual.getPlanProjectInfo().getName());
        Assert.assertEquals("Main plan id should match", consumptionPlanInstance.getPlanId(),
                actual.getPlanProjectInfo().getMainPlanId());
        Assert.assertEquals("There should be one single related plan",
                1, actual.getPlanProjectInfo().getRelatedPlanIdsCount());
        Assert.assertEquals("Related plan id should match", allocationPlanInstance.getPlanId(),
                actual.getPlanProjectInfo().getRelatedPlanIds(0));
    }

    /**
     * Testing the "createPlanProject()" method for cases that an exception will be thrown.
     */
    @Test(expected = OperationFailedException.class)
    @Parameters({
            OPTIMIZE_CONTAINER_CLUSTER_PLAN
    })
    public void testCreatePlanProjectThrowsException(final String planType)
            throws OperationFailedException {
        final Scenario testScenario = Scenario.newBuilder().setId(19)
                .setScenarioInfo(ScenarioInfo.newBuilder().setType(planType).setName("foo")).build();
        testPlanProjectBuilder.createPlanProject(testScenario);
    }
}
