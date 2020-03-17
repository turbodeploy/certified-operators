package com.vmturbo.plan.orchestrator.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.CostMoles.PlanReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceBoughtServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IncludedCoupons;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Test class for the PlanReservedInstanceClient.
 *
 */
public class PlanReservedInstanceClientTest {

    // The plan reserved instance service mole.
    private final PlanReservedInstanceServiceMole testPlanRIService = spy(new PlanReservedInstanceServiceMole());

    // The reserved instance bought service mole.
    private final ReservedInstanceBoughtServiceMole testRiBoughtService = spy(new ReservedInstanceBoughtServiceMole());

    // The plan reserved instance client.
    private PlanReservedInstanceClient planReservedInstanceClient;

    /**
     * The  real-time topology context Id.
     */
    private static final Long realtimeTopologyContextId = 777777L;

    /**
     * The grpc Test Server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testPlanRIService, testRiBoughtService);

    /**
     * Setup to run before tests.
     */
    @Before
    public void setup() {
        this.planReservedInstanceClient = new PlanReservedInstanceClient(
            PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            realtimeTopologyContextId);
    }

    /**
     * Test parsing of Included RIs/Coupons from the plan scenario.
     *
     * <p>There could be none, all or a subset of RIs/Coupons to be included.
     */
    @Test
    public void testParsePlanIncludedCoupons() {
       PlanInstance.Builder planInstanceBuilder1 =
            PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
            .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                    .addChanges(ScenarioChange.newBuilder()
                            .setRiSetting(RISetting.newBuilder().build()))
                    .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build()));

       // No included RIs specified in plan scenario => Include All.
       // true return of method implies that all RIs in scope should be retrieved.
       // returnedIncludedRiOids itself is not populated.
       List<Long> returnedIncludedRiOids = new ArrayList<>();
       assertTrue(planReservedInstanceClient.parsePlanIncludedCoupons(
                              planInstanceBuilder1.build(), returnedIncludedRiOids));
       assertTrue(returnedIncludedRiOids.isEmpty());

       // Some Included some RIs specified in plan scenario.
       List<Long> someIncludedRIs = new ArrayList<>();
       someIncludedRIs.add(1L);
       someIncludedRIs.add(2L);

       IncludedCoupons includedCoupons = IncludedCoupons.newBuilder()
                       .addAllIncludedCouponOids(someIncludedRIs).build();

       PlanInstance.Builder planInstanceBuilder2 =
                       PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                       .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                               .addChanges(ScenarioChange.newBuilder()
                                       .setRiSetting(RISetting.newBuilder().build()))
                               .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build()));

       planInstanceBuilder2.setScenario(Scenario.newBuilder()
                                        .setScenarioInfo(ScenarioInfo.newBuilder()
                                                   .addChanges(ScenarioChange.newBuilder()
          .setPlanChanges(PlanChanges.newBuilder().setIncludedCoupons(includedCoupons)))));

       returnedIncludedRiOids.clear();
       assertFalse(planReservedInstanceClient.parsePlanIncludedCoupons(
                                      planInstanceBuilder2.build(), returnedIncludedRiOids));

       assertEquals(2, returnedIncludedRiOids.size());


       // Empty Included RIs specified in plan scenario => include none.
       PlanInstance.Builder planInstanceBuilder3 =
                       PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                       .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                               .addChanges(ScenarioChange.newBuilder()
                                       .setRiSetting(RISetting.newBuilder().build()))
                               .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build()));
       planInstanceBuilder3.setScenario(Scenario.newBuilder()
                                        .setScenarioInfo(ScenarioInfo.newBuilder()
                                                  .addChanges(ScenarioChange.newBuilder()
                                     .setPlanChanges(PlanChanges.newBuilder()
                                     .setIncludedCoupons(IncludedCoupons.newBuilder()
                                                 .addAllIncludedCouponOids(new ArrayList<Long>())
                                                                           .build())))));
       returnedIncludedRiOids.clear();
       assertFalse(planReservedInstanceClient.parsePlanIncludedCoupons(
                                       planInstanceBuilder3.build(), returnedIncludedRiOids));
       assertEquals(0, returnedIncludedRiOids.size());
    }
}