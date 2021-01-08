package com.vmturbo.plan.orchestrator.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
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
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.DestinationEntityType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

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

    /**
     * Method savePlanIncludedCoupons calls repository API getReservedInstanceBoughtByFilter to
     * get a list of RIs. Verify account, region and RI filters are included in the request if the
     * corresponding data is present in the plan scenario.
     */
    @Test
    public void testSavePlanIncludedCoupons() {
        Long accountOid = 1234L;
        Set<Long> relatedAccounts = ImmutableSet.of(1234L, 1235L, 1236L);
        Long regionOid = 2222L;
        List<Long> riOidList = ImmutableList.of(11L, 12L, 13L);
        PlanInstance planInstance =
                PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                        .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                                .setType(StringConstants.CLOUD_MIGRATION_PLAN)
                                .addChanges(ScenarioChange.newBuilder()
                                        .setRiSetting(RISetting.newBuilder().build()))
                                .addChanges(ScenarioChange.newBuilder()
                                        .setPlanChanges(PlanChanges.newBuilder()
                                                .setIncludedCoupons(IncludedCoupons.newBuilder()
                                                        .addAllIncludedCouponOids(riOidList).build())))
                                .addChanges(ScenarioChange.newBuilder()
                                        .setTopologyMigration(TopologyMigration.newBuilder()
                                                .setDestinationEntityType(DestinationEntityType.VIRTUAL_MACHINE)
                                                .addDestination(MigrationReference.newBuilder().setEntityType(EntityType.REGION_VALUE).setOid(regionOid).build())
                                                .setDestinationAccount(MigrationReference.newBuilder()
                                                        .setOid(accountOid).build()).build())).build())).build();

        RepositoryClient repositoryClient = mock(RepositoryClient.class);
        when(repositoryClient.getAllRelatedBusinessAccountOids(accountOid)).thenReturn(relatedAccounts);

        planReservedInstanceClient.savePlanIncludedCoupons(planInstance, Collections.emptyList(), repositoryClient);

        verify(testRiBoughtService, times(1)).getReservedInstanceBoughtByFilter(any());

        ArgumentCaptor<GetReservedInstanceBoughtByFilterRequest> riBoughtByFilterRequestCaptor
                = ArgumentCaptor.forClass(GetReservedInstanceBoughtByFilterRequest.class);

        verify(testRiBoughtService).getReservedInstanceBoughtByFilter(riBoughtByFilterRequestCaptor.capture());

        assertTrue(riBoughtByFilterRequestCaptor.getValue().hasAccountFilter());
        assertEquals(3, riBoughtByFilterRequestCaptor.getValue().getAccountFilter().getAccountIdCount());

        assertTrue(riBoughtByFilterRequestCaptor.getValue().hasRegionFilter());
        assertEquals(1, riBoughtByFilterRequestCaptor.getValue().getRegionFilter().getRegionIdCount());
        assertEquals(regionOid, new Long(riBoughtByFilterRequestCaptor.getValue().getRegionFilter().getRegionId(0)));

        assertTrue(riBoughtByFilterRequestCaptor.getValue().hasExcludeUndiscoveredUnused());
        assertEquals(true, riBoughtByFilterRequestCaptor.getValue().getExcludeUndiscoveredUnused());

        assertTrue(riBoughtByFilterRequestCaptor.getValue().hasRiFilter());
        assertEquals(riOidList, riBoughtByFilterRequestCaptor.getValue().getRiFilter().getRiIdList());
    }
}
