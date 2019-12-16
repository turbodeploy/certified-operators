package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for the {@link PlanReservedInstanceRpcService}.
 */
public class PlanReservedInstanceRpcServiceTest {
    private static final long PLAN_ID = 11111L;
    private static final String RI_NAME = "t1000.medium";
    private static final long RI_SPEC_ID = 2222L;
    private static final long REGION1_OID = 789456L;
    private static final long TIER_ID = 3333L;
    private static final long RI_BOUGHT_COUNT = 4L;
    private static final double DELTA = 0.01;

    private PlanReservedInstanceStore planReservedInstanceStore =
                    Mockito.mock(PlanReservedInstanceStore.class);

    private ReservedInstanceSpecStore reservedInstanceSpecStore =
                    Mockito.mock(ReservedInstanceSpecStore.class);

    private PlanReservedInstanceRpcService service = new PlanReservedInstanceRpcService(
                    planReservedInstanceStore);

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private PlanReservedInstanceServiceBlockingStub client;

    /**
     * Initialize instances before test.
     */
    @Before
    public void setup() {
        client = PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel());
        Mockito.when(planReservedInstanceStore.getPlanReservedInstanceCountByRISpecIdMap(PLAN_ID))
                        .thenReturn(Collections.singletonMap(RI_NAME, RI_BOUGHT_COUNT));
        Mockito.when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(Matchers.any()))
                        .thenReturn(Collections.singletonList(createRiSpec()));
    }

    /**
     * Tests get plan reserved instance bought count by template type.
     */
    @Test
    public void testGetPlanReservedInstanceBoughtCountByTemplateType() {
        final GetPlanReservedInstanceBoughtCountRequest request =
                        GetPlanReservedInstanceBoughtCountRequest.newBuilder().setPlanId(PLAN_ID).build();
        final GetPlanReservedInstanceBoughtCountByTemplateResponse response = client
                        .getPlanReservedInstanceBoughtCountByTemplateType(request);
        Assert.assertNotNull(response);
        final Map<String, Long> riBoughtCountByTierId = response.getReservedInstanceCountMapMap();
        Assert.assertFalse(riBoughtCountByTierId.isEmpty());
        Assert.assertEquals(RI_NAME, riBoughtCountByTierId.keySet().iterator().next());
        Assert.assertEquals(RI_BOUGHT_COUNT, riBoughtCountByTierId.get(RI_NAME), DELTA);
    }

    private static ReservedInstanceSpec createRiSpec() {
        return ReservedInstanceSpec.newBuilder()
                        .setId(RI_SPEC_ID)
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setRegionId(REGION1_OID)
                                        .setTierId(TIER_ID)
                                        .build())
                        .build();
    }

}
