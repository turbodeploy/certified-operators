package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.PlanReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Tests for the {@link PlanRIStatsSubQuery}.
 */
public class PlanRIStatsSubQueryTest {
    private static final long PLAN_ID = 111111L;
    private static final String TIER_NAME = "t101.medium";
    private static final long TIER_ID = 101L;
    private static final long RI_COUNT = 10L;
    private static final StatApiInputDTO NUM_RI_INPUT = StatsTestUtil.statInput(StringConstants.NUM_RI);
    private static final StatApiInputDTO RI_COST_INPUT = StatsTestUtil.statInput(StringConstants.RI_COST);
    private static final double DELTA = 0.01;

    private PlanReservedInstanceServiceMole planReservedInstanceService =
            Mockito.spy(PlanReservedInstanceServiceMole.class);
    private ReservedInstanceUtilizationCoverageServiceMole backend =
            spy(ReservedInstanceUtilizationCoverageServiceMole.class);
    private final BuyRiScopeHandler buyRiScopeHandler = mock(BuyRiScopeHandler.class);
    private final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);

    private PlanRIStatsSubQuery planRIStatsQuery;

    /**
     * Set up a test plan RI GRPC server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(planReservedInstanceService);

    /**
     * Setup a test RI coverage and utilization GRPC server
     */
    @Rule
    public GrpcTestServer riCoverageUtilizationTestServer = GrpcTestServer.newServer(backend);


    /**
     * Initialize instances before test.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
        final MultiEntityRequest request = Mockito.mock(MultiEntityRequest.class);
        final ServiceEntityApiDTO tierApiDto = new ServiceEntityApiDTO();
        tierApiDto.setDisplayName(TIER_NAME);
        when(request.getSEMap()).thenReturn(Collections.singletonMap(TIER_ID, tierApiDto));
        when(repositoryApi.entitiesRequest(any())).thenReturn(request);
        final PlanInstance planInstance =
                        PlanInstance.newBuilder().setPlanId(PLAN_ID).setStatus(PlanStatus.SUCCEEDED).build();
        when(context.getPlanInstance()).thenReturn(Optional.of(planInstance));
        planRIStatsQuery = new PlanRIStatsSubQuery(repositoryApi,
                        PlanReservedInstanceServiceGrpc.newBlockingStub(testServer.getChannel()),
                        ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(riCoverageUtilizationTestServer.getChannel()),
                        buyRiScopeHandler);
    }

    /**
     * Tests getAggregateStats method for NumRI stats request.
     *
     * @throws OperationFailedException If anything goes wrong.
     */
    @Test
    public void testGetAggregateStatsForNumRI() throws OperationFailedException {
        final Map<Long, Long> riBoughtCountByTierId = Collections.singletonMap(TIER_ID, RI_COUNT);
        GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                        GetPlanReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                                        .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                                        .build();
        when(planReservedInstanceService.getPlanReservedInstanceBoughtCountByTemplateType(any()))
                        .thenReturn(response);
        final List<StatSnapshotApiDTO> result =
                        planRIStatsQuery.getAggregateStats(Collections.singleton(NUM_RI_INPUT), context);
        Assert.assertFalse(result.isEmpty());
        final List<StatApiDTO> statApiDTOS = result.get(0).getStatistics();
        Assert.assertFalse(statApiDTOS.isEmpty());
        final StatApiDTO statApiDTO = statApiDTOS.iterator().next();
        Assert.assertEquals(TIER_NAME, statApiDTO.getFilters().iterator().next().getValue());
        Assert.assertEquals(RI_COUNT, statApiDTO.getValues().getAvg(), DELTA);
    }

    /**
     * Tests getAggregateStats method for RICost stats request.
     *
     * @throws OperationFailedException If anything goes wrong.
     */
    @Test
    public void testGetAggregateStatsForRICost() throws OperationFailedException {
        final Cost.ReservedInstanceCostStat costStat = Cost.ReservedInstanceCostStat.newBuilder()
                        .setReservedInstanceOid(201L)
                        .setRecurringCost(2.85)
                        .setFixedCost(1950.0)
                        .setAmortizedCost(3.06)
                        .setSnapshotTime(1234567L).build();
        final GetPlanReservedInstanceCostStatsResponse response = GetPlanReservedInstanceCostStatsResponse.newBuilder()
                        .addStats(costStat)
                        .build();

        when(planReservedInstanceService.getPlanReservedInstanceCostStats(any())).thenReturn(response);
        final List<StatSnapshotApiDTO> result =
                        planRIStatsQuery.getAggregateStats(Collections.singleton(RI_COST_INPUT), context);
        Assert.assertFalse(result.isEmpty());
        final List<StatApiDTO> statApiDTOS = result.get(0).getStatistics();
        Assert.assertFalse(statApiDTOS.isEmpty());
        final StatApiDTO statApiDTO = statApiDTOS.iterator().next();
        Assert.assertEquals(StringConstants.RI_COST, statApiDTO.getName());
        Assert.assertEquals(3.06, statApiDTO.getValues().getAvg(), DELTA);
    }

    /**
     * Tests if RI counts per tier type name is being returned correctly or not.
     *
     * @throws InterruptedException Thrown on internal API call error.
     * @throws ConversionException Thrown on internal API call error.
     */
    @Test
    public void getNumRIStatsSnapshots() throws InterruptedException, ConversionException {
        final Map<Long, Long> riBoughtCountByTierId = new HashMap<>();
        final String statsName = "numRIs";
        final String filterType = "template";
        final long tierId1 = 1001L;
        final long riCount1 = 10L;
        final String tierName1 = "Standard_DS2_v2";
        final long tierId2 = 1002L;
        final long riCount2 = 20L;
        final String tierName2 = "Standard_B1ms";

        riBoughtCountByTierId.put(tierId1, riCount1);
        riBoughtCountByTierId.put(tierId2, riCount2);
        GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                GetPlanReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                        .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                        .build();
        when(planReservedInstanceService.getPlanReservedInstanceBoughtCountByTemplateType(any()))
                .thenReturn(response);

        final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
        final MultiEntityRequest request = Mockito.mock(MultiEntityRequest.class);

        Map<Long, ServiceEntityApiDTO> seMap = new HashMap<>();
        final ServiceEntityApiDTO tierApiDto1 = new ServiceEntityApiDTO();
        tierApiDto1.setDisplayName(tierName1);
        seMap.put(tierId1, tierApiDto1);
        final ServiceEntityApiDTO tierApiDto2 = new ServiceEntityApiDTO();
        tierApiDto2.setDisplayName(tierName2);
        seMap.put(tierId2, tierApiDto2);

        when(request.getSEMap()).thenReturn(seMap);
        when(repositoryApi.entitiesRequest(any())).thenReturn(request);
        PlanRIStatsSubQuery planQuery = new PlanRIStatsSubQuery(repositoryApi,
                PlanReservedInstanceServiceGrpc.newBlockingStub(testServer.getChannel()),
                ReservedInstanceUtilizationCoverageServiceGrpc
                        .newBlockingStub(riCoverageUtilizationTestServer.getChannel()),
                buyRiScopeHandler);
        List<StatSnapshotApiDTO> snaps = planQuery.getNumRIStatsSnapshots(PLAN_ID);

        assertEquals(1, snaps.size());
        StatSnapshotApiDTO dto = snaps.get(0);
        assertEquals(2, dto.getStatistics().size());
        final StatApiDTO stat1 = dto.getStatistics().get(0);
        final StatApiDTO stat2 = dto.getStatistics().get(1);

        assertEquals(statsName, stat1.getName());
        assertEquals(riCount1, stat1.getValue(), 0.1);
        assertEquals(tierName1, stat1.getFilters().get(0).getValue());
        assertEquals(filterType, stat1.getFilters().get(0).getType());

        assertEquals(statsName, stat2.getName());
        assertEquals(riCount2, stat2.getValue(), 0.1);
        assertEquals(tierName2, stat2.getFilters().get(0).getValue());
        assertEquals(filterType, stat2.getFilters().get(0).getType());
    }
}
