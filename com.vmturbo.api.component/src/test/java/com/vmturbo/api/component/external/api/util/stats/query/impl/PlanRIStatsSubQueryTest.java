package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.PlanReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Tests for the {@link PlanRIStatsSubQuery}.
 */
public class PlanRIStatsSubQueryTest {
    private static final long PLAN_ID = 111111L;
    private static final String TIER_NAME = "t101.medium";
    private static final long TIER_ID = 101L;
    private static final long RI_COUNT = 10L;
    private static final StatApiInputDTO NUM_RI_INPUT = StatsTestUtil.statInput(StringConstants.NUM_RI);

    private PlanReservedInstanceServiceMole planReservedInstanceService = Mockito.spy(PlanReservedInstanceServiceMole.class);

    private PlanRIStatsSubQuery planRIStatsQuery;

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(planReservedInstanceService);

    /**
     * Initialize instances before test.
     */
    @Before
    public void setup() {
        final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
        final MultiEntityRequest request = Mockito.mock(MultiEntityRequest.class);
        final ServiceEntityApiDTO tierApiDto = new ServiceEntityApiDTO();
        tierApiDto.setDisplayName(TIER_NAME);
        Mockito.when(request.getSEMap()).thenReturn(Collections.singletonMap(TIER_ID, tierApiDto));
        Mockito.when(repositoryApi.entitiesRequest(Matchers.any())).thenReturn(request);
        planRIStatsQuery = new PlanRIStatsSubQuery(repositoryApi,
                        PlanReservedInstanceServiceGrpc.newBlockingStub(testServer.getChannel()));
    }

    /**
     * Tests getAggregateStats method.
     *
     * @throws OperationFailedException If anything goes wrong.
     */
    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final Map<String, Long> riBoughtCountByTierId = Collections.singletonMap(TIER_NAME, RI_COUNT);
        GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                        GetPlanReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                                        .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                                        .build();
        Mockito.when(planReservedInstanceService.getPlanReservedInstanceBoughtCountByTemplateType(Matchers.any()))
                        .thenReturn(response);
        final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);
        final PlanInstance planInstance =
                        PlanInstance.newBuilder().setPlanId(PLAN_ID).setStatus(PlanStatus.SUCCEEDED).build();
        Mockito.when(context.getPlanInstance()).thenReturn(Optional.of(planInstance));
        final List<StatSnapshotApiDTO> result =
                        planRIStatsQuery.getAggregateStats(Collections.singleton(NUM_RI_INPUT), context);
        Assert.assertFalse(result.isEmpty());
        final List<StatApiDTO> statApiDTOS = result.get(0).getStatistics();
        Assert.assertFalse(statApiDTOS.isEmpty());
        final StatApiDTO statApiDTO = statApiDTOS.iterator().next();
        Assert.assertEquals(TIER_NAME, statApiDTO.getFilters().iterator().next().getValue());
        Assert.assertEquals(RI_COUNT, statApiDTO.getValues().getAvg(), 0);
    }
}
